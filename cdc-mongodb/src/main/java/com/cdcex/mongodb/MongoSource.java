package com.cdcex.mongodb;

import com.cdcex.core.*;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.*;
import org.bson.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Snapshot at one persisted timestamp, followed by exact historical change-stream images. */
final class MongoSource implements SourceConnector {
    private final Endpoint endpoint;
    private final ConnectorProvider.SourceContext context;
    private volatile boolean closed;
    private volatile MongoClient client;
    private volatile MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> activeCursor;

    MongoSource(Endpoint endpoint,ConnectorProvider.SourceContext context) {
        this.endpoint=endpoint;this.context=context;
        if(!Set.of("initial","no_data").contains(context.snapshotMode()))
            throw new IllegalArgumentException("MongoDB supports initial and no_data capture modes");
    }
    @Override public void run(DurableReceiver receiver) throws Exception {
        if(closed)return;
        try(var opened=MongoProvider.connect(endpoint)) {
            client=opened;
            if(closed)return;
            var inspection=MongoProvider.inspect(opened,endpoint,ConnectorProvider.Role.SOURCE);
            var checkpoint=new MongoCheckpoint(context.directory());
            MongoCheckpoint.State state=checkpoint.exists()?checkpoint.read():null;
            if(state==null && !context.fresh())throw new IllegalStateException("Missing MongoDB checkpoint; automatic reset refused");
            if(state!=null && (!state.identity().equals(inspection.identity()) || !state.mode().equals(context.snapshotMode())))
                throw new IllegalStateException("MongoDB source identity or capture mode changed");
            if(state==null || "snapshot".equals(state.phase()))
                state=snapshot(opened,inspection.identity(),checkpoint,state,receiver);
            if(!closed)stream(opened,checkpoint,state,inspection.details().get("schemaOptions").toString(),receiver);
        } catch(MongoException e) {
            if(!closed)throw new IllegalStateException("MongoDB capture blocked (server code "+e.getCode()+"); retained offsets were not reset");
        } finally {client=null;activeCursor=null;}
    }
    private MongoCheckpoint.State snapshot(MongoClient client,String identity,MongoCheckpoint checkpoint,
                                           MongoCheckpoint.State state,DurableReceiver receiver) throws Exception {
        var database=client.getDatabase(endpoint.require("database"));
        String table=endpoint.require("table");
        long cursorId=0;
        try(var session=client.startSession(ClientSessionOptions.builder().causallyConsistent(false).build())) {
            BsonDocument concern=new BsonDocument("level",new BsonString("snapshot"));
            if(state!=null)concern.append("atClusterTime",state.boundary());
            BsonDocument command=new BsonDocument("find",new BsonString(table))
                .append("filter",new BsonDocument()).append("sort",new BsonDocument("_id",new BsonInt32(1)))
                .append("batchSize",new BsonInt32(128)).append("readConcern",concern)
                .append("maxTimeMS",new BsonInt64(30_000));
            if("no_data".equals(context.snapshotMode()))command.append("limit",new BsonInt32(1)).append("singleBatch",BsonBoolean.TRUE);
            BsonDocument reply=database.runCommand(session,command,BsonDocument.class);
            BsonDocument cursor=reply.getDocument("cursor");
            cursorId=cursor.getNumber("id").longValue();
            if(state==null) {
                BsonTimestamp boundary=cursor.getTimestamp("atClusterTime",reply.getTimestamp("atClusterTime",null));
                if(boundary==null)throw new IllegalStateException("MongoDB did not return a snapshot boundary");
                state=new MongoCheckpoint.State(identity,context.snapshotMode(),
                    "initial".equals(context.snapshotMode())?"snapshot":"stream",boundary,null);
                // Never emit a row before the immutable snapshot boundary is durable.
                checkpoint.write(state);
            }
            if("initial".equals(context.snapshotMode())) {
                BsonArray rows=cursor.getArray("firstBatch");
                while(!closed) {
                    List<ChangeEvent> batch=new ArrayList<>(rows.size());
                    for(BsonValue row:rows)batch.add(snapshotEvent(endpoint,state,row.asDocument()));
                    if(!batch.isEmpty())receiver.accept(batch);
                    if(cursorId==0)break;
                    reply=database.runCommand(session,new BsonDocument("getMore",new BsonInt64(cursorId))
                        .append("collection",new BsonString(table)).append("batchSize",new BsonInt32(128)),BsonDocument.class);
                    cursor=reply.getDocument("cursor");cursorId=cursor.getNumber("id").longValue();
                    rows=cursor.getArray("nextBatch");
                }
            }
            if(!closed) {state=state.stream(null);checkpoint.write(state);}
            if(cursorId!=0)database.runCommand(session,new BsonDocument("killCursors",new BsonString(table))
                .append("cursors",new BsonArray(List.of(new BsonInt64(cursorId)))),BsonDocument.class);
            return state;
        }
    }
    static ChangeEvent snapshotEvent(Endpoint endpoint,MongoCheckpoint.State state,BsonDocument row) {
        Map<String,Object> key=BsonValues.key(row);
        String position=new BsonDocument("snapshotAt",state.boundary()).toJson(BsonValues.CANONICAL);
        String id=EventCodec.digest(List.of(state.identity(),"snapshot",position,key));
        return new ChangeEvent(id,endpoint.require("database"),endpoint.require("table"),ChangeEvent.Operation.SNAPSHOT,
            position,key,null,BsonValues.map(row),Integer.toUnsignedLong(state.boundary().getTime())*1000);
    }
    private void stream(MongoClient client,MongoCheckpoint checkpoint,MongoCheckpoint.State initial,String schema,
                        DurableReceiver receiver) throws Exception {
        var collection=client.getDatabase(endpoint.require("database")).getCollection(endpoint.require("table"),BsonDocument.class);
        var stream=collection.watch().fullDocument(FullDocument.REQUIRED)
            .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED).showExpandedEvents(true)
            .maxAwaitTime(1000,TimeUnit.MILLISECONDS).batchSize(128);
        if(initial.token()!=null)stream.resumeAfter(initial.token());else stream.startAtOperationTime(initial.boundary());
        MongoCheckpoint.State state=initial;
        try(var cursor=stream.cursor()) {
            activeCursor=cursor;
            while(!closed) {
                BsonDocument metadata=MongoProvider.collectionMetadata(client,endpoint);
                if(!initial.identity().equals(MongoProvider.identity(endpoint,metadata)) || !schema.equals(MongoProvider.schema(metadata)))
                    throw new IllegalStateException("MongoDB source collection identity or options changed");
                List<ChangeEvent> batch=new ArrayList<>();
                BsonDocument token=null;
                ChangeStreamDocument<BsonDocument> change=cursor.tryNext();
                while(change!=null) {
                    // The snapshot already includes all operations at the exact boundary.
                    if(change.getClusterTime()==null)throw new IllegalStateException("MongoDB change has no cluster timestamp");
                    if(change.getClusterTime().compareTo(initial.boundary())>0)
                        batch.add(normalize(endpoint,state.identity(),change));
                    token=change.getResumeToken();
                    if(token==null)throw new IllegalStateException("MongoDB change has no resume token");
                    if(batch.size()>=128 || cursor.available()==0)break;
                    change=cursor.tryNext();
                }
                if(!batch.isEmpty())receiver.accept(batch);
                // Empty-batch high-water tokens also retain progress on idle collections.
                if(token==null)token=cursor.getResumeToken();
                if(token!=null && !token.equals(state.token())) {
                    state=state.stream(token);checkpoint.write(state);
                }
            }
        } finally {activeCursor=null;}
    }
    static ChangeEvent normalize(Endpoint endpoint,String identity,ChangeStreamDocument<BsonDocument> change) {
        ChangeEvent.Operation operation=switch(change.getOperationType()) {
            case INSERT -> ChangeEvent.Operation.INSERT;
            case UPDATE,REPLACE -> ChangeEvent.Operation.UPDATE;
            case DELETE -> ChangeEvent.Operation.DELETE;
            default -> throw new IllegalStateException("MongoDB schema/topology change blocked; review the task before resuming");
        };
        BsonDocument after=change.getFullDocument(),before=change.getFullDocumentBeforeChange();
        if(operation!=ChangeEvent.Operation.DELETE && after==null)
            throw new IllegalStateException("Exact MongoDB post-image is unavailable");
        if((operation==ChangeEvent.Operation.UPDATE || operation==ChangeEvent.Operation.DELETE) && before==null)
            throw new IllegalStateException("Exact MongoDB pre-image is unavailable");
        Map<String,Object> key=BsonValues.key(change.getDocumentKey());
        if(after!=null && !key.equals(BsonValues.key(after)))throw new IllegalStateException("MongoDB post-image key mismatch");
        String position=Objects.requireNonNull(change.getResumeToken()).toJson(BsonValues.CANONICAL);
        return new ChangeEvent(EventCodec.digest(List.of(identity,position)),endpoint.require("database"),endpoint.require("table"),
            operation,position,key,BsonValues.map(before),BsonValues.map(after),
            Integer.toUnsignedLong(change.getClusterTime().getTime())*1000);
    }
    @Override public void close() {
        closed=true;
        var cursor=activeCursor;if(cursor!=null)cursor.close();
        var active=client;if(active!=null)active.close();
    }
}
