package com.cdcex.mongodb;

import com.cdcex.core.*;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** One ordered transaction per journal batch; retries replace the same complete documents. */
final class MongoSink implements SinkConnector {
    private final Endpoint endpoint;
    private final MongoClient client;
    private final String identity,schema;
    MongoSink(Endpoint endpoint) {
        this.endpoint=endpoint;
        MongoClient opened=MongoProvider.connect(endpoint);
        try {
            var inspection=MongoProvider.inspect(opened,endpoint,ConnectorProvider.Role.SINK);
            identity=inspection.identity();schema=inspection.details().get("schemaOptions").toString();client=opened;
        } catch(RuntimeException e){opened.close();throw e;}
    }
    @Override public void apply(List<ChangeEvent> events) {
        BsonDocument meta=MongoProvider.collectionMetadata(client,endpoint);
        if(!identity.equals(MongoProvider.identity(client,endpoint,meta)) || !schema.equals(MongoProvider.schema(meta)))
            throw new IllegalStateException("MongoDB target replica-set identity, collection identity or validation options changed");
        var collection=client.getDatabase(endpoint.require("database")).getCollection(endpoint.require("table"),BsonDocument.class);
        try(var session=client.startSession()) {
            session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                .writeConcern(WriteConcern.MAJORITY.withJournal(true)).maxCommitTime(10L,TimeUnit.SECONDS).build());
            try {
                for(ChangeEvent event:events) {
                    if(!endpoint.require("database").equals(event.database()) || !endpoint.require("table").equals(event.table()))
                        throw new IllegalArgumentException("MongoDB event namespace does not match target mapping");
                    if(event.key()==null || !event.key().keySet().equals(Set.of("_id")))
                        throw new IllegalArgumentException("MongoDB requires exactly one mapped _id key");
                    BsonDocument key=BsonValues.document(event.key());BsonValues.key(key);
                    switch(event.operation()) {
                        case DELETE -> collection.deleteOne(session,key);
                        case SNAPSHOT,INSERT,UPDATE -> {
                            BsonDocument after=BsonValues.document(event.after());
                            if(after==null || !Objects.equals(after.get("_id"),key.get("_id")))
                                throw new IllegalArgumentException("MongoDB mapped document _id does not match its event key");
                            if(event.operation()==ChangeEvent.Operation.UPDATE && event.before()!=null) {
                                BsonDocument before=BsonValues.document(event.before());
                                BsonValues.key(before);
                                if(!Objects.equals(before.get("_id"),key.get("_id")))
                                    collection.deleteOne(session,new BsonDocument("_id",before.get("_id")));
                            }
                            collection.replaceOne(session,key,after,new ReplaceOptions().upsert(true));
                        }
                        default -> throw new IllegalArgumentException("Unsupported MongoDB sink operation");
                    }
                }
                session.commitTransaction();
            } catch(RuntimeException e) {
                try {session.abortTransaction();}catch(RuntimeException ignored){}
                // No checkpoint advancement on a failed or ambiguous commit.
                throw e;
            }
        }
    }
    @Override public void close(){client.close();}
}
