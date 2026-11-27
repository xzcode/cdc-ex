package com.cdcex.storage;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.types.Binary;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/** MongoDB management repository, independent of the MongoDB capture connector. */
public final class MongoControlStore implements ControlRepository {
    private final MongoClient client;
    private final MongoCollection<Document> records,events,owner;
    private final SecretBox secrets;
    private static final List<String> INTERRUPTED=List.of("STARTING","CAPTURING","RETRYING","STOPPING","PAUSING");
    public MongoControlStore(String uri,String database,boolean tls,byte[] key,String deployment,boolean initialize)throws Exception{
        if(database==null||!database.matches("[A-Za-z0-9_-]{1,63}"))throw new IllegalArgumentException("Invalid control MongoDB database");
        secrets=new SecretBox(key);
        var settings=MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
            .applyToSslSettings(b->b.enabled(tls).invalidHostNameAllowed(false))
            .applyToClusterSettings(b->b.serverSelectionTimeout(5,TimeUnit.SECONDS))
            .applyToSocketSettings(b->b.connectTimeout(5,TimeUnit.SECONDS).readTimeout(10,TimeUnit.SECONDS))
            .readPreference(ReadPreference.primary()).readConcern(ReadConcern.MAJORITY)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(10,TimeUnit.SECONDS)).build();
        client=MongoClients.create(settings);
        var db=client.getDatabase(database);records=db.getCollection("cdc_ex_records");events=db.getCollection("cdc_ex_audit");owner=db.getCollection("cdc_ex_secrets_check");
        try{
            String expected="cdc-ex-control-store:"+deployment;
            Document check=owner.find(eq("_id","owner")).first();
            if(check==null){
                if(!initialize)throw new IllegalStateException("Bound control store is missing; restore its metadata");
                if(records.find().limit(1).first()!=null||events.find().limit(1).first()!=null)throw new IllegalStateException("Unbound control store contains records");
                owner.insertOne(new Document("_id","owner").append("value",new Binary(secrets.seal("key-check",expected))));
                check=owner.find(eq("_id","owner")).first();
            }
            if(check==null||!expected.equals(secrets.open("key-check",check.get("value",Binary.class).getData())))throw new IllegalStateException("Control store owner or deployment key does not match");
            if(initialize)records.createIndex(Indexes.ascending("kind","id"));
        }catch(Exception e){client.close();throw e;}
    }
    private static String key(String kind,String id){ControlStore.id(id);return kind+":"+id;}
    private static void name(String name){if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid display name");}
    private static long now(){return System.currentTimeMillis();}
    private ControlStore.Record record(Document row)throws Exception{
        String kind=row.getString("kind"),id=row.getString("id");
        return new ControlStore.Record(kind,id,row.getString("name"),secrets.open(key(kind,id),row.get("body",Binary.class).getData()),row.getString("state"),row.getString("error"),row.getString("source_identity"),row.getString("sink_identity"),row.getLong("updated"));
    }
    @Override public synchronized void create(String kind,String id,String name,String json)throws Exception{
        name(name);String k=key(kind,id);
        try{records.insertOne(new Document("_id",k).append("kind",kind).append("id",id).append("name",name).append("body",new Binary(secrets.seal(k,json))).append("state","DRAFT").append("updated",now()));}
        catch(MongoWriteException e){if(e.getError().getCategory()==ErrorCategory.DUPLICATE_KEY)throw new IllegalStateException("Resource already exists");throw e;}
        audit("CREATE_"+kind.toUpperCase(Locale.ROOT),id,"OK");
    }
    @Override public synchronized void replace(String kind,String id,String name,String json)throws Exception{
        name(name);String k=key(kind,id);
        if(records.updateOne(eq("_id",k),combine(set("name",name),set("body",new Binary(secrets.seal(k,json))),set("updated",now()))).getMatchedCount()!=1)throw new NoSuchElementException("Resource not found");
        audit("UPDATE_"+kind.toUpperCase(Locale.ROOT),id,"OK");
    }
    @Override public synchronized ControlStore.Record get(String kind,String id)throws Exception{
        Document row=records.find(eq("_id",key(kind,id))).first();if(row==null)throw new NoSuchElementException("Resource not found");return record(row);
    }
    @Override public synchronized List<ControlStore.Record> list(String kind)throws Exception{
        List<ControlStore.Record> out=new ArrayList<>();
        try(var cursor=records.find(eq("kind",kind)).sort(Sorts.ascending("id")).iterator()){while(cursor.hasNext())out.add(record(cursor.next()));}return out;
    }
    @Override public synchronized void state(String id,String state,String error){records.updateOne(eq("_id",key("task",id)),combine(set("state",state),set("error",error),set("updated",now())));}
    @Override public synchronized void identities(String id,String source,String sink){records.updateOne(eq("_id",key("task",id)),combine(set("source_identity",source),set("sink_identity",sink)));}
    @Override public synchronized void recoverInterrupted(){records.updateMany(and(eq("kind","task"),in("state",INTERRUPTED)),combine(set("state","STOPPED"),set("error","INTERRUPTED_REVIEW_AND_RESUME")));}
    @Override public synchronized void audit(String action,String resource,String result){
        events.insertOne(new Document("at",now()).append("action",action).append("resource",resource).append("result",result));
        Document boundary=events.find().sort(Sorts.descending("_id")).skip(9999).limit(1).projection(Projections.include("_id")).first();
        if(boundary!=null)events.deleteMany(lt("_id",boundary.getObjectId("_id")));
    }
    @Override public synchronized List<ControlStore.Audit> audits(){
        List<ControlStore.Audit> out=new ArrayList<>();
        try(var cursor=events.find().sort(Sorts.descending("_id")).limit(100).iterator()){while(cursor.hasNext()){var row=cursor.next();out.add(new ControlStore.Audit(row.getLong("at"),row.getString("action"),row.getString("resource"),row.getString("result")));}}return out;
    }
    @Override public synchronized void close(){client.close();}
}
