package com.cdcex.mongodb;

import com.cdcex.core.*;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class MongoProvider implements ConnectorProvider {
    @Override public String type(){return "MONGODB";}
    @Override public Map<String,Object> capabilities(){return Map.of(
        "sourceModes",List.of("initial","no_data"),"sinkModes",List.of("replace"),
        "nativeCdc",true,"documentEncoding","canonical-extended-json", "topology","replica-set",
        "requiresPrePostImages",true,"requiresKey",true);}
    static void names(Endpoint endpoint) {
        for(String key:List.of("database","table"))
            if(!endpoint.require(key).matches("[A-Za-z_][A-Za-z0-9_]{0,62}"))
                throw new IllegalArgumentException("Unsupported MongoDB namespace");
        if(Set.of("admin","config","local").contains(endpoint.require("database")))
            throw new IllegalArgumentException("MongoDB system databases are not supported");
    }
    static MongoClient connect(Endpoint endpoint) {
        names(endpoint);
        boolean tls=endpoint.flag("tls",true);
        if(!tls && !endpoint.flag("allowInsecure",false))throw new IllegalArgumentException("Unencrypted MongoDB requires explicit allowInsecure=true");
        String uri=endpoint.options().get("uri");
        if(uri==null) {
            String host=endpoint.require("host");int port=endpoint.integer("port",27017);
            if(!host.matches("[A-Za-z0-9.-]+")||port<1||port>65535)throw new IllegalArgumentException("Invalid MongoDB host/port");
            uri="mongodb://"+host+":"+port+"/";
        }
        ConnectionString connection;
        try {connection=new ConnectionString(uri);}catch(Exception e){throw new IllegalArgumentException("Invalid MongoDB connection URI");}
        if(Boolean.TRUE.equals(connection.getSslInvalidHostnameAllowed()))throw new IllegalArgumentException("MongoDB TLS hostname verification cannot be disabled");
        var builder=MongoClientSettings.builder().applyConnectionString(connection)
            .applicationName("cdc-ex").readPreference(ReadPreference.primary()).readConcern(ReadConcern.MAJORITY)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(10,TimeUnit.SECONDS))
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .applyToSslSettings(b->b.enabled(tls).invalidHostNameAllowed(false))
            .applyToSocketSettings(b->b.connectTimeout(5,TimeUnit.SECONDS).readTimeout(15,TimeUnit.SECONDS))
            .applyToClusterSettings(b->{
                b.serverSelectionTimeout(10,TimeUnit.SECONDS);
                if(endpoint.options().containsKey("replicaSet"))b.requiredReplicaSetName(endpoint.require("replicaSet"));
                if(endpoint.flag("directConnection",false))b.mode(com.mongodb.connection.ClusterConnectionMode.SINGLE);
            });
        if(endpoint.options().containsKey("user")) {
            String password=endpoint.options().get("password");
            if(password==null)throw new IllegalArgumentException("Missing MongoDB password");
            builder.credential(MongoCredential.createCredential(endpoint.require("user"),endpoint.option("authDatabase","admin"),password.toCharArray()));
        }
        return MongoClients.create(builder.build());
    }
    static BsonDocument collectionMetadata(MongoClient client,Endpoint endpoint) {
        BsonDocument meta=client.getDatabase(endpoint.require("database")).listCollections(BsonDocument.class)
            .filter(new BsonDocument("name",new BsonString(endpoint.require("table")))).first();
        if(meta==null || !"collection".equals(meta.getString("type").getValue()))
            throw new IllegalStateException("MongoDB collection must already exist and cannot be a view");
        BsonDocument options=meta.getDocument("options",new BsonDocument());
        if(options.getBoolean("capped",BsonBoolean.FALSE).getValue() || options.containsKey("timeseries"))
            throw new IllegalStateException("Capped and time-series collections are not supported");
        return meta;
    }
    static String identity(MongoClient client,Endpoint endpoint,BsonDocument meta) {
        BsonDocument configuration;
        try {
            configuration=client.getDatabase("admin").runCommand(
                new BsonDocument("replSetGetConfig",new BsonInt32(1)),BsonDocument.class).getDocument("config");
        } catch(MongoCommandException e) {
            if(e.getErrorCode()==13)throw new IllegalStateException(
                "MongoDB identity verification requires replSetGetConfig on the cluster; grant clusterMonitor or a custom read-only privilege");
            throw e;
        }
        return identity(endpoint,meta,configuration);
    }
    static String identity(Endpoint endpoint,BsonDocument meta,BsonDocument configuration) {
        BsonValue uuid=meta.getDocument("info").get("uuid");
        if(uuid==null)throw new IllegalStateException("MongoDB collection UUID unavailable");
        BsonValue replicaSetId=configuration.getDocument("settings",new BsonDocument()).get("replicaSetId");
        BsonValue setName=configuration.get("_id");
        if(replicaSetId==null || !replicaSetId.isObjectId() || setName==null || !setName.isString())
            throw new IllegalStateException("MongoDB immutable replica-set identity unavailable");
        return "mongodb:"+EventCodec.digest(List.of("replica-set-and-collection-v2",setName.asString().getValue(),
            replicaSetId.asObjectId().getValue().toHexString(),endpoint.require("database"),endpoint.require("table"),
            new BsonDocument("uuid",uuid).toJson(BsonValues.CANONICAL)));
    }
    static String schema(BsonDocument meta) {
        return EventCodec.digest(meta.getDocument("options",new BsonDocument()).toJson(BsonValues.CANONICAL));
    }
    static Inspection inspect(MongoClient client,Endpoint endpoint,Role role) {
        names(endpoint);
        BsonDocument hello=client.getDatabase("admin").runCommand(new BsonDocument("hello",new BsonInt32(1)),BsonDocument.class);
        if(!hello.containsKey("setName") || hello.containsKey("msg"))
            throw new IllegalStateException("MongoDB CDC requires the tested replica-set topology; standalone and mongos are not enabled");
        if(hello.getInt32("maxWireVersion").getValue()<17)throw new IllegalStateException("MongoDB 6.0 or newer is required");
        BsonDocument meta=collectionMetadata(client,endpoint);
        var collection=client.getDatabase(endpoint.require("database")).getCollection(endpoint.require("table"),BsonDocument.class);
        if(role==Role.SOURCE) {
            BsonDocument images=meta.getDocument("options",new BsonDocument()).getDocument("changeStreamPreAndPostImages",new BsonDocument());
            if(!images.getBoolean("enabled",BsonBoolean.FALSE).getValue())
                throw new IllegalStateException("Enable changeStreamPreAndPostImages on the source collection before capture");
            try(var probe=collection.watch().fullDocument(FullDocument.REQUIRED)
                .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED).maxAwaitTime(100,TimeUnit.MILLISECONDS).cursor()) {probe.tryNext();}
        } else {
            if(!"replace".equals(endpoint.option("documentMode","")))
                throw new IllegalArgumentException("MongoDB sink requires explicit documentMode=replace (whole mapped document ownership)");
            for(BsonDocument index:collection.listIndexes(BsonDocument.class))
                if(index.getBoolean("unique",BsonBoolean.FALSE).getValue() && !"_id_".equals(index.getString("name").getValue()))
                    throw new IllegalStateException("Secondary unique indexes are outside the MongoDB sink contract");
        }
        String version=client.getDatabase("admin").runCommand(new BsonDocument("buildInfo",new BsonInt32(1)),BsonDocument.class).getString("version").getValue();
        return new Inspection(identity(client,endpoint,meta),Map.of("_id","BSON","*","BSON"),List.of("_id"),
            Map.of("version",version,"topology","replica-set","schemaOptions",schema(meta),
                "empty",collection.find().limit(1).first()==null,"encoding","canonical-extended-json"));
    }
    @Override public Inspection inspect(Endpoint endpoint,Role role) {
        try(var client=connect(endpoint)){return inspect(client,endpoint,role);}
    }
    @Override public List<Map<String,Object>> readRows(Endpoint endpoint,int limit) {
        if(limit<1 || limit>100_001)throw new IllegalArgumentException("Invalid comparison row limit");
        try(var client=connect(endpoint)) {
            var rows=new ArrayList<Map<String,Object>>();long bytes=0;
            try(var cursor=client.getDatabase(endpoint.require("database")).getCollection(endpoint.require("table"),BsonDocument.class)
                .find().sort(new BsonDocument("_id",new BsonInt32(1))).limit(limit).iterator()) {
                while(cursor.hasNext()) {
                    var row=BsonValues.map(cursor.next());
                    bytes+=EventCodec.json(row).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                    if(bytes>32L*1024*1024)throw new IllegalStateException("MongoDB comparison exceeds the 32 MiB payload budget");
                    rows.add(row);
                }
            }
            return rows;
        }
    }
    @Override public SourceConnector openSource(Endpoint endpoint,SourceContext context){return new MongoSource(endpoint,context);}
    @Override public SinkConnector openSink(Endpoint endpoint,SinkContext context){return new MongoSink(endpoint);}
}
