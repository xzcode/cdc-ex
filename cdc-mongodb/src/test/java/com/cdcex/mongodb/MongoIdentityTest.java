package com.cdcex.mongodb;

import com.cdcex.core.Endpoint;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class MongoIdentityTest {
    private static final Endpoint ENDPOINT=new Endpoint("MONGODB",Map.of("database","src","table","items"));
    private static BsonDocument configuration(String id,int version) {
        return new BsonDocument("_id",new BsonString("same-name"))
            .append("version",new BsonInt32(version)).append("settings",new BsonDocument(
                "replicaSetId",new BsonObjectId(new ObjectId(id))));
    }
    private static BsonDocument metadata() {
        return new BsonDocument("info",new BsonDocument("uuid",new BsonBinary((byte)4,new byte[16])));
    }
    @Test void replicaReconfigurationIsStableButARestoredCollectionOnAnotherSetIsFenced() {
        var metadata=metadata();
        String first=MongoProvider.identity(ENDPOINT,metadata,configuration("507f1f77bcf86cd799439011",1));
        assertEquals(first,MongoProvider.identity(ENDPOINT,metadata,configuration("507f1f77bcf86cd799439011",2)));
        assertNotEquals(first,MongoProvider.identity(ENDPOINT,metadata,configuration("507f1f77bcf86cd799439012",1)));
    }
    @Test void MissingImmutableReplicaIdentityFailsClosed() {
        assertThrows(IllegalStateException.class,()->MongoProvider.identity(ENDPOINT,metadata(),
            new BsonDocument("_id",new BsonString("same-name"))));
    }
}
