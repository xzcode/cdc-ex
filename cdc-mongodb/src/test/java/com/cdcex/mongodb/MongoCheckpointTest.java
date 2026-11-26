package com.cdcex.mongodb;

import org.bson.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

class MongoCheckpointTest {
    @TempDir Path directory;
    @Test void snapshotBoundaryAndCommittedTokenSurviveReopen() throws Exception {
        var checkpoint=new MongoCheckpoint(directory);
        var initial=new MongoCheckpoint.State("uuid","initial","snapshot",new BsonTimestamp(123,4),null);
        checkpoint.write(initial);
        assertEquals(initial,new MongoCheckpoint(directory).read());
        BsonDocument token=new BsonDocument("_data",new BsonString("token"));
        checkpoint.write(initial.stream(token));
        var reopened=new MongoCheckpoint(directory).read();
        assertEquals(initial.boundary(),reopened.boundary());
        assertEquals(token,reopened.token());
        assertEquals("stream",reopened.phase());
    }
    @Test void corruptOrUnknownCheckpointBlocksInsteadOfChoosingNow() throws Exception {
        Files.writeString(directory.resolve("mongodb-checkpoint.json"),"{\"version\":2}");
        assertThrows(IllegalStateException.class,()->new MongoCheckpoint(directory).read());
        Files.writeString(directory.resolve("mongodb-checkpoint.json"),"broken");
        assertThrows(RuntimeException.class,()->new MongoCheckpoint(directory).read());
    }
}
