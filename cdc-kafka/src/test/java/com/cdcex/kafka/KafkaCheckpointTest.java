package com.cdcex.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaCheckpointTest {
    @TempDir Path directory;
    private final KafkaMetadata metadata=new KafkaMetadata("cluster","id","topic",2);
    @Test void checkpointSurvivesReopenAndNeverMovesBackwards() throws Exception {
        Path path=directory.resolve("source.json");
        var first=new KafkaCheckpoint(path,metadata,"group","earliest",true);
        first.save(metadata,Map.of(0,3L,1,9L));first.save(metadata,Map.of(0,5L));
        var restored=new KafkaCheckpoint(path,metadata,"group","earliest",false);
        assertEquals(Map.of(0,5L,1,9L),restored.state().nextOffsets());
        assertThrows(IllegalStateException.class,()->restored.save(metadata,Map.of(0,4L)));
        assertEquals(Map.of(0,5L,1,9L),new KafkaCheckpoint(path,metadata,"group","earliest",false).state().nextOffsets());
    }
    @Test void missingOrReplacedStateDoesNotSilentlyRestart() throws Exception {
        Path path=directory.resolve("source.json");
        assertThrows(IllegalStateException.class,()->new KafkaCheckpoint(path,metadata,"group","earliest",false));
        var first=new KafkaCheckpoint(path,metadata,"group","earliest",true);first.save(metadata,Map.of(0,0L));
        assertThrows(IllegalStateException.class,()->new KafkaCheckpoint(path,metadata,"other-group","earliest",false));
        assertThrows(IllegalStateException.class,()->new KafkaCheckpoint(path,new KafkaMetadata("cluster","replacement","topic",2),"group","earliest",false));
        assertThrows(IllegalStateException.class,()->new KafkaCheckpoint(path,metadata,"group","latest",false));
        assertThrows(IllegalStateException.class,()->new KafkaCheckpoint(path,metadata,"group","earliest",true));
    }
    @Test void expiryAndSinkRepartitionAreBlocked(){
        KafkaCheckpoint.requireRetained(5,5,20);KafkaCheckpoint.requireRetained(20,5,20);
        assertThrows(IllegalStateException.class,()->KafkaCheckpoint.requireRetained(4,5,20));
        assertThrows(IllegalStateException.class,()->KafkaCheckpoint.requireRetained(21,5,20));
        metadata.requireSameTopic(new KafkaMetadata("cluster","id","topic",3));
        assertThrows(IllegalStateException.class,()->metadata.requireSameSink(new KafkaMetadata("cluster","id","topic",3)));
    }
    @Test void validJsonOffsetCorruptionIsDetectedBeforeCapture() throws Exception {
        Path path=directory.resolve("source.json");var checkpoint=new KafkaCheckpoint(path,metadata,"group","earliest",true);
        checkpoint.save(metadata,Map.of(0,5L));
        Files.writeString(path,Files.readString(path).replace("\"0\":5","\"0\":8"));
        assertThrows(IllegalStateException.class,()->new KafkaCheckpoint(path,metadata,"group","earliest",false));
    }
}
