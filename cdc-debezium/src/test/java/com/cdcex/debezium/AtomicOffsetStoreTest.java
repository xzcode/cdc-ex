package com.cdcex.debezium;

import com.cdcex.core.EventCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class AtomicOffsetStoreTest {
    @TempDir Path dir;
    private byte[] state(Map<String,Object> offset) throws Exception {
        var json=new ObjectMapper();String key=Base64.getEncoder().encodeToString("[\"task\",{\"server\":\"source\"}]".getBytes(StandardCharsets.UTF_8));
        String value=Base64.getEncoder().encodeToString(json.writeValueAsBytes(offset));
        var entries=Map.of(key,value);
        return json.writeValueAsBytes(Map.of("version",1,"entries",entries,"sha256",EventCodec.digest(entries)));
    }
    @Test void distinguishesIncompleteSnapshotFromDurableLastRecord() throws Exception {
        Path file=dir.resolve("offsets.json");AtomicOffsetStore.writeAtomic(file,state(Map.of("lsn",42,"snapshot","INITIAL","last_snapshot_record",false)));
        assertFalse(AtomicOffsetStore.snapshotComplete(file));
        AtomicOffsetStore.writeAtomic(file,state(Map.of("lsn",43,"snapshot","INITIAL","last_snapshot_record",true)));
        assertTrue(AtomicOffsetStore.snapshotComplete(file));assertEquals(43,AtomicOffsetStore.readOffsets(file).getFirst().get("lsn"));
    }
    @Test void refusesEmptyCorruptOrLegacyCheckpoint() throws Exception {
        Path file=dir.resolve("offsets.json");Files.writeString(file,"");assertThrows(Exception.class,()->AtomicOffsetStore.readOffsets(file));
        Files.writeString(file,"{\"version\":1,\"entries\":{}}");assertThrows(Exception.class,()->AtomicOffsetStore.readOffsets(file));
        Files.writeString(file,new String(state(Map.of("lsn",42)),StandardCharsets.UTF_8).replace("sha256","wrong_checksum"));assertThrows(Exception.class,()->AtomicOffsetStore.readOffsets(file));
        Files.write(file,new byte[]{(byte)0xac,(byte)0xed,0,5});assertThrows(Exception.class,()->AtomicOffsetStore.readOffsets(file));
    }
    @Test void replacementRemovesTemporaryFileAndPreservesLatestCheckpoint() throws Exception {
        Path file=dir.resolve("offsets.json");AtomicOffsetStore.writeAtomic(file,state(Map.of("lsn",7,"snapshot",false)));
        AtomicOffsetStore.writeAtomic(file,state(Map.of("lsn",8)));
        assertEquals(8,AtomicOffsetStore.readOffsets(file).getFirst().get("lsn"));assertTrue(AtomicOffsetStore.snapshotComplete(file));
        try(var paths=Files.list(dir)){assertEquals(List.of(file),paths.toList());}
    }
}
