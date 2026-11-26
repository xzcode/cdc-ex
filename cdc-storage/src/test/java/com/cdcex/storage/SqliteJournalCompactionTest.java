package com.cdcex.storage;

import com.cdcex.core.ChangeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SqliteJournalCompactionTest {
    @TempDir Path directory;
    private ChangeEvent event(String id,String value){return new ChangeEvent(id,"source","items",ChangeEvent.Operation.INSERT,"offset-"+id,Map.of("id",id),null,Map.of("id",id,"value",value),1);}
    @Test void compactedReceiptsDeduplicateReplayAcrossRestart() throws Exception {
        Path path=directory.resolve("journal.db");var delivered=event("first","x".repeat(10000));var pending=event("second","pending");
        try(var journal=new SqliteJournal(path,100000,100)){
            journal.initialize("generation");journal.append(List.of(delivered,pending));long before=journal.stats().bytes();
            journal.acknowledge(journal.next(1));journal.compact();
            assertTrue(journal.stats().bytes()<before);assertEquals(1,journal.stats().pending());
        }
        try(var journal=new SqliteJournal(path,100000,100)){
            journal.initialize("generation");journal.append(List.of(delivered,pending));
            assertEquals(2,journal.stats().total());assertEquals(1,journal.stats().pending());
            assertEquals("second",journal.next(10).getFirst().event().id());
            journal.acknowledge(journal.next(10));journal.compact();journal.compact();
            assertEquals(0,journal.stats().pending());
            journal.append(List.of(delivered,pending));assertEquals(0,journal.stats().pending());
        }
    }
    @Test void identityCollisionAfterCompactionRollsBackTheWholeCaptureBatch() throws Exception {
        try(var journal=new SqliteJournal(directory.resolve("journal.db"),100000,100)){
            journal.append(List.of(event("same","original")));journal.acknowledge(journal.next(1));journal.compact();
            var before=journal.stats();
            assertThrows(IllegalStateException.class,()->journal.append(List.of(event("new","not-committed"),event("same","different"))));
            assertEquals(before,journal.stats());assertTrue(journal.next(10).isEmpty());
            journal.append(List.of(event("same","original")));assertEquals(before,journal.stats());
        }
    }
    @Test void lifetimeReceiptLimitBlocksNewEventsWithoutDroppingReplayIdentity() throws Exception {
        Path path=directory.resolve("journal.db");
        try(var journal=new SqliteJournal(path,100000,1)){
            journal.append(List.of(event("first","data")));journal.acknowledge(journal.next(1));journal.compact();
            assertThrows(SqliteJournal.CapacityException.class,()->journal.append(List.of(event("second","data"))));
            journal.append(List.of(event("first","data")));assertEquals(1,journal.stats().total());assertEquals(0,journal.stats().pending());
        }
        // An explicit capacity increase preserves the generation and all old receipts.
        try(var journal=new SqliteJournal(path,100000,2)){
            journal.append(List.of(event("second","data")));assertEquals(1,journal.stats().pending());
            journal.append(List.of(event("first","data")));assertEquals(2,journal.stats().total());
        }
    }
}
