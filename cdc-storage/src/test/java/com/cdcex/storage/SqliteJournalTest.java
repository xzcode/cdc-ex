package com.cdcex.storage;

import com.cdcex.core.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class SqliteJournalTest {
    @TempDir Path dir;
    ChangeEvent event(String id) {return new ChangeEvent(id,"a","t",ChangeEvent.Operation.INSERT,id,Map.of("id",1),null,Map.of("id",1),1);}
    @Test void durableReplayAndIndependentAck() throws Exception {
        Path file=dir.resolve("journal.db");
        try(var j=new SqliteJournal(file,100000,100)) {
            j.initialize("task"); j.append(List.of(event("a"),event("b"))); j.append(List.of(event("a")));
            assertEquals(2,j.stats().pending()); j.acknowledge(j.next(1)); assertEquals(1,j.stats().pending());
        }
        try(var j=new SqliteJournal(file,100000,100)) {
            j.initialize("task"); assertEquals("b",j.next(2).getFirst().event().id());
            assertThrows(IllegalStateException.class,()->j.initialize("changed"));
        }
    }
    @Test void overflowRollsBackWholeBatch() throws Exception {
        try(var j=new SqliteJournal(dir.resolve("limited.db"),100000,1)) {
            assertThrows(IllegalStateException.class,()->j.append(List.of(event("a"),event("b"))));
            assertEquals(0,j.stats().total());
        }
    }
    @Test void refusesSkippingPendingEvents() throws Exception {
        try(var j=new SqliteJournal(dir.resolve("skip.db"),100000,100)) {
            j.append(List.of(event("a"),event("b")));
            assertThrows(IllegalStateException.class,()->j.acknowledge(List.of(j.next(2).getLast())));
            assertEquals(2,j.stats().pending());
        }
    }
    @Test void rejectsSameIdentityWithDifferentPayloadAtomically() throws Exception {
        try(var j=new SqliteJournal(dir.resolve("collision.db"),100000,100)) {
            j.append(List.of(event("a")));
            var altered=new ChangeEvent("a","a","t",ChangeEvent.Operation.INSERT,"a",Map.of("id",1),null,Map.of("id",2),1);
            assertThrows(IllegalStateException.class,()->j.append(List.of(event("b"),altered)));
            assertEquals(1,j.stats().total()); assertEquals(1,j.stats().pending());
        }
    }
    @Test void accountingSurvivesReplayRollbackAndReopen() throws Exception {
        Path file=dir.resolve("account.db"); long bytes;
        try(var j=new SqliteJournal(file,100000,2)) {
            j.append(List.of(event("a"),event("a"),event("b")));
            bytes=j.stats().bytes();
            j.acknowledge(j.next(1));
            assertThrows(IllegalStateException.class,()->j.append(List.of(event("c"))));
            assertEquals(2,j.stats().total());assertEquals(bytes,j.stats().bytes());
        }
        try(var j=new SqliteJournal(file,100000,2)) {
            assertEquals(2,j.stats().total());assertEquals(1,j.stats().pending());assertEquals(bytes,j.stats().bytes());
            j.acknowledge(j.next(2)); assertEquals(0,j.stats().pending());
        }
    }
}
