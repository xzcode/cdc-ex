package com.cdcex.runtime;

import com.cdcex.core.*;
import com.cdcex.storage.SqliteJournal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class TaskRuntimeTest {
    @TempDir Path dir;
    static ChangeEvent event(String id) { return new ChangeEvent(id,"db","items",ChangeEvent.Operation.INSERT,id,Map.of("id",id),null,Map.of("id",id),1); }
    static final class Source implements SourceConnector {
        private final CountDownLatch closed=new CountDownLatch(1);
        public void run(DurableReceiver receiver) throws Exception {receiver.accept(List.of(event("a"),event("b")));closed.await();}
        public void close(){closed.countDown();}
    }
    @Test void retriesWithoutPrematureAck() throws Exception {
        try(var journal=new SqliteJournal(dir.resolve("retry.db"),100000,100)){
            AtomicInteger tries=new AtomicInteger();
            CountDownLatch delivered=new CountDownLatch(1);
            SinkConnector sink=new SinkConnector(){
                public void apply(List<ChangeEvent> batch) throws Exception {
                    assertEquals(List.of("a","b"),batch.stream().map(ChangeEvent::id).toList());
                    assertEquals(2,journal.stats().pending());
                    if(tries.incrementAndGet()==1) throw new java.io.IOException("temporary failure");
                    delivered.countDown();
                }
            };
            var task=new TaskRuntime(new Source(),sink,journal,10,3);
            try(var executor=Executors.newSingleThreadExecutor()){
                var future=executor.submit(()->{task.run();return null;});
                try{assertTrue(delivered.await(5,TimeUnit.SECONDS));}
                finally{task.requestStop();}
                future.get(5,TimeUnit.SECONDS);
            }
            assertEquals(2,tries.get());assertEquals(0,journal.stats().pending());assertEquals(TaskRuntime.State.STOPPED,task.state());
        }
    }
    @Test void permanentFailureLeavesBatchForRecovery() throws Exception {
        Path path=dir.resolve("blocked.db");
        try(var journal=new SqliteJournal(path,100000,100)){
            SinkConnector sink=new SinkConnector(){public void apply(List<ChangeEvent> batch) throws Exception {throw new java.io.IOException("offline");}};
            var task=new TaskRuntime(new Source(),sink,journal,10,2);
            assertThrows(IllegalStateException.class,task::run);
            assertEquals(TaskRuntime.State.BLOCKED,task.state());
            assertEquals(2,journal.stats().pending());
        }
        try(var journal=new SqliteJournal(path,100000,100)){
            assertEquals(List.of("a","b"),journal.next(10).stream().map(x->x.event().id()).toList());
        }
    }
}
