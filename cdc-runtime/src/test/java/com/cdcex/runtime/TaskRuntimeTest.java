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
    @Test void gracefulStopPersistsFinalCaptureBatchBeforeAcknowledgement() throws Exception {
        Path path=dir.resolve("stop-race.db");
        try(var journal=new SqliteJournal(path,100000,100)){
            CountDownLatch running=new CountDownLatch(1),finishCapture=new CountDownLatch(1),accepted=new CountDownLatch(1);
            java.util.concurrent.atomic.AtomicReference<Exception> captureFailure=new java.util.concurrent.atomic.AtomicReference<>();
            SourceConnector source=new SourceConnector(){
                public void run(DurableReceiver receiver) throws Exception{
                    running.countDown();finishCapture.await();
                    try{receiver.accept(List.of(event("last")));receiver.accept(List.of());}
                    catch(Exception e){captureFailure.set(e);throw e;}
                    finally{accepted.countDown();}
                }
                public void close() throws Exception{
                    finishCapture.countDown();
                    if(!accepted.await(5,TimeUnit.SECONDS))throw new IllegalStateException("Capture did not finish");
                    if(captureFailure.get()!=null)throw captureFailure.get();
                }
            };
            var task=new TaskRuntime(source,new SinkConnector(){public void apply(List<ChangeEvent> events){fail("A stopped task must retain the final batch");}},journal,10,3);
            try(var executor=Executors.newSingleThreadExecutor()){
                var future=executor.submit(()->{task.run();return null;});
                try{assertTrue(running.await(5,TimeUnit.SECONDS));}
                finally{task.requestStop();}
                future.get(10,TimeUnit.SECONDS);
            }
            assertNull(captureFailure.get());assertEquals(TaskRuntime.State.STOPPED,task.state());
            assertEquals(1,journal.stats().pending());
        }
        try(var journal=new SqliteJournal(path,100000,100)){
            assertEquals("last",journal.next(1).getFirst().event().id());
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
