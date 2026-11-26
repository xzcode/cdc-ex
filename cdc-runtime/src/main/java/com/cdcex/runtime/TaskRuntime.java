package com.cdcex.runtime;

import com.cdcex.core.*;
import com.cdcex.storage.SqliteJournal;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
public final class TaskRuntime {
    public enum State { STARTING, CAPTURING, RETRYING, STOPPING, STOPPED, BLOCKED }
    private final SourceConnector source;
    private final SinkConnector sink;
    private final SqliteJournal journal;
    private final int batchSize,maxFailures;
    private final AtomicBoolean stopping=new AtomicBoolean();
    private volatile State state=State.STARTING;
    private volatile int retries;
    private volatile long lastProgressMillis,sourceTimeMillis;
    private volatile String errorCode;
    public record Health(State state,int retries,long lastProgressMillis,long sourceTimeMillis,String errorCode,SqliteJournal.Stats journal){}
    public TaskRuntime(SourceConnector source,SinkConnector sink,SqliteJournal journal,int batchSize,int maxFailures) {
        if(batchSize<1 || maxFailures<1) throw new IllegalArgumentException("Invalid runtime settings");
        this.source=source;this.sink=sink;this.journal=journal;this.batchSize=batchSize;this.maxFailures=maxFailures;
    }
    public State state(){return state;}
    public Health health() throws Exception{return new Health(state,retries,lastProgressMillis,sourceTimeMillis,errorCode,journal.stats());}
    public void requestStop(){stopping.set(true);}
    public void run() throws Exception {
        ExecutorService executor=Executors.newSingleThreadExecutor();
        Future<?> capture=executor.submit(()->{ source.run(batch->{
            long bytes=0;
            for(ChangeEvent event:batch){long size=EventCodec.json(event).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;if(size>8L*1024*1024||(bytes+=size)>64L*1024*1024)throw new IllegalStateException("Capture batch exceeds the supported event or batch byte limit");}
            while(!stopping.get()){
                try{journal.append(batch);return;}
                catch(SqliteJournal.CapacityException capacity){
                    if(journal.stats().pending()==0)throw capacity;
                    Thread.sleep(250);
                }
            }
            throw new InterruptedException("Task stopped before accepting capture batch");
        }); return null; });
        boolean failed=false;
        try {
            state=State.CAPTURING;
            int failures=0;
            while(!stopping.get()) {
                if(capture.isDone()) {capture.get();if(!source.finite())throw new IllegalStateException("Source stopped unexpectedly");}
                var batch=journal.next(batchSize);
                if(batch.isEmpty()) {if(capture.isDone()&&source.finite())break;Thread.sleep(150);continue;}
                try {
                    if(sink instanceof SequencedSinkConnector sequenced)sequenced.applySequenced(batch.stream().map(e->new SequencedSinkConnector.Delivery(e.sequence(),e.event())).toList());
                    else sink.apply(batch.stream().map(SqliteJournal.Entry::event).toList());
                } catch(Exception e) {
                    state=State.RETRYING;errorCode=Failure.describe(e).code();retries=failures+1;
                    System.err.println("Sink batch failed; retained for retry ("+e.getClass().getSimpleName()+")");
                    if(++failures>=maxFailures) throw new IllegalStateException("Sink blocked; pending events retained",e);
                    Thread.sleep(Math.min(5000,250L*failures));
                    continue;
                }
                // Journal failures are fatal storage errors, not a reason to repeat a committed acknowledgement.
                journal.acknowledge(batch);journal.compact();failures=0;retries=0;errorCode=null;state=State.CAPTURING;
                lastProgressMillis=System.currentTimeMillis();sourceTimeMillis=batch.getLast().event().sourceTimeMillis();
            }
        } catch(Exception e) {failed=true;state=State.BLOCKED;if(errorCode==null)errorCode="CAPTURE_"+e.getClass().getSimpleName().toUpperCase(java.util.Locale.ROOT);throw e;}
        finally {
            stopping.set(true);
            if(!failed) state=State.STOPPING;
            try {source.close();} finally {
                executor.shutdownNow();
                if(!executor.awaitTermination(30,TimeUnit.SECONDS)) throw new IllegalStateException("Capture worker did not stop");
                sink.close();
                if(!failed) state=State.STOPPED;
            }
        }
    }
}
