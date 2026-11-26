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
    public TaskRuntime(SourceConnector source,SinkConnector sink,SqliteJournal journal,int batchSize,int maxFailures) {
        if(batchSize<1 || maxFailures<1) throw new IllegalArgumentException("Invalid runtime settings");
        this.source=source;this.sink=sink;this.journal=journal;this.batchSize=batchSize;this.maxFailures=maxFailures;
    }
    public State state(){return state;}
    public void requestStop(){stopping.set(true);}
    public void run() throws Exception {
        ExecutorService executor=Executors.newSingleThreadExecutor();
        Future<?> capture=executor.submit(()->{ source.run(journal::append); return null; });
        boolean failed=false;
        try {
            state=State.CAPTURING;
            int failures=0;
            while(!stopping.get()) {
                if(capture.isDone()) {capture.get();throw new IllegalStateException("Source stopped unexpectedly");}
                var batch=journal.next(batchSize);
                if(batch.isEmpty()) {Thread.sleep(150);continue;}
                try {
                    sink.apply(batch.stream().map(SqliteJournal.Entry::event).toList());
                    journal.acknowledge(batch); failures=0; state=State.CAPTURING;
                } catch(Exception e) {
                    state=State.RETRYING;
                    System.err.println("Sink batch failed; retained for retry ("+e.getClass().getSimpleName()+")");
                    if(++failures>=maxFailures) throw new IllegalStateException("Sink blocked; pending events retained");
                    Thread.sleep(Math.min(5000,250L*failures));
                }
            }
        } catch(Exception e) {failed=true;state=State.BLOCKED;throw e;}
        finally {
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
