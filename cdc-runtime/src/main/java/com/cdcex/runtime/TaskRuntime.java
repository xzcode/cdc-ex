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
    private final RateSampler rates=new RateSampler();
    private volatile Long lastDeliveryDelayMillis;
    public record Health(State state,int retries,long lastProgressMillis,long sourceTimeMillis,String errorCode,SqliteJournal.Stats journal,
                         Double capturedPerSecond,Double deliveredPerSecond,Long rateWindowMillis,Long lastDeliveryDelayMillis){
        public static Health retained(SqliteJournal.Stats stats){return new Health(State.STOPPED,0,0,0,null,stats,null,null,null,null);}
    }
    public TaskRuntime(SourceConnector source,SinkConnector sink,SqliteJournal journal,int batchSize,int maxFailures) {
        if(batchSize<1 || maxFailures<1) throw new IllegalArgumentException("Invalid runtime settings");
        this.source=source;this.sink=sink;this.journal=journal;this.batchSize=batchSize;this.maxFailures=maxFailures;
    }
    public State state(){return state;}
    public Health health() throws Exception{
        SqliteJournal.Stats stats=journal.stats();State current=state;
        RateSampler.Rates rate=current==State.CAPTURING||current==State.RETRYING?rates.sample(System.nanoTime(),stats.total(),stats.total()-stats.pending()):null;
        return new Health(current,retries,lastProgressMillis,sourceTimeMillis,errorCode,stats,rate==null?null:rate.capturedPerSecond(),rate==null?null:rate.deliveredPerSecond(),rate==null?null:rate.windowMillis(),lastDeliveryDelayMillis);
    }
    public void requestStop(){stopping.set(true);}
    public void run() throws Exception {
        SqliteJournal.Stats baseline=journal.stats();rates.sample(System.nanoTime(),baseline.total(),baseline.total()-baseline.pending());
        ExecutorService executor=Executors.newSingleThreadExecutor();
        Future<?> capture=executor.submit(()->{ source.run(batch->{
            long bytes=0;
            for(ChangeEvent event:batch){long size=EventCodec.json(event).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;if(size>8L*1024*1024||(bytes+=size)>64L*1024*1024)throw new IllegalStateException("Capture batch exceeds the supported event or batch byte limit");}
            // A graceful stop can race with a native batch or heartbeat already handed to us.
            // Persist it if capacity permits. No successful return (and therefore no source
            // acknowledgement) is allowed for a batch that could not be committed.
            while(true){
                try{journal.append(batch);return;}
                catch(SqliteJournal.CapacityException capacity){
                    if(stopping.get()||journal.stats().pending()==0)throw capacity;
                    Thread.sleep(250);
                }
            }
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
                journal.acknowledge(batch);
                long deliveredAt=System.currentTimeMillis();ChangeEvent last=batch.getLast().event();
                long eventTime=last.sourceTimeMillis();
                lastDeliveryDelayMillis=last.operation()!=ChangeEvent.Operation.SNAPSHOT&&eventTime>0&&eventTime<=deliveredAt?deliveredAt-eventTime:null;
                lastProgressMillis=deliveredAt;sourceTimeMillis=eventTime;
                journal.compact();failures=0;retries=0;errorCode=null;state=State.CAPTURING;
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
