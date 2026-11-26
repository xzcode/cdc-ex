package com.cdcex.postgresql;

import com.cdcex.core.*;
import com.cdcex.debezium.AtomicOffsetStore;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

final class PostgresSource implements SourceConnector {
    private final PostgresConfig cfg;
    private final ConnectorProvider.SourceContext context;
    private final PostgresSchema schema;
    private volatile DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private volatile boolean closed;
    PostgresSource(PostgresConfig cfg,ConnectorProvider.SourceContext context) throws Exception {
        this.cfg=cfg;this.context=context;
        if(!Set.of("initial","no_data").contains(context.snapshotMode()))throw new IllegalArgumentException("Unsupported PostgreSQL snapshot mode");
        Files.createDirectories(context.directory());
        try(Connection c=cfg.connect()) {
            schema=PostgresSchema.read(c,cfg,false);
            var slot=PostgresSchema.sourcePreflight(c,cfg,schema,true);
            Path offsets=context.directory().resolve("pg-offsets.json"),identity=context.directory().resolve("pg-source.identity");
            String fingerprint=EventCodec.digest(Arrays.asList(schema.identity(),schema.columns(),schema.keys(),cfg.slot(),cfg.publication(),context.taskId(),context.snapshotMode()));
            if(context.fresh()) {
                if(Files.exists(offsets)||Files.exists(identity))throw new IllegalStateException("Orphaned PostgreSQL state; explicit recovery required");
                AtomicOffsetStore.writeAtomic(identity,fingerprint.getBytes(StandardCharsets.UTF_8));
            } else {
                if(!Files.isRegularFile(identity)||!Files.readString(identity).equals(fingerprint))throw new IllegalStateException("PostgreSQL source identity or schema changed");
                if(!Files.isRegularFile(offsets))throw new IllegalStateException("PostgreSQL durable offsets missing; refusing to reset");
                if(!AtomicOffsetStore.snapshotComplete(offsets))throw new IllegalStateException("Interrupted PostgreSQL snapshot requires explicit new generation and reconciliation");
                BigInteger maximum=BigInteger.ZERO;
                for(var offset:AtomicOffsetStore.readOffsets(offsets)) for(String key:List.of("lsn","lsn_proc","lsn_commit")) {
                    Object value=offset.get(key);if(value instanceof Number n)maximum=maximum.max(unsigned(n.longValue()));
                }
                if(maximum.signum()==0||lsn(slot.restartLsn()).compareTo(maximum)>0)throw new IllegalStateException("PostgreSQL WAL checkpoint is no longer retained; refusing to skip changes");
            }
        }
    }
    static BigInteger unsigned(long value) {return new BigInteger(Long.toUnsignedString(value));}
    static BigInteger lsn(String text) {String[] parts=text.split("/",-1);if(parts.length!=2)throw new IllegalArgumentException("Invalid PostgreSQL LSN");return new BigInteger(parts[0],16).shiftLeft(32).add(new BigInteger(parts[1],16));}
    private Map<String,Object> row(Object value) {
        if(value==null)return null;
        if(!(value instanceof Struct struct))throw new IllegalStateException("Expected PostgreSQL row structure");
        Map<String,Object> result=new LinkedHashMap<>();
        for(Field field:struct.schema().fields())result.put(field.name(),PostgresValues.fromConnect(field.schema(),struct.getWithoutDefault(field.name())));
        return result;
    }
    ChangeEvent normalize(SourceRecord record) {
        if(record.value()==null)return null;
        if(!(record.value() instanceof Struct envelope)||envelope.schema().field("source")==null)throw new IllegalStateException("Unexpected PostgreSQL capture record");
        Struct source=envelope.getStruct("source");
        if(!cfg.database().equals(source.getString("db"))||!cfg.schema().equals(source.getString("schema"))||!cfg.table().equals(source.getString("table")))throw new IllegalStateException("Unexpected PostgreSQL source namespace");
        String op=envelope.getString("op");
        ChangeEvent.Operation operation=switch(op) {
            case "r"->ChangeEvent.Operation.SNAPSHOT;case "c"->ChangeEvent.Operation.INSERT;
            case "u"->ChangeEvent.Operation.UPDATE;case "d"->ChangeEvent.Operation.DELETE;
            default->throw new IllegalStateException("PostgreSQL operation requires explicit recovery policy");
        };
        Map<String,Object> key=row(record.key()),before=row(envelope.get("before")),after=row(envelope.get("after"));
        Set<String> fields=new HashSet<>(schema.columns().stream().map(PostgresSchema.Column::name).toList());
        if(key==null||!key.keySet().equals(Set.copyOf(schema.keys())))throw new IllegalStateException("PostgreSQL key schema changed");
        if(after!=null&&!after.keySet().equals(fields)||before!=null&&!before.keySet().equals(fields))throw new IllegalStateException("PostgreSQL row schema changed");
        for(Map<String,Object> row:Arrays.asList(before,after))if(row!=null)for(var column:schema.columns()) {
            Object value=row.get(column.name());
            if(value==null&&!column.nullable())throw new IllegalStateException("Missing non-null PostgreSQL column image");
            PostgresValues.validate(column.type(),value);
        }
        if(operation==ChangeEvent.Operation.UPDATE&&before==null)throw new IllegalStateException("Full PostgreSQL before image required");
        String position=EventCodec.canonicalJson(List.of(record.sourcePartition(),record.sourceOffset()));
        String id=EventCodec.digest(Arrays.asList(schema.identity(),position,op,key,operation==ChangeEvent.Operation.SNAPSHOT?after:null));
        Long time=source.getInt64("ts_ms");
        return new ChangeEvent(id,cfg.database(),cfg.table(),operation,position,key,before,after,time==null?0:time);
    }
    Properties properties() {
        Properties p=new Properties();var e=cfg.endpoint();
        p.setProperty("name","cdc-ex-postgres-"+context.taskId());p.setProperty("connector.class","io.debezium.connector.postgresql.PostgresConnector");
        p.setProperty("database.hostname",e.require("host"));p.setProperty("database.port",Integer.toString(e.integer("port",5432)));
        p.setProperty("database.user",e.require("user"));p.setProperty("database.password",e.require("password"));p.setProperty("database.dbname",cfg.database());
        p.setProperty("database.sslmode",e.option("sslMode","verify-full"));if(e.options().containsKey("sslRootCert"))p.setProperty("database.sslrootcert",e.require("sslRootCert"));
        p.setProperty("topic.prefix","cdc-ex-pg-"+context.taskId());p.setProperty("plugin.name","pgoutput");p.setProperty("slot.name",cfg.slot());p.setProperty("slot.drop.on.stop","false");
        p.setProperty("publication.name",cfg.publication());p.setProperty("publication.autocreate.mode","disabled");
        p.setProperty("table.include.list",Pattern.quote(cfg.schema()+"."+cfg.table()));
        p.setProperty("offset.storage",AtomicOffsetStore.class.getName());p.setProperty("offset.storage.file.filename",context.directory().resolve("pg-offsets.json").toString());p.setProperty("offset.flush.interval.ms","0");
        p.setProperty("snapshot.mode",context.snapshotMode());p.setProperty("snapshot.isolation.mode","serializable");
        p.setProperty("decimal.handling.mode","precise");p.setProperty("time.precision.mode","adaptive_time_microseconds");p.setProperty("binary.handling.mode","bytes");
        p.setProperty("unavailable.value.placeholder",PostgresValues.UNAVAILABLE);p.setProperty("tombstones.on.delete","false");p.setProperty("include.schema.changes","false");
        p.setProperty("max.batch.size","256");p.setProperty("max.queue.size","1024");p.setProperty("record.processing.order","ORDERED");p.setProperty("record.processing.threads","1");
        p.setProperty("event.processing.failure.handling.mode","fail");p.setProperty("slot.max.retries","2");p.setProperty("slot.retry.delay.ms","1000");
        return p;
    }
    @Override public void run(DurableReceiver receiver) throws Exception {
        AtomicReference<Throwable> failure=new AtomicReference<>();
        var built=DebeziumEngine.create(ChangeEventFormat.of(Connect.class)).using(properties())
            .using((success,message,error)->{if(!success)failure.set(error==null?new IllegalStateException("PostgreSQL capture failed"):error);})
            .notifying((List<RecordChangeEvent<SourceRecord>> records,DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer)->{
                List<ChangeEvent> batch=new ArrayList<>();for(var record:records){ChangeEvent event=normalize(record.record());if(event!=null)batch.add(event);}
                try {
                    try(Connection c=cfg.connect()){schema.requireSame(PostgresSchema.read(c,cfg,false));PostgresSchema.sourcePreflight(c,cfg,schema,false);}
                    receiver.accept(batch);
                }catch(InterruptedException e){throw e;}catch(Exception e){throw new IllegalStateException("PostgreSQL durable capture rejected",e);}
                for(var record:records)committer.markProcessed(record);committer.markBatchFinished();
            }).build();
        synchronized(this){if(closed){built.close();return;}engine=built;}
        try {built.run();if(failure.get()!=null)throw new IllegalStateException("PostgreSQL capture failed",failure.get());}
        finally{synchronized(this){engine=null;}}
    }
    @Override public void close() throws Exception {DebeziumEngine<RecordChangeEvent<SourceRecord>> active;synchronized(this){closed=true;active=engine;}if(active!=null)active.close();}
}
