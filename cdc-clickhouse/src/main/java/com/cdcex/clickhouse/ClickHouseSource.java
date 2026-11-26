package com.cdcex.clickhouse;

import com.cdcex.core.*;
import java.math.BigInteger;
import java.util.*;

/** Explicit finite scan, append cursor, or producer changelog. This is never native ClickHouse CDC. */
final class ClickHouseSource implements SourceConnector {
    private final Endpoint endpoint;
    private final ConnectorProvider.SourceContext context;
    private final String mode;
    private volatile boolean closed;
    private volatile Thread worker;
    ClickHouseSource(Endpoint endpoint,ConnectorProvider.SourceContext context) {
        this.endpoint=endpoint;this.context=context;mode=ClickHouseProvider.sourceMode(endpoint,context.snapshotMode());
        if(!mode.equals(context.snapshotMode()))throw new IllegalArgumentException("ClickHouse source mode and task snapshotMode must match");
    }
    @Override public boolean finite(){return mode.equals("snapshot");}
    @Override public void run(DurableReceiver receiver)throws Exception {
        worker=Thread.currentThread();
        try(var http=new ClickHouseHttp(endpoint)) {
            if(closed)return;
            var table=ClickHouseProvider.table(http,endpoint);ClickHouseProvider.validateSource(endpoint,table,mode);
            var checkpoint=new ClickHouseCheckpoint(context.directory());
            ClickHouseCheckpoint.State state=checkpoint.exists()?checkpoint.read():null;
            if(state==null) {
                if(!context.fresh())throw new IllegalStateException("Missing ClickHouse checkpoint; reset refused");
                List<String> cursor=mode.equals("changelog")?List.of(endpoint.option("startSequence","0")):null;
                if(cursor!=null && new BigInteger(cursor.getFirst()).signum()<0)throw new IllegalArgumentException("Negative ClickHouse start sequence");
                state=new ClickHouseCheckpoint.State(1,table.identity(),table.fingerprint(),mode,mode.equals("snapshot")?"ready":"stream",UUID.randomUUID().toString(),cursor);
                checkpoint.write(state);
            }
            if(!state.identity().equals(table.identity()) || !state.schema().equals(table.fingerprint()) || !state.mode().equals(mode))
                throw new IllegalStateException("ClickHouse source identity, schema or mode changed");
            if(mode.equals("snapshot"))snapshot(http,table,checkpoint,state,receiver);
            else poll(http,table,checkpoint,state,receiver);
        } catch(InterruptedException e) {if(!closed){Thread.currentThread().interrupt();throw e;}}
        finally {worker=null;}
    }
    private void snapshot(ClickHouseHttp http,ClickHouseProvider.Table table,ClickHouseCheckpoint checkpoint,
                          ClickHouseCheckpoint.State state,DurableReceiver receiver)throws Exception {
        if(state.phase().equals("complete"))return;
        if(!state.phase().equals("ready"))throw new IllegalStateException("Interrupted ClickHouse snapshot requires an explicit reconciled generation; source history cannot be reconstructed");
        checkpoint.write(state.phase("running"));
        List<String> keys=ClickHouseProvider.columns(endpoint,"keyColumns");
        String order=String.join(",",keys.stream().map(ClickHouseProvider::quote).toList());
        List<ChangeEvent> batch=new ArrayList<>();String[] previous={null};
        http.scan("SELECT * FROM "+ClickHouseProvider.qualified(endpoint)+" ORDER BY "+order,Map.of(),raw->{
            if(closed)throw new InterruptedException("ClickHouse snapshot stopped");
            Map<String,Object> row=ClickHouseProvider.typed(raw,table.fields());Map<String,Object> key=key(row,keys);
            String serialized=EventCodec.canonicalJson(key);
            if(serialized.equals(previous[0]))throw new IllegalStateException("ClickHouse snapshot key is not unique");
            previous[0]=serialized;
            String position=EventCodec.canonicalJson(Map.of("snapshotGeneration",state.generation()));
            batch.add(new ChangeEvent(EventCodec.digest(List.of(table.identity(),position,key)),endpoint.require("database"),endpoint.require("table"),
                ChangeEvent.Operation.SNAPSHOT,position,key,null,row,0));
            if(batch.size()==128){receiver.accept(List.copyOf(batch));batch.clear();}
        });
        if(!batch.isEmpty())receiver.accept(batch);
        if(!closed)checkpoint.write(state.phase("complete"));
    }
    private void poll(ClickHouseHttp http,ClickHouseProvider.Table table,ClickHouseCheckpoint checkpoint,
                      ClickHouseCheckpoint.State initial,DurableReceiver receiver)throws Exception {
        List<String> cursorColumns=mode.equals("changelog")?List.of("sequence"):ClickHouseProvider.columns(endpoint,"cursorColumns");
        List<String> keys=mode.equals("changelog")?List.of():ClickHouseProvider.columns(endpoint,"keyColumns");
        ClickHouseCheckpoint.State state=initial;
        while(!closed) {
            if(!state.schema().equals(ClickHouseProvider.table(http,endpoint).fingerprint()))throw new IllegalStateException("ClickHouse source schema or identity changed");
            Query query=cursorQuery(endpoint,table,cursorColumns,state.cursor());
            var rows=http.query(query.sql(),query.parameters());
            if(rows.isEmpty()){Thread.sleep(400);continue;}
            List<ChangeEvent> batch=new ArrayList<>();List<String> previous=state.cursor();
            for(var raw:rows) {
                var row=ClickHouseProvider.typed(raw,table.fields());
                List<String> next=cursorColumns.stream().map(n->{Object v=row.get(n);if(v==null)throw new IllegalStateException("ClickHouse cursor cannot be null");return v.toString();}).toList();
                if(next.equals(previous))throw new IllegalStateException("ClickHouse cursor tuple is not unique");
                if(mode.equals("changelog")) {
                    BigInteger expected=new BigInteger(previous.getFirst()).add(BigInteger.ONE);
                    if(!new BigInteger(next.getFirst()).equals(expected))throw new IllegalStateException("ClickHouse changelog gap or expired history; cursor not advanced");
                    batch.add(changelog(endpoint,table.identity(),row));
                } else {
                    Map<String,Object> key=key(row,keys);String position=EventCodec.canonicalJson(next);
                    batch.add(new ChangeEvent(EventCodec.digest(List.of(table.identity(),position)),endpoint.require("database"),endpoint.require("table"),
                        ChangeEvent.Operation.INSERT,position,key,null,row,0));
                }
                previous=next;
            }
            receiver.accept(batch);
            state=state.cursor(previous);checkpoint.write(state);
        }
    }
    record Query(String sql,Map<String,String> parameters){}
    static Query cursorQuery(Endpoint endpoint,ClickHouseProvider.Table table,List<String> columns,List<String> cursor) {
        String tuple="tuple("+String.join(",",columns.stream().map(ClickHouseProvider::quote).toList())+")";
        StringBuilder sql=new StringBuilder("SELECT * FROM ").append(ClickHouseProvider.qualified(endpoint));
        Map<String,String> parameters=new LinkedHashMap<>();
        if(cursor!=null) {
            if(cursor.size()!=columns.size())throw new IllegalStateException("ClickHouse checkpoint cursor arity changed");
            List<String> binds=new ArrayList<>();
            for(int i=0;i<columns.size();i++) {
                String type=table.fields().get(columns.get(i));
                if(!Set.of("Int64","UInt64","String").contains(type))throw new IllegalArgumentException("Unsupported cursor type");
                String name="cursor"+i;binds.add("{"+name+":"+type+"}");parameters.put(name,cursor.get(i));
            }
            sql.append(" WHERE ").append(tuple).append(" > tuple(").append(String.join(",",binds)).append(')');
        }
        sql.append(" ORDER BY ").append(tuple).append(" LIMIT 128");return new Query(sql.toString(),parameters);
    }
    static Map<String,Object> key(Map<String,Object> row,List<String> names) {
        Map<String,Object> key=new LinkedHashMap<>();
        names.forEach(name->{if(!row.containsKey(name)||row.get(name)==null)throw new IllegalArgumentException("ClickHouse key is missing or null");key.put(name,row.get(name));});
        return key;
    }
    static ChangeEvent changelog(Endpoint endpoint,String identity,Map<String,Object> row) {
        ChangeEvent.Operation op;
        try{op=ChangeEvent.Operation.valueOf(row.get("operation").toString());}catch(Exception e){throw new IllegalArgumentException("Invalid ClickHouse changelog operation");}
        if(!Set.of(ChangeEvent.Operation.SNAPSHOT,ChangeEvent.Operation.INSERT,ChangeEvent.Operation.UPDATE,ChangeEvent.Operation.DELETE).contains(op))
            throw new IllegalArgumentException("Unsupported ClickHouse changelog operation");
        Map<String,Object> key=ClickHouseHttp.object(row.get("key_json").toString());
        Map<String,Object> before=row.get("before_json")==null?null:ClickHouseHttp.object(row.get("before_json").toString());
        Map<String,Object> after=row.get("after_json")==null?null:ClickHouseHttp.object(row.get("after_json").toString());
        if(key==null||key.isEmpty()||key.values().stream().anyMatch(Objects::isNull)||(op!=ChangeEvent.Operation.DELETE&&after==null)||(op==ChangeEvent.Operation.UPDATE&&before==null))
            throw new IllegalArgumentException("Incomplete ClickHouse changelog images or key");
        String position=row.get("sequence").toString();
        return new ChangeEvent(EventCodec.digest(List.of(identity,position,row.get("event_id"))),endpoint.require("database"),endpoint.require("table"),
            op,position,key,before,after,new BigInteger(row.get("source_time_ms").toString()).longValueExact());
    }
    @Override public void close(){closed=true;Thread active=worker;if(active!=null)active.interrupt();}
}
