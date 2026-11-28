package com.cdcex.clickhouse;

import com.cdcex.core.*;
import java.math.*;
import java.util.*;

public final class ClickHouseProvider implements ConnectorProvider {
    static final Map<String,String> STATE_COLUMNS=Map.of("entity_key","String","version","UInt64","is_deleted","UInt8",
        "event_id","String","payload","String","source_position","String","source_time_ms","Int64");
    static final Map<String,String> HISTORY_COLUMNS=Map.of("event_id","String","entity_key","String","operation","String",
        "before_json","Nullable(String)","after_json","Nullable(String)","source_position","String","source_time_ms","Int64");
    static final Map<String,String> CHANGELOG_COLUMNS=Map.of("sequence","UInt64","event_id","String","operation","String",
        "key_json","String","before_json","Nullable(String)","after_json","Nullable(String)","source_time_ms","Int64");
    record Table(String identity,String engine,String engineFull,String sortingKey,String partitionKey,Map<String,String> fields) {
        String fingerprint(){return EventCodec.digest(List.of(identity,engine,engineFull,sortingKey,partitionKey,fields));}
    }
    @Override public String type(){return "CLICKHOUSE";}
    @Override public Map<String,Object> capabilities(){return Map.of("sourceModes",List.of("snapshot","cursor","changelog"),
        "sinkModes",List.of("history","current_state"),"nativeCdc",false,"requiresKey",true,
        "currentStateQuery","FINAL with is_deleted=0","envelopeSink",true);}
    @Override public List<String> resources(Endpoint endpoint,String query,int limit)throws Exception{
        quote(endpoint.require("database"));
        try(var http=new ClickHouseHttp(endpoint)){
            return http.query("SELECT name FROM system.tables WHERE database={db:String} AND ({query:String}='' OR position(name,{query:String})>0) AND match(name,'^[A-Za-z_][A-Za-z0-9_]{0,62}$') AND engine IN ('MergeTree','ReplacingMergeTree') ORDER BY name LIMIT {limit:UInt32}",Map.of("db",endpoint.require("database"),"query",query,"limit",Integer.toString(limit))).stream().map(row->row.get("name").toString()).toList();
        }
    }
    static String quote(String value) {
        if(value==null||!value.matches("[A-Za-z_][A-Za-z0-9_]{0,62}"))throw new IllegalArgumentException("Unsupported ClickHouse identifier");
        return "`"+value+"`";
    }
    static String qualified(Endpoint endpoint){return quote(endpoint.require("database"))+"."+quote(endpoint.require("table"));}
    static List<String> columns(Endpoint endpoint,String option) {
        String value=endpoint.require(option);List<String> result=Arrays.stream(value.split(",",-1)).map(String::trim).toList();
        result.forEach(ClickHouseProvider::quote);
        if(new HashSet<>(result).size()!=result.size())throw new IllegalArgumentException("Repeated ClickHouse column");
        return result;
    }
    static Table table(ClickHouseHttp http,Endpoint endpoint)throws Exception {
        qualified(endpoint);
        var params=Map.of("db",endpoint.require("database"),"table",endpoint.require("table"));
        var tables=http.query("SELECT toString(uuid) AS uuid,engine,engine_full,sorting_key,partition_key FROM system.tables WHERE database={db:String} AND name={table:String}",params);
        if(tables.size()!=1)throw new IllegalStateException("ClickHouse table must already exist");
        var t=tables.getFirst();String uuid=t.get("uuid").toString();
        if(uuid.equals("00000000-0000-0000-0000-000000000000"))throw new IllegalStateException("ClickHouse table requires a persistent Atomic database UUID");
        Map<String,String> fields=new LinkedHashMap<>();
        for(var c:http.query("SELECT name,type,default_kind FROM system.columns WHERE database={db:String} AND table={table:String} ORDER BY position",params)) {
            String name=c.get("name").toString();quote(name);
            if(Set.of("ALIAS","MATERIALIZED").contains(c.get("default_kind").toString()))throw new IllegalStateException("Generated ClickHouse columns are not supported");
            fields.put(name,c.get("type").toString());
        }
        return new Table("clickhouse:"+uuid+":"+endpoint.require("database")+"."+endpoint.require("table"),
            t.get("engine").toString(),t.get("engine_full").toString(),t.get("sorting_key").toString(),t.get("partition_key").toString(),fields);
    }
    static String sinkMode(Endpoint endpoint) {
        String mode=endpoint.option("mode","current_state");
        if(!Set.of("current_state","history").contains(mode))throw new IllegalArgumentException("Unsupported ClickHouse sink mode");
        return mode;
    }
    static String sourceMode(Endpoint endpoint,String fallback) {
        String mode=endpoint.option("mode",fallback);
        if(!Set.of("snapshot","cursor","changelog").contains(mode))throw new IllegalArgumentException("ClickHouse has no native CDC source mode");
        return mode;
    }
    static void validateSink(Endpoint endpoint,Table table) {
        String mode=sinkMode(endpoint);
        for(String setting:List.of("fsync_after_insert","fsync_part_directory"))
            if(!java.util.regex.Pattern.compile("\\b"+setting+"\\s*=\\s*1\\b").matcher(table.engineFull()).find())
                throw new IllegalStateException("ClickHouse durable sink requires explicit table setting "+setting+"=1");
        if(!table.fields().equals(mode.equals("history")?HISTORY_COLUMNS:STATE_COLUMNS))
            throw new IllegalStateException("ClickHouse target does not match the required event-envelope schema");
        if(mode.equals("history")) {
            if(!table.engine().equals("MergeTree"))throw new IllegalStateException("History target requires MergeTree");
        } else {
            if(!endpoint.flag("exclusiveWriter",false))throw new IllegalArgumentException("ClickHouse current_state requires exclusiveWriter=true");
            String engine=table.engineFull().replaceAll("[`\\s]","");
            String sorting=table.sortingKey().replace("`","").replace(" ","");
            if(!table.engine().equals("ReplacingMergeTree") || !engine.startsWith("ReplacingMergeTree(version,is_deleted)")
                || !sorting.equals("entity_key") || !table.partitionKey().isBlank())
                throw new IllegalStateException("Current-state target requires ReplacingMergeTree(version,is_deleted), ORDER BY entity_key, no partition expression");
        }
    }
    static void validateSource(Endpoint endpoint,Table table,String mode) {
        if(!table.engine().equals("MergeTree"))throw new IllegalStateException("ClickHouse limited source currently requires ordinary MergeTree");
        if(mode.equals("changelog")) {
            if(!table.fields().equals(CHANGELOG_COLUMNS))throw new IllegalStateException("ClickHouse changelog schema mismatch");
        } else {
            table.fields().values().forEach(ClickHouseProvider::validateType);
            List<String> keys=columns(endpoint,"keyColumns");
            if(!table.fields().keySet().containsAll(keys))throw new IllegalArgumentException("ClickHouse key columns are missing");
            if(mode.equals("cursor")) {
                List<String> cursor=columns(endpoint,"cursorColumns");
                if(!table.fields().keySet().containsAll(cursor)||!cursor.containsAll(keys))
                    throw new IllegalArgumentException("ClickHouse cursor tuple must contain all stable key columns");
                for(String name:cursor)if(!Set.of("UInt64","Int64","String").contains(table.fields().get(name)))
                    throw new IllegalArgumentException("ClickHouse cursor columns must be non-null UInt64, Int64 or String");
            }
        }
        String guarantee=mode.equals("changelog")?"contiguous-visible":"monotonic-visible";
        if(!mode.equals("snapshot") && !guarantee.equals(endpoint.option("cursorGuarantee","")))
            throw new IllegalArgumentException("ClickHouse polling requires explicit cursorGuarantee="+guarantee);
    }
    static void validateType(String type) {
        String t=baseType(type);
        if(!t.matches("U?Int(8|16|32|64|128|256)|String|Bool|Decimal\\(\\d+,\\s*\\d+\\)"))
            throw new IllegalArgumentException("Unsupported ClickHouse source column type: "+type);
    }
    static String baseType(String type) {
        if(type.startsWith("Nullable(")&&type.endsWith(")"))return baseType(type.substring(9,type.length()-1));
        if(type.startsWith("LowCardinality(")&&type.endsWith(")"))return baseType(type.substring(15,type.length()-1));
        return type;
    }
    static Map<String,Object> typed(Map<String,Object> row,Map<String,String> fields) {
        if(!row.keySet().equals(fields.keySet()))throw new IllegalStateException("ClickHouse row/schema mismatch");
        Map<String,Object> result=new LinkedHashMap<>();
        fields.forEach((name,type)->{
            Object value=row.get(name);String t=baseType(type);
            if(value==null){if(!type.contains("Nullable("))throw new IllegalStateException("Unexpected ClickHouse null");result.put(name,null);}
            else if(t.matches("U?Int\\d+"))result.put(name,new BigInteger(value.toString()));
            else if(t.startsWith("Decimal("))result.put(name,new BigDecimal(value.toString()));
            else if(t.equals("Bool"))result.put(name,value instanceof Boolean?value:"1".equals(value.toString())||"true".equals(value.toString()));
            else result.put(name,value.toString());
        });return result;
    }
    @Override public Inspection inspect(Endpoint endpoint,Role role)throws Exception {
        try(var http=new ClickHouseHttp(endpoint)) {
            Table table=table(http,endpoint);
            Map<String,String> fields=table.fields();List<String> keys;
            if(role==Role.SINK) {validateSink(endpoint,table);fields=Map.of("*","EVENT");keys=List.of();}
            else {
                String mode=sourceMode(endpoint,"snapshot");validateSource(endpoint,table,mode);
                if(mode.equals("changelog")){fields=Map.of("*","EVENT");keys=List.of();}
                else keys=columns(endpoint,"keyColumns");
            }
            String version=http.query("SELECT version() AS version",Map.of()).getFirst().get("version").toString();
            boolean empty=http.query("SELECT 1 AS present FROM "+qualified(endpoint)+" LIMIT 1",Map.of()).isEmpty();
            return new Inspection(table.identity(),fields,keys,Map.of("version",version,"empty",empty,
                "engine",table.engine(),"schema",table.fingerprint(),"nativeCdc",false,"envelope",role==Role.SINK));
        }
    }
    @Override public SourceConnector openSource(Endpoint endpoint,SourceContext context){return new ClickHouseSource(endpoint,context);}
    @Override public SinkConnector openSink(Endpoint endpoint,SinkContext context)throws Exception{return new ClickHouseSink(endpoint,context);}
    @Override public List<Map<String,Object>> readRows(Endpoint endpoint,int limit)throws Exception {
        if(limit<1||limit>100_001)throw new IllegalArgumentException("Invalid comparison limit");
        try(var http=new ClickHouseHttp(endpoint)) {
            Table table=table(http,endpoint);
            if(table.fields().equals(STATE_COLUMNS)) {
                validateSink(endpoint,table);
                var rows=http.query("SELECT payload FROM "+qualified(endpoint)+" FINAL WHERE is_deleted=0 ORDER BY entity_key LIMIT "+limit,Map.of());
                return rows.stream().map(r->ClickHouseHttp.object(r.get("payload").toString())).toList();
            }
            if(table.fields().equals(HISTORY_COLUMNS))throw new UnsupportedOperationException("Event history is not a current-state comparison target");
            validateSource(endpoint,table,"snapshot");
            String order=String.join(",",columns(endpoint,"keyColumns").stream().map(ClickHouseProvider::quote).toList());
            return http.query("SELECT * FROM "+qualified(endpoint)+" ORDER BY "+order+" LIMIT "+limit,Map.of()).stream().map(r->typed(r,table.fields())).toList();
        }
    }
}
