package com.cdcex.mysql;

import com.cdcex.core.*;
import com.cdcex.debezium.AtomicOffsetStore;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** SQL endpoint adapter used by the task supervisor and the command-line runner. */
public class MysqlProvider implements ConnectorProvider {
    protected final MysqlTaskConfig.Dialect dialect;
    public MysqlProvider(){this(MysqlTaskConfig.Dialect.MYSQL);}
    protected MysqlProvider(MysqlTaskConfig.Dialect dialect){this.dialect=dialect;}
    @Override public String type(){return dialect.name();}
    @Override public Map<String,Object> capabilities(){return Map.of(
        "sourceModes",List.of("initial","no_data"),"sink",true,"comparison",true,
        "operations",List.of("SNAPSHOT","INSERT","UPDATE","DELETE"),
        "types",List.of("signed integers","fixed decimal","text"),
        "limits",List.of("one table per task","primary key required","incompatible DDL blocks","interrupted snapshots require a new reconciled generation"));}
    protected MysqlTaskConfig.Endpoint sql(Endpoint e){
        if(!type().equals(e.type()))throw new IllegalArgumentException("Wrong connector type");
        return new MysqlTaskConfig.Endpoint(e.require("host"),e.integer("port",3306),e.require("user"),e.options().getOrDefault("password",""),
            e.require("database"),e.require("table"),e.option("sslMode","VERIFY_IDENTITY").toUpperCase(Locale.ROOT),dialect);
    }
    private long serverId(Endpoint e){
        long id=Long.parseLong(e.require("replicationServerId"));
        if(id<1||id>4294967295L)throw new IllegalArgumentException("Invalid replicationServerId");return id;
    }
    @Override public List<String> resources(Endpoint endpoint,String query,int limit)throws Exception{
        var options=new LinkedHashMap<>(endpoint.options());options.put("table","cdc_ex_discovery");
        var e=sql(new Endpoint(endpoint.type(),options));List<String> result=new ArrayList<>();
        try(Connection c=MysqlSchema.connect(e);PreparedStatement s=c.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' AND ENGINE='InnoDB' AND LOCATE(?,TABLE_NAME)>0 AND TABLE_NAME REGEXP '^[A-Za-z0-9_]{1,64}(Endpoint endpoint,Role role) throws Exception {
        var e=sql(endpoint);
        try(Connection c=MysqlSchema.connect(e)){
            if(role==Role.SOURCE){
                if(!MysqlSchema.enabled(MysqlSchema.variable(c,"SELECT @@log_bin"))||!"ROW".equalsIgnoreCase(MysqlSchema.variable(c,"SELECT @@binlog_format"))||!"FULL".equalsIgnoreCase(MysqlSchema.variable(c,"SELECT @@binlog_row_image")))
                    throw new IllegalArgumentException("Source requires log_bin=ON, binlog_format=ROW, binlog_row_image=FULL");
                if(serverId(endpoint)==Long.parseLong(MysqlSchema.variable(c,"SELECT @@server_id")))throw new IllegalArgumentException("Replication client ID conflicts with server ID");
                if(dialect==MysqlTaskConfig.Dialect.MARIADB){
                    if(MysqlSchema.enabled(MysqlSchema.variable(c,"SELECT @@log_bin_compress")))throw new IllegalArgumentException("Compressed MariaDB binlog is not supported");
                    if(!MysqlSchema.enabled(MysqlSchema.variable(c,"SELECT @@binlog_legacy_event_pos")))throw new IllegalArgumentException("MariaDB binlog_legacy_event_pos must be ON");
                }
            }
            var schema=MysqlSchema.read(c,e,role==Role.SINK);
            Map<String,String> fields=new LinkedHashMap<>();schema.columns().forEach(col->fields.put(col.name(),col.type()));
            return new Inspection(schema.uuid(),fields,schema.keys(),Map.of(
                "version",MysqlSchema.variable(c,"SELECT VERSION()"),"empty","0".equals(MysqlSchema.variable(c,"SELECT EXISTS(SELECT 1 FROM "+e.qualified()+" LIMIT 1)")),
                "schema",EventCodec.digest(schema),"namespace",e.database()+"."+e.table(),"serverIdentity",MysqlSchema.identity(c,e)));
        }
    }
    protected SourceConnector capture(MysqlTaskConfig cfg,String taskId){return new MysqlSource(cfg,taskId);}
    @Override public SourceConnector openSource(Endpoint endpoint,SourceContext context) throws Exception {
        var e=sql(endpoint);
        if(!context.fresh()){
            Path offsets=context.directory().resolve("offsets.dat"),history=context.directory().resolve("schema-history.dat");
            if(!Files.isRegularFile(offsets)||!Files.isRegularFile(history)||Files.size(history)==0)throw new IllegalStateException("Missing binlog recovery state; explicit recovery is required");
            AtomicOffsetStore.readOffsets(offsets);
            if("initial".equals(context.snapshotMode())&&!AtomicOffsetStore.snapshotComplete(offsets))throw new IllegalStateException("Interrupted snapshot; reconcile in a new task generation");
        }
        Inspection pinned=inspect(endpoint,Role.SOURCE);
        SourceConnector delegate=capture(new MysqlTaskConfig(e,e,serverId(endpoint),context.directory(),context.snapshotMode(),1,1),context.taskId());
        return new SourceConnector(){
            public void run(DurableReceiver receiver)throws Exception{delegate.run(batch->{
                Inspection current=inspect(endpoint,Role.SOURCE);
                if(!pinned.identity().equals(current.identity())||!pinned.details().get("schema").equals(current.details().get("schema")))throw new IllegalStateException("Source identity or schema changed");
                receiver.accept(batch);
            });}
            public void close()throws Exception{delegate.close();}
        };
    }
    @Override public SinkConnector openSink(Endpoint endpoint,SinkContext context) throws Exception {
        var e=sql(endpoint);
        try(Connection c=MysqlSchema.connect(e)){
            // The mapping stage has already replaced the source namespace with the target namespace.
            return new MysqlSink(new MysqlTaskConfig(e,e,1,context.directory(),"no_data",1,1),MysqlSchema.read(c,e,true));
        }
    }
    @Override public List<Map<String,Object>> readRows(Endpoint endpoint,int limit)throws Exception{
        if(limit<1||limit>100001)throw new IllegalArgumentException("Invalid comparison limit");
        var e=sql(endpoint);List<Map<String,Object>> rows=new ArrayList<>();
        try(Connection c=MysqlSchema.connect(e)){
            var schema=MysqlSchema.read(c,e,false);c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
            try(PreparedStatement s=c.prepareStatement("SELECT * FROM "+e.qualified()+" ORDER BY "+String.join(",",schema.keys().stream().map(MysqlTaskConfig::quote).toList())+" LIMIT ?")){
                s.setInt(1,limit);
                long bytes=0;try(ResultSet r=s.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();for(var col:schema.columns())row.put(col.name(),r.getObject(col.name()));bytes+=EventCodec.json(row).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;if(bytes>32L*1024*1024)throw new IllegalStateException("Comparison byte limit exceeded");rows.add(row);}}
            }finally{c.rollback();}
        }return rows;
    }
}
 ORDER BY TABLE_NAME LIMIT ?")){
            // Empty query matches all names (LOCATE('', name)=1).
            s.setString(1,e.database());s.setString(2,query);s.setInt(3,limit);
            try(ResultSet r=s.executeQuery()){while(r.next())result.add(r.getString(1));}
        }
        return result;
    }
    @Override public Inspection inspect(Endpoint endpoint,Role role) throws Exception {
        var e=sql(endpoint);
        try(Connection c=MysqlSchema.connect(e)){
            if(role==Role.SOURCE){
                if(!MysqlSchema.enabled(MysqlSchema.variable(c,"SELECT @@log_bin"))||!"ROW".equalsIgnoreCase(MysqlSchema.variable(c,"SELECT @@binlog_format"))||!"FULL".equalsIgnoreCase(MysqlSchema.variable(c,"SELECT @@binlog_row_image")))
                    throw new IllegalArgumentException("Source requires log_bin=ON, binlog_format=ROW, binlog_row_image=FULL");
                if(serverId(endpoint)==Long.parseLong(MysqlSchema.variable(c,"SELECT @@server_id")))throw new IllegalArgumentException("Replication client ID conflicts with server ID");
                if(dialect==MysqlTaskConfig.Dialect.MARIADB){
                    if(MysqlSchema.enabled(MysqlSchema.variable(c,"SELECT @@log_bin_compress")))throw new IllegalArgumentException("Compressed MariaDB binlog is not supported");
                    if(!MysqlSchema.enabled(MysqlSchema.variable(c,"SELECT @@binlog_legacy_event_pos")))throw new IllegalArgumentException("MariaDB binlog_legacy_event_pos must be ON");
                }
            }
            var schema=MysqlSchema.read(c,e,role==Role.SINK);
            Map<String,String> fields=new LinkedHashMap<>();schema.columns().forEach(col->fields.put(col.name(),col.type()));
            return new Inspection(schema.uuid(),fields,schema.keys(),Map.of(
                "version",MysqlSchema.variable(c,"SELECT VERSION()"),"empty","0".equals(MysqlSchema.variable(c,"SELECT EXISTS(SELECT 1 FROM "+e.qualified()+" LIMIT 1)")),
                "schema",EventCodec.digest(schema),"namespace",e.database()+"."+e.table(),"serverIdentity",MysqlSchema.identity(c,e)));
        }
    }
    protected SourceConnector capture(MysqlTaskConfig cfg,String taskId){return new MysqlSource(cfg,taskId);}
    @Override public SourceConnector openSource(Endpoint endpoint,SourceContext context) throws Exception {
        var e=sql(endpoint);
        if(!context.fresh()){
            Path offsets=context.directory().resolve("offsets.dat"),history=context.directory().resolve("schema-history.dat");
            if(!Files.isRegularFile(offsets)||!Files.isRegularFile(history)||Files.size(history)==0)throw new IllegalStateException("Missing binlog recovery state; explicit recovery is required");
            AtomicOffsetStore.readOffsets(offsets);
            if("initial".equals(context.snapshotMode())&&!AtomicOffsetStore.snapshotComplete(offsets))throw new IllegalStateException("Interrupted snapshot; reconcile in a new task generation");
        }
        Inspection pinned=inspect(endpoint,Role.SOURCE);
        SourceConnector delegate=capture(new MysqlTaskConfig(e,e,serverId(endpoint),context.directory(),context.snapshotMode(),1,1),context.taskId());
        return new SourceConnector(){
            public void run(DurableReceiver receiver)throws Exception{delegate.run(batch->{
                Inspection current=inspect(endpoint,Role.SOURCE);
                if(!pinned.identity().equals(current.identity())||!pinned.details().get("schema").equals(current.details().get("schema")))throw new IllegalStateException("Source identity or schema changed");
                receiver.accept(batch);
            });}
            public void close()throws Exception{delegate.close();}
        };
    }
    @Override public SinkConnector openSink(Endpoint endpoint,SinkContext context) throws Exception {
        var e=sql(endpoint);
        try(Connection c=MysqlSchema.connect(e)){
            // The mapping stage has already replaced the source namespace with the target namespace.
            return new MysqlSink(new MysqlTaskConfig(e,e,1,context.directory(),"no_data",1,1),MysqlSchema.read(c,e,true));
        }
    }
    @Override public List<Map<String,Object>> readRows(Endpoint endpoint,int limit)throws Exception{
        if(limit<1||limit>100001)throw new IllegalArgumentException("Invalid comparison limit");
        var e=sql(endpoint);List<Map<String,Object>> rows=new ArrayList<>();
        try(Connection c=MysqlSchema.connect(e)){
            var schema=MysqlSchema.read(c,e,false);c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
            try(PreparedStatement s=c.prepareStatement("SELECT * FROM "+e.qualified()+" ORDER BY "+String.join(",",schema.keys().stream().map(MysqlTaskConfig::quote).toList())+" LIMIT ?")){
                s.setInt(1,limit);
                long bytes=0;try(ResultSet r=s.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();for(var col:schema.columns())row.put(col.name(),r.getObject(col.name()));bytes+=EventCodec.json(row).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;if(bytes>32L*1024*1024)throw new IllegalStateException("Comparison byte limit exceeded");rows.add(row);}}
            }finally{c.rollback();}
        }return rows;
    }
}
