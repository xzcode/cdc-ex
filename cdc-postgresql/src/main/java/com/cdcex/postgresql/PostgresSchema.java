package com.cdcex.postgresql;

import com.cdcex.core.ConnectorProvider;
import com.cdcex.core.EventCodec;
import java.sql.*;
import java.util.*;

record PostgresSchema(String identity, long tableOid, List<Column> columns, List<String> keys, String version) {
    record Column(String name,String type,boolean nullable,String collation) {}
    static PostgresSchema read(Connection c,PostgresConfig cfg,boolean target) throws SQLException {
        String version=one(c,"SHOW server_version"),versionNumber=one(c,"SHOW server_version_num");
        int v=Integer.parseInt(versionNumber);if(v<160000||v>=180000) throw new IllegalStateException("PostgreSQL provider currently accepts major versions 16 and 17");
        String system=one(c,"SELECT system_identifier::text FROM pg_control_system()");
        String databaseOid=one(c,"SELECT oid::text FROM pg_database WHERE datname=current_database()");
        long oid;
        try(var q=c.prepareStatement("SELECT c.oid,c.relkind,c.relpersistence,c.relreplident,c.relrowsecurity,c.relispartition FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=? AND c.relname=?")) {
            q.setString(1,cfg.schema());q.setString(2,cfg.table());try(var r=q.executeQuery()) {
                if(!r.next()) throw new IllegalStateException("PostgreSQL table does not exist");oid=r.getLong(1);
                if(!"r".equals(r.getString(2))||!"p".equals(r.getString(3))||r.getBoolean(6)) throw new IllegalStateException("Only permanent non-partitioned PostgreSQL tables are supported");
                if(!target&&!"f".equals(r.getString(4))) throw new IllegalStateException("PostgreSQL source requires REPLICA IDENTITY FULL");
                if(r.getBoolean(5)) throw new IllegalStateException("Row-level security is unsupported for replication");
            }
        }
        for(String privilege:target?List.of("SELECT","INSERT","UPDATE","DELETE"):List.of("SELECT")) {
            try(var q=c.prepareStatement("SELECT has_table_privilege(?::oid,?)")) {
                q.setLong(1,oid);q.setString(2,privilege);try(var r=q.executeQuery()) {
                    if(!r.next()||!r.getBoolean(1))throw new IllegalStateException("PostgreSQL table privilege required: "+privilege);
                }
            }
        }
        List<Column> columns=new ArrayList<>();
        try(var q=c.prepareStatement("SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attgenerated,a.attidentity,co.collname FROM pg_attribute a LEFT JOIN pg_collation co ON co.oid=a.attcollation WHERE a.attrelid=? AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum")) {
            q.setLong(1,oid);try(var r=q.executeQuery()) {while(r.next()) {
                String name=r.getString(1),type=r.getString(2);PostgresConfig.identifier(name);
                if(!supported(type)) throw new IllegalStateException("Unsupported PostgreSQL type: "+type);
                if(!r.getString(4).isEmpty()||!r.getString(5).isEmpty()) throw new IllegalStateException("Generated/identity PostgreSQL columns require an explicit mapping policy");
                columns.add(new Column(name,type,!r.getBoolean(3),r.getString(6)));
            }}
        }
        if(columns.isEmpty()) throw new IllegalStateException("Empty PostgreSQL table schema");
        List<String> keys=new ArrayList<>();
        try(var q=c.prepareStatement("SELECT a.attname FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,ord) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE i.indrelid=? AND i.indisprimary ORDER BY k.ord")) {
            q.setLong(1,oid);try(var r=q.executeQuery()) {while(r.next())keys.add(r.getString(1));}
        }
        if(keys.isEmpty()) throw new IllegalStateException("PostgreSQL replication requires a primary key");
        // Excluding table inheritance avoids SELECT snapshots that include rows never published by this table.
        if(count(c,"SELECT count(*) FROM pg_inherits WHERE inhrelid=? OR inhparent=?",oid,oid)>0) throw new IllegalStateException("Inherited PostgreSQL tables are unsupported");
        if(target) {
            if(count(c,"SELECT count(*) FROM pg_trigger WHERE tgrelid=? AND NOT tgisinternal",oid)>0) throw new IllegalStateException("PostgreSQL sink triggers are unsupported");
            if(count(c,"SELECT count(*) FROM pg_constraint WHERE contype='f' AND (conrelid=? OR confrelid=?)",oid,oid)>0) throw new IllegalStateException("PostgreSQL sink foreign keys are unsupported");
            if(count(c,"SELECT count(*) FROM pg_index WHERE indrelid=? AND indisunique AND NOT indisprimary",oid)>0) throw new IllegalStateException("PostgreSQL sink secondary unique indexes are unsupported");
        }
        return new PostgresSchema("postgresql:"+system+":"+databaseOid+":"+oid,oid,List.copyOf(columns),List.copyOf(keys),version);
    }
    static boolean supported(String type) {
        return type.matches("smallint|integer|bigint|boolean|text|character varying(?:\\(\\d+\\))?|character\\(\\d+\\)|numeric\\(\\d+,\\d+\\)|date|time(?:\\([0-6]\\))? without time zone|timestamp(?:\\([0-6]\\))? (?:with|without) time zone|bytea|jsonb?|uuid");
    }
    private static long count(Connection c,String sql,long... params) throws SQLException {
        try(var q=c.prepareStatement(sql)) {for(int i=0;i<params.length;i++)q.setLong(i+1,params[i]);try(var r=q.executeQuery()){r.next();return r.getLong(1);}}
    }
    static String one(Connection c,String sql) throws SQLException { try(var s=c.createStatement();var r=s.executeQuery(sql)){if(!r.next())throw new SQLException("Missing PostgreSQL metadata");return r.getString(1);} }
    record Slot(String restartLsn,String confirmedLsn) {}
    static Slot sourcePreflight(Connection c,PostgresConfig cfg,PostgresSchema schema,boolean mustBeInactive) throws SQLException {
        if(!"logical".equals(one(c,"SHOW wal_level"))) throw new IllegalStateException("PostgreSQL wal_level must be logical");
        if(!"t".equals(one(c,"SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname=current_user"))) throw new IllegalStateException("PostgreSQL replication privilege is required");
        try(var q=c.prepareStatement("SELECT pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication WHERE pubname=?")) {
            q.setString(1,cfg.publication());try(var r=q.executeQuery()) {if(!r.next()||!r.getBoolean(1)||!r.getBoolean(2)||!r.getBoolean(3)||!r.getBoolean(4)) throw new IllegalStateException("Publication must exist and publish insert/update/delete/truncate");}
        }
        try(var q=c.prepareStatement("SELECT attnames,rowfilter FROM pg_publication_tables WHERE pubname=? AND schemaname=? AND tablename=?")) {
            q.setString(1,cfg.publication());q.setString(2,cfg.schema());q.setString(3,cfg.table());try(var r=q.executeQuery()) {
                if(!r.next()||r.getString(2)!=null) throw new IllegalStateException("Publication must contain the complete unfiltered source table");
                Set<String> actual=new HashSet<>(Arrays.asList((String[])r.getArray(1).getArray()));
                if(!actual.equals(new HashSet<>(schema.columns.stream().map(Column::name).toList()))) throw new IllegalStateException("Publication column filtering is unsupported");
            }
        }
        try(var q=c.prepareStatement("SELECT plugin,slot_type,database,active,temporary,wal_status,restart_lsn::text,confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=?")) {
            q.setString(1,cfg.slot());try(var r=q.executeQuery()) {
                if(!r.next()) throw new IllegalStateException("Dedicated pgoutput replication slot must already exist; missing slots are never recreated automatically");
                if(!"pgoutput".equals(r.getString(1))||!"logical".equals(r.getString(2))||!cfg.database().equals(r.getString(3))||r.getBoolean(5)) throw new IllegalStateException("Incompatible PostgreSQL replication slot");
                if(mustBeInactive&&r.getBoolean(4)) throw new IllegalStateException("PostgreSQL replication slot is already in use");
                if("lost".equals(r.getString(6))||r.getString(7)==null) throw new IllegalStateException("PostgreSQL retained WAL was lost; explicit recovery is required");
                return new Slot(r.getString(7),r.getString(8));
            }
        }
    }
    ConnectorProvider.Inspection inspection() {
        Map<String,String> fields=new LinkedHashMap<>();for(var c:columns)fields.put(c.name(),c.type());
        return new ConnectorProvider.Inspection(identity,fields,keys,Map.of("version",version,"tableOid",tableOid,"replicaIdentity","FULL","typePolicy","strict","schema",EventCodec.digest(List.of(columns,keys))));
    }
    void requireSame(PostgresSchema actual) {
        if(!identity.equals(actual.identity)||!columns.equals(actual.columns)||!keys.equals(actual.keys)) throw new IllegalStateException("PostgreSQL identity or schema changed");
    }
}
