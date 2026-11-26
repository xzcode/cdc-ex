package com.cdcex.mysql;

import java.sql.*;
import java.util.*;
import static com.cdcex.mysql.MysqlTaskConfig.quote;
public record MysqlSchema(List<Column> columns,List<String> keys,String uuid) {
    public record Column(String name,String type,boolean nullable,String collation){}
    public record Preflight(MysqlSchema source,MysqlSchema target){}
    static Connection connect(MysqlTaskConfig.Endpoint e) throws SQLException {
        try { Class.forName(e.dialect()==MysqlTaskConfig.Dialect.MYSQL?"com.mysql.cj.jdbc.Driver":"org.mariadb.jdbc.Driver"); } catch(ClassNotFoundException failure) { throw new SQLException("SQL JDBC driver unavailable", failure); }
        Properties p=new Properties();p.setProperty("user",e.user());p.setProperty("password",e.password());
        p.setProperty("sslMode",e.dialect()==MysqlTaskConfig.Dialect.MYSQL?e.sslMode():e.connectorSslMode());p.setProperty("connectTimeout","5000");p.setProperty("socketTimeout","15000");
        return DriverManager.getConnection(e.url(),p);
    }
    static String variable(Connection c,String query) throws SQLException {
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery(query)){if(!r.next())throw new SQLException("Missing database prerequisite");return r.getString(1);}
    }
    static boolean enabled(String value){return "1".equals(value)||"ON".equalsIgnoreCase(value)||"true".equalsIgnoreCase(value);}
    static String identity(Connection c,MysqlTaskConfig.Endpoint e) throws SQLException {
        String version=variable(c,"SELECT VERSION()");
        boolean maria=version.toLowerCase(Locale.ROOT).contains("mariadb");
        if(maria!=(e.dialect()==MysqlTaskConfig.Dialect.MARIADB))throw new IllegalArgumentException("Configured database dialect does not match server");
        if(!maria)return variable(c,"SELECT @@server_uuid");
        // MariaDB server_uid is host/port-derived; it is a guard, not an immutable database generation identifier.
        return "mariadb:"+variable(c,"SELECT @@server_uid")+":"+variable(c,"SELECT @@server_id");
    }
    public static MysqlSchema read(Connection c,MysqlTaskConfig.Endpoint e,boolean target) throws SQLException {
        if(!"InnoDB".equalsIgnoreCase(variable(c,"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='"+e.database()+"' AND TABLE_NAME='"+e.table()+"'")))
            throw new IllegalArgumentException("Only InnoDB base tables are supported");
        List<Column> cols=new ArrayList<>();List<String> keys=new ArrayList<>();
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SHOW FULL COLUMNS FROM "+e.qualified())) {
            while(r.next()){
                String n=r.getString("Field"),t=r.getString("Type").toLowerCase(Locale.ROOT);
                MysqlTaskConfig.identifier(n);
                if(!t.matches("(tinyint|smallint|mediumint|int|bigint)(\\(\\d+\\))?|decimal\\(\\d+,\\d+\\)|varchar\\(\\d+\\)|char\\(\\d+\\)|(tinytext|text|mediumtext|longtext)"))
                    throw new IllegalArgumentException("Unsupported M1 column type: "+t);
                if(r.getString("Extra").toLowerCase(Locale.ROOT).contains("generated")) throw new IllegalArgumentException("Generated columns are unsupported");
                if(t.matches("(tinyint|smallint|mediumint|int|bigint)\\(\\d+\\)")) t=t.substring(0,t.indexOf('('));
                cols.add(new Column(n,t,"YES".equals(r.getString("Null")),r.getString("Collation")));
            }
        }
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SHOW INDEX FROM "+e.qualified())){
            TreeMap<Integer,String> ordered=new TreeMap<>();while(r.next())if("PRIMARY".equals(r.getString("Key_name")))ordered.put(r.getInt("Seq_in_index"),r.getString("Column_name"));keys.addAll(ordered.values());
        }
        if(keys.isEmpty()) throw new IllegalArgumentException("A stable primary key is required");
        if(target) {
            for(Column column:cols)if(keys.contains(column.name())&&column.collation()!=null&&!column.collation().endsWith("_bin")&&!column.collation().equals("binary"))
                throw new IllegalArgumentException("Text target keys require a binary collation to preserve distinct mapped identities");
            try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SHOW INDEX FROM "+e.qualified())){
                while(r.next()) if(r.getInt("Non_unique")==0&&!"PRIMARY".equals(r.getString("Key_name")))throw new IllegalArgumentException("Secondary unique target indexes are unsupported");
            }
            if(!"0".equals(variable(c,"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA='"+e.database()+"' AND EVENT_OBJECT_TABLE='"+e.table()+"'")))throw new IllegalArgumentException("Target triggers unsupported");
            if(!"0".equals(variable(c,"SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL AND ((TABLE_SCHEMA='"+e.database()+"' AND TABLE_NAME='"+e.table()+"') OR (REFERENCED_TABLE_SCHEMA='"+e.database()+"' AND REFERENCED_TABLE_NAME='"+e.table()+"'))")))throw new IllegalArgumentException("Target foreign keys unsupported");
        }
        String catalog=e.dialect()==MysqlTaskConfig.Dialect.MYSQL?"INNODB_TABLES":"INNODB_SYS_TABLES";
        String tableId=variable(c,"SELECT TABLE_ID FROM information_schema."+catalog+" WHERE NAME='"+e.database()+"/"+e.table()+"'");
        return new MysqlSchema(List.copyOf(cols),List.copyOf(keys),identity(c,e)+":innodb:"+tableId);
    }
    public static Preflight preflight(MysqlTaskConfig config,boolean fresh) throws SQLException {
        try(Connection s=connect(config.source());Connection t=connect(config.target())){
            if(!enabled(variable(s,"SELECT @@log_bin"))||!"ROW".equalsIgnoreCase(variable(s,"SELECT @@binlog_format"))||!"FULL".equalsIgnoreCase(variable(s,"SELECT @@binlog_row_image")))
                throw new IllegalArgumentException("Source requires log_bin=ON, ROW, FULL");
            if(Long.parseLong(variable(s,"SELECT @@server_id"))==config.replicationServerId())throw new IllegalArgumentException("Replication client ID conflicts with source server ID");
            if(config.source().dialect()==MysqlTaskConfig.Dialect.MARIADB){
                if(enabled(variable(s,"SELECT @@log_bin_compress")))throw new IllegalArgumentException("MariaDB compressed binlog unsupported");
                if(!enabled(variable(s,"SELECT @@binlog_legacy_event_pos")))throw new IllegalArgumentException("MariaDB requires binlog_legacy_event_pos=ON");
            }
            MysqlSchema a=read(s,config.source(),false),b=read(t,config.target(),true);
            if(a.uuid.equals(b.uuid)&&config.source().database().equalsIgnoreCase(config.target().database())&&config.source().table().equalsIgnoreCase(config.target().table()))
                throw new IllegalArgumentException("Source and target are the same physical table");
            if(!a.columns.equals(b.columns)||!a.keys.equals(b.keys))throw new IllegalArgumentException("M1 requires identical source and target columns and primary keys");
            if(fresh&&"initial".equals(config.snapshotMode())&&!"0".equals(variable(t,"SELECT EXISTS(SELECT 1 FROM "+config.target().qualified()+" LIMIT 1)")))throw new IllegalArgumentException("Initial snapshot requires an empty target");
            return new Preflight(a,b);
        }
    }
}
