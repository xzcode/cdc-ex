package com.cdcex.mysql;

import java.sql.*;
import java.util.*;
import static com.cdcex.mysql.MysqlTaskConfig.quote;
public record MysqlSchema(List<Column> columns,List<String> keys,String uuid) {
    public record Column(String name,String type,boolean nullable,String collation){}
    public record Preflight(MysqlSchema source,MysqlSchema target){}
    static Connection connect(MysqlTaskConfig.Endpoint e) throws SQLException {
        Properties p=new Properties();p.setProperty("user",e.user());p.setProperty("password",e.password());
        p.setProperty("sslMode",e.sslMode());p.setProperty("connectTimeout","5000");p.setProperty("socketTimeout","15000");
        return DriverManager.getConnection(e.url(),p);
    }
    static String variable(Connection c,String query) throws SQLException {
        try(Statement s=c.createStatement();ResultSet r=s.executeQuery(query)){if(!r.next())throw new SQLException("Missing database prerequisite");return r.getString(1);}
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
                cols.add(new Column(n,t,"YES".equals(r.getString("Null")),r.getString("Collation")));
            }
        }
        try(ResultSet r=c.getMetaData().getPrimaryKeys(e.database(),null,e.table())){
            TreeMap<Short,String> ordered=new TreeMap<>();while(r.next())ordered.put(r.getShort("KEY_SEQ"),r.getString("COLUMN_NAME"));keys.addAll(ordered.values());
        }
        if(keys.isEmpty()) throw new IllegalArgumentException("A stable primary key is required");
        if(target) {
            try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SHOW INDEX FROM "+e.qualified())){
                while(r.next()) if(r.getInt("Non_unique")==0&&!"PRIMARY".equals(r.getString("Key_name")))throw new IllegalArgumentException("Secondary unique target indexes are unsupported");
            }
            if(!"0".equals(variable(c,"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA='"+e.database()+"' AND EVENT_OBJECT_TABLE='"+e.table()+"'")))throw new IllegalArgumentException("Target triggers unsupported");
            if(!"0".equals(variable(c,"SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL AND ((TABLE_SCHEMA='"+e.database()+"' AND TABLE_NAME='"+e.table()+"') OR (REFERENCED_TABLE_SCHEMA='"+e.database()+"' AND REFERENCED_TABLE_NAME='"+e.table()+"'))")))throw new IllegalArgumentException("Target foreign keys unsupported");
        }
        return new MysqlSchema(List.copyOf(cols),List.copyOf(keys),variable(c,"SELECT @@server_uuid"));
    }
    public static Preflight preflight(MysqlTaskConfig config,boolean fresh) throws SQLException {
        try(Connection s=connect(config.source());Connection t=connect(config.target())){
            if(!"1".equals(variable(s,"SELECT @@log_bin"))||!"ROW".equalsIgnoreCase(variable(s,"SELECT @@binlog_format"))||!"FULL".equalsIgnoreCase(variable(s,"SELECT @@binlog_row_image")))
                throw new IllegalArgumentException("Source requires log_bin=ON, ROW, FULL");
            MysqlSchema a=read(s,config.source(),false),b=read(t,config.target(),true);
            if(a.uuid.equals(b.uuid)&&config.source().database().equalsIgnoreCase(config.target().database())&&config.source().table().equalsIgnoreCase(config.target().table()))
                throw new IllegalArgumentException("Source and target are the same physical table");
            if(!a.columns.equals(b.columns)||!a.keys.equals(b.keys))throw new IllegalArgumentException("M1 requires identical source and target columns and primary keys");
            if(fresh&&"initial".equals(config.snapshotMode())&&!"0".equals(variable(t,"SELECT EXISTS(SELECT 1 FROM "+config.target().qualified()+" LIMIT 1)")))throw new IllegalArgumentException("Initial snapshot requires an empty target");
            return new Preflight(a,b);
        }
    }
}
