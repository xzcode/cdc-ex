package com.cdcex.mysql;

import com.cdcex.core.EventCodec;
import java.nio.file.Path;
import java.util.*;
public record MysqlTaskConfig(Endpoint source,Endpoint target,long replicationServerId,Path directory,
                              String snapshotMode,long journalBytes,long journalEvents) {
    public enum Dialect { MYSQL, MARIADB }
    public record Endpoint(String host,int port,String user,String password,String database,String table,String sslMode,Dialect dialect) {
        public Endpoint(String host,int port,String user,String password,String database,String table,String sslMode){this(host,port,user,password,database,table,sslMode,Dialect.MYSQL);}
        public String connectorSslMode(){return dialect==Dialect.MYSQL?sslMode.toLowerCase(Locale.ROOT):switch(sslMode){case "DISABLED"->"disable";case "REQUIRED"->"trust";case "VERIFY_CA"->"verify-ca";case "VERIFY_IDENTITY"->"verify-full";default->throw new IllegalStateException("Invalid SSL mode");};}
        public Endpoint {
            Objects.requireNonNull(dialect);
            if(host==null || !host.matches("[A-Za-z0-9.-]+") || port<1 || port>65535) throw new IllegalArgumentException("Invalid host/port");
            if(user==null||user.isBlank()||password==null) throw new IllegalArgumentException("Missing credentials");
            identifier(database);identifier(table);
            if(!Set.of("DISABLED","REQUIRED","VERIFY_CA","VERIFY_IDENTITY").contains(sslMode)) throw new IllegalArgumentException("Invalid SSL mode");
        }
        public String url(){return "jdbc:"+dialect.name().toLowerCase(Locale.ROOT)+"://"+host+":"+port+"/"+database;}
        public String qualified(){return quote(database)+"."+quote(table);}
        public Object identity(){return dialect==Dialect.MYSQL?List.of(host,port,user,database,table,sslMode):List.of(host,port,user,database,table,sslMode,dialect.name());}
        @Override public String toString(){return "Endpoint[redacted]";}
    }
    public MysqlTaskConfig {
        Objects.requireNonNull(source);Objects.requireNonNull(target);Objects.requireNonNull(directory);
        if(replicationServerId<1||replicationServerId>4294967295L) throw new IllegalArgumentException("Invalid replication server ID");
        if(!Set.of("initial","no_data").contains(snapshotMode)) throw new IllegalArgumentException("Unsupported snapshot mode");
        if(journalBytes<1||journalEvents<1) throw new IllegalArgumentException("Invalid journal limits");
    }
    public String fingerprint(){return EventCodec.digest(List.of(source.identity(),target.identity(),replicationServerId,snapshotMode));}
    public static void identifier(String v){if(v==null||!v.matches("[A-Za-z0-9_]{1,64}")) throw new IllegalArgumentException("Unsupported SQL identifier");}
    public static String quote(String v){identifier(v);return "`"+v+"`";}
}
