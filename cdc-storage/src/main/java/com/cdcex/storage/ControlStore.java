package com.cdcex.storage;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.util.*;

/** Control-plane records. Credentials and full endpoint settings never appear in plaintext columns. */
public final class ControlStore implements ControlRepository {
    public record Record(String kind,String id,String name,String json,String state,String error,String sourceIdentity,String sinkIdentity,long updated){}
    public record Audit(long timestamp,String action,String resource,String result){}
    private Connection db;private final SecretBox secrets;
    @FunctionalInterface private interface Factory { Connection open()throws SQLException; }
    private final Factory remote;
    private final String checkValue;
    private final boolean mysql;
    private static final String KEY_CHECK="cdc-ex-control-store";
    private String sql(String statement){return mysql?statement.replaceAll("\\b(records|audit|secrets_check)\\b","cdc_ex_$1"):statement;}
    private PreparedStatement prepare(String statement)throws Exception{return connection().prepareStatement(sql(statement));}
    private Connection connection()throws Exception{
        if(!mysql)return db;
        if(db!=null&&db.isValid(2))return db;
        if(db!=null)try{db.close();}catch(SQLException ignored){}
        db=null;Connection next=remote.open();
        try{verify(next,false);db=next;return db;}catch(Exception e){next.close();throw e;}
    }
    private void verify(Connection c,boolean initialize)throws Exception{
        try(Statement st=c.createStatement();ResultSet r=st.executeQuery(sql("SELECT value FROM secrets_check WHERE id=1"))){
            if(r.next()){
                if(!checkValue.equals(secrets.open("key-check",r.getBytes(1))))throw new IllegalStateException("Control store owner or deployment key does not match");
                return;
            }
        }
        if(!initialize)throw new IllegalStateException("Bound control store is missing; restore its metadata");
        for(String table:List.of("records","audit")){
            try(Statement st=c.createStatement();ResultSet r=st.executeQuery(sql("SELECT COUNT(*) FROM "+table))){
                r.next();if(r.getLong(1)!=0)throw new IllegalStateException("Unbound control store contains records");
            }
        }
        try(PreparedStatement insert=c.prepareStatement(sql("INSERT INTO secrets_check(id,value) VALUES(1,?)"))){
            insert.setBytes(1,secrets.seal("key-check",checkValue));insert.executeUpdate();
        }
    }
    public static ControlStore mysql(String host,int port,String database,String user,String password,String sslMode,byte[] key,String owner,boolean initialize)throws Exception{
        if(host==null||!host.matches("[A-Za-z0-9.:-]+")||port<1||port>65535||database==null||!database.matches("[A-Za-z0-9_]{1,64}"))throw new IllegalArgumentException("Invalid control MySQL endpoint");
        if(!Set.of("VERIFY_IDENTITY","VERIFY_CA","REQUIRED","DISABLED").contains(sslMode))throw new IllegalArgumentException("Invalid control MySQL TLS mode");
        Class.forName("com.mysql.cj.jdbc.Driver");
        Properties properties=new Properties();properties.setProperty("user",user);properties.setProperty("password",password);
        properties.setProperty("sslMode",sslMode);properties.setProperty("connectTimeout","5000");properties.setProperty("socketTimeout","10000");properties.setProperty("autoReconnect","false");
        String address=host.contains(":")?"["+host+"]":host;
        return new ControlStore(()->DriverManager.getConnection("jdbc:mysql://"+address+":"+port+"/"+database,properties),key,owner,initialize);
    }
    private ControlStore(Factory factory,byte[] key,String owner,boolean initialize)throws Exception{
        mysql=true;remote=factory;checkValue=KEY_CHECK+":"+owner;secrets=new SecretBox(key);db=factory.open();
        try{
            if(initialize)try(Statement st=db.createStatement()){
                st.execute("CREATE TABLE IF NOT EXISTS cdc_ex_records(kind VARCHAR(32) NOT NULL,id VARCHAR(64) NOT NULL,name VARCHAR(128) NOT NULL,body LONGBLOB NOT NULL,state VARCHAR(32) NOT NULL,error TEXT,source_identity TEXT,sink_identity TEXT,updated BIGINT NOT NULL,PRIMARY KEY(kind,id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
                st.execute("CREATE TABLE IF NOT EXISTS cdc_ex_audit(seq BIGINT PRIMARY KEY AUTO_INCREMENT,at BIGINT NOT NULL,action VARCHAR(128) NOT NULL,resource VARCHAR(128) NOT NULL,result TEXT NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
                st.execute("CREATE TABLE IF NOT EXISTS cdc_ex_secrets_check(id INT PRIMARY KEY,value BLOB NOT NULL) ENGINE=InnoDB");
            }
            verify(db,initialize);
        }catch(Exception e){db.close();throw e;}
    }
    public ControlStore(Path file,byte[] key)throws Exception{this(file,key,null,true);}
    public ControlStore(Path file,byte[] key,String owner,boolean initialize)throws Exception{
        if(!initialize&&!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Bound SQLite control store is missing");
        if(Files.isSymbolicLink(file))throw new IllegalStateException("Invalid SQLite control store path");
        mysql=false;remote=null;checkValue=owner==null?KEY_CHECK:KEY_CHECK+":"+owner;
        Class.forName("org.sqlite.JDBC");Files.createDirectories(file.toAbsolutePath().getParent());
        secrets=new SecretBox(key);db=DriverManager.getConnection("jdbc:sqlite:"+file.toAbsolutePath());
        try(Statement s=db.createStatement()){
            s.execute("PRAGMA journal_mode=WAL");s.execute("PRAGMA synchronous=FULL");s.execute("PRAGMA busy_timeout=5000");
            if(initialize){
            s.execute("CREATE TABLE IF NOT EXISTS records(kind TEXT NOT NULL,id TEXT NOT NULL,name TEXT NOT NULL,body BLOB NOT NULL,state TEXT NOT NULL,error TEXT,source_identity TEXT,sink_identity TEXT,updated INTEGER NOT NULL,PRIMARY KEY(kind,id))");
            s.execute("CREATE TABLE IF NOT EXISTS audit(seq INTEGER PRIMARY KEY AUTOINCREMENT,at INTEGER NOT NULL,action TEXT NOT NULL,resource TEXT NOT NULL,result TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS secrets_check(id INTEGER PRIMARY KEY CHECK(id=1),value BLOB NOT NULL)");
            }
        }
        try{verify(db,initialize);}catch(Exception e){db.close();throw e;}
        if(Files.getFileStore(file).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
    }
    public synchronized void create(String kind,String id,String name,String json)throws Exception{
        id(id);if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid display name");
        try(PreparedStatement s=prepare("INSERT INTO records(kind,id,name,body,state,updated) VALUES(?,?,?,?,?,?)")){
            s.setString(1,kind);s.setString(2,id);s.setString(3,name);s.setBytes(4,secrets.seal(kind+":"+id,json));s.setString(5,"DRAFT");s.setLong(6,System.currentTimeMillis());s.executeUpdate();
        }audit("CREATE_"+kind.toUpperCase(Locale.ROOT),id,"OK");
    }
    public synchronized void replace(String kind,String id,String name,String json)throws Exception{
        id(id);if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid display name");
        try(PreparedStatement s=prepare("UPDATE records SET name=?,body=?,updated=? WHERE kind=? AND id=?")){
            s.setString(1,name);s.setBytes(2,secrets.seal(kind+":"+id,json));s.setLong(3,System.currentTimeMillis());s.setString(4,kind);s.setString(5,id);if(s.executeUpdate()!=1)throw new NoSuchElementException("Resource not found");
        }audit("UPDATE_"+kind.toUpperCase(Locale.ROOT),id,"OK");
    }
    public synchronized Record get(String kind,String id)throws Exception{
        id(id);try(PreparedStatement s=prepare("SELECT * FROM records WHERE kind=? AND id=?")){s.setString(1,kind);s.setString(2,id);try(ResultSet r=s.executeQuery()){if(!r.next())throw new NoSuchElementException("Resource not found");return record(r);}}
    }
    private Record record(ResultSet r)throws Exception{
        String kind=r.getString("kind"),id=r.getString("id");return new Record(kind,id,r.getString("name"),secrets.open(kind+":"+id,r.getBytes("body")),r.getString("state"),r.getString("error"),r.getString("source_identity"),r.getString("sink_identity"),r.getLong("updated"));
    }
    public synchronized List<Record> list(String kind)throws Exception{
        List<Record> rows=new ArrayList<>();try(PreparedStatement s=prepare("SELECT * FROM records WHERE kind=? ORDER BY id")){s.setString(1,kind);try(ResultSet r=s.executeQuery()){while(r.next())rows.add(record(r));}}return rows;
    }
    public synchronized void state(String id,String state,String error)throws Exception{
        try(PreparedStatement s=prepare("UPDATE records SET state=?,error=?,updated=? WHERE kind='task' AND id=?")){s.setString(1,state);s.setString(2,error);s.setLong(3,System.currentTimeMillis());s.setString(4,id);s.executeUpdate();}
    }
    public synchronized void identities(String id,String source,String sink)throws Exception{
        try(PreparedStatement s=prepare("UPDATE records SET source_identity=?,sink_identity=? WHERE kind='task' AND id=?")){s.setString(1,source);s.setString(2,sink);s.setString(3,id);s.executeUpdate();}
    }
    public synchronized void recoverInterrupted()throws Exception{
        try(PreparedStatement s=prepare("UPDATE records SET state='STOPPED',error='INTERRUPTED_REVIEW_AND_RESUME' WHERE kind='task' AND state IN ('STARTING','CAPTURING','RETRYING','STOPPING','PAUSING')")){s.executeUpdate();}
    }
    public synchronized void audit(String action,String resource,String result)throws Exception{
        try(PreparedStatement s=prepare("INSERT INTO audit(at,action,resource,result) VALUES(?,?,?,?)")){s.setLong(1,System.currentTimeMillis());s.setString(2,action);s.setString(3,resource);s.setString(4,result);s.executeUpdate();}
        long newest;
        try(PreparedStatement s=prepare("SELECT COALESCE(MAX(seq),0) FROM audit");ResultSet r=s.executeQuery()){r.next();newest=r.getLong(1);}
        try(PreparedStatement s=prepare("DELETE FROM audit WHERE seq <= ?")){s.setLong(1,newest-10000);s.executeUpdate();}
    }
    public synchronized List<Audit> audits()throws Exception{
        List<Audit> result=new ArrayList<>();try(PreparedStatement s=prepare("SELECT at,action,resource,result FROM audit ORDER BY seq DESC LIMIT 100");ResultSet r=s.executeQuery()){while(r.next())result.add(new Audit(r.getLong(1),r.getString(2),r.getString(3),r.getString(4)));}return result;
    }
    @Override public synchronized Snapshot snapshot()throws Exception{
        List<Record> all=new ArrayList<>();List<Audit> history=new ArrayList<>();
        try(PreparedStatement s=prepare("SELECT * FROM records ORDER BY kind,id");ResultSet r=s.executeQuery()){while(r.next())all.add(record(r));}
        try(PreparedStatement s=prepare("SELECT at,action,resource,result FROM audit ORDER BY seq ASC");ResultSet r=s.executeQuery()){while(r.next())history.add(new Audit(r.getLong(1),r.getString(2),r.getString(3),r.getString(4)));}
        return new Snapshot(all,history);
    }
    @Override public synchronized void importSnapshot(Snapshot snapshot)throws Exception{
        Snapshot existing=snapshot();
        if(!existing.records().isEmpty()||!existing.audit().isEmpty())throw new IllegalStateException("Migration destination is not empty");
        Connection c=connection();c.setAutoCommit(false);
        try{
            try(PreparedStatement s=c.prepareStatement(sql("INSERT INTO records(kind,id,name,body,state,error,source_identity,sink_identity,updated) VALUES(?,?,?,?,?,?,?,?,?)"))){
                for(Record r:snapshot.records()){
                    id(r.id());s.setString(1,r.kind());s.setString(2,r.id());s.setString(3,r.name());s.setBytes(4,secrets.seal(r.kind()+":"+r.id(),r.json()));s.setString(5,r.state());s.setString(6,r.error());s.setString(7,r.sourceIdentity());s.setString(8,r.sinkIdentity());s.setLong(9,r.updated());s.executeUpdate();
                }
            }
            try(PreparedStatement s=c.prepareStatement(sql("INSERT INTO audit(at,action,resource,result) VALUES(?,?,?,?)"))){
                for(Audit a:snapshot.audit()){s.setLong(1,a.timestamp());s.setString(2,a.action());s.setString(3,a.resource());s.setString(4,a.result());s.executeUpdate();}
            }
            c.commit();
        }catch(Exception e){try{c.rollback();}catch(SQLException rollback){e.addSuppressed(rollback);}throw e;}
        finally{c.setAutoCommit(true);}
    }
    public static void id(String id){if(id==null||!id.matches("[A-Za-z0-9_-]{1,64}"))throw new IllegalArgumentException("Invalid resource ID");}
    @Override public synchronized void close()throws SQLException{if(db!=null)db.close();}
}
