package com.cdcex.storage;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.util.*;

/** Control-plane records. Credentials and full endpoint settings never appear in plaintext columns. */
public final class ControlStore implements AutoCloseable {
    public record Record(String kind,String id,String name,String json,String state,String error,String sourceIdentity,String sinkIdentity,long updated){}
    public record Audit(long timestamp,String action,String resource,String result){}
    private final Connection db;private final SecretBox secrets;
    public ControlStore(Path file,byte[] key)throws Exception{
        Class.forName("org.sqlite.JDBC");Files.createDirectories(file.toAbsolutePath().getParent());
        secrets=new SecretBox(key);db=DriverManager.getConnection("jdbc:sqlite:"+file.toAbsolutePath());
        try(Statement s=db.createStatement()){
            s.execute("PRAGMA journal_mode=WAL");s.execute("PRAGMA synchronous=FULL");s.execute("PRAGMA busy_timeout=5000");
            s.execute("CREATE TABLE IF NOT EXISTS records(kind TEXT NOT NULL,id TEXT NOT NULL,name TEXT NOT NULL,body BLOB NOT NULL,state TEXT NOT NULL,error TEXT,source_identity TEXT,sink_identity TEXT,updated INTEGER NOT NULL,PRIMARY KEY(kind,id))");
            s.execute("CREATE TABLE IF NOT EXISTS audit(seq INTEGER PRIMARY KEY AUTOINCREMENT,at INTEGER NOT NULL,action TEXT NOT NULL,resource TEXT NOT NULL,result TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS secrets_check(id INTEGER PRIMARY KEY CHECK(id=1),value BLOB NOT NULL)");
        }
        try(Statement s=db.createStatement();ResultSet r=s.executeQuery("SELECT value FROM secrets_check WHERE id=1")){
            if(r.next()){if(!"cdc-ex-control-store".equals(secrets.open("key-check",r.getBytes(1))))throw new IllegalStateException("Deployment key does not match this data directory");}
            else try(PreparedStatement insert=db.prepareStatement("INSERT INTO secrets_check(id,value) VALUES(1,?)")){insert.setBytes(1,secrets.seal("key-check","cdc-ex-control-store"));insert.executeUpdate();}
        }catch(Exception e){db.close();throw e;}
        if(Files.getFileStore(file).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
    }
    public synchronized void create(String kind,String id,String name,String json)throws Exception{
        id(id);if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid display name");
        try(PreparedStatement s=db.prepareStatement("INSERT INTO records(kind,id,name,body,state,updated) VALUES(?,?,?,?,?,?)")){
            s.setString(1,kind);s.setString(2,id);s.setString(3,name);s.setBytes(4,secrets.seal(kind+":"+id,json));s.setString(5,"DRAFT");s.setLong(6,System.currentTimeMillis());s.executeUpdate();
        }audit("CREATE_"+kind.toUpperCase(Locale.ROOT),id,"OK");
    }
    public synchronized void replace(String kind,String id,String name,String json)throws Exception{
        id(id);if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid display name");
        try(PreparedStatement s=db.prepareStatement("UPDATE records SET name=?,body=?,updated=? WHERE kind=? AND id=?")){
            s.setString(1,name);s.setBytes(2,secrets.seal(kind+":"+id,json));s.setLong(3,System.currentTimeMillis());s.setString(4,kind);s.setString(5,id);if(s.executeUpdate()!=1)throw new NoSuchElementException("Resource not found");
        }audit("UPDATE_"+kind.toUpperCase(Locale.ROOT),id,"OK");
    }
    public synchronized Record get(String kind,String id)throws Exception{
        id(id);try(PreparedStatement s=db.prepareStatement("SELECT * FROM records WHERE kind=? AND id=?")){s.setString(1,kind);s.setString(2,id);try(ResultSet r=s.executeQuery()){if(!r.next())throw new NoSuchElementException("Resource not found");return record(r);}}
    }
    private Record record(ResultSet r)throws Exception{
        String kind=r.getString("kind"),id=r.getString("id");return new Record(kind,id,r.getString("name"),secrets.open(kind+":"+id,r.getBytes("body")),r.getString("state"),r.getString("error"),r.getString("source_identity"),r.getString("sink_identity"),r.getLong("updated"));
    }
    public synchronized List<Record> list(String kind)throws Exception{
        List<Record> rows=new ArrayList<>();try(PreparedStatement s=db.prepareStatement("SELECT * FROM records WHERE kind=? ORDER BY id")){s.setString(1,kind);try(ResultSet r=s.executeQuery()){while(r.next())rows.add(record(r));}}return rows;
    }
    public synchronized void state(String id,String state,String error)throws Exception{
        try(PreparedStatement s=db.prepareStatement("UPDATE records SET state=?,error=?,updated=? WHERE kind='task' AND id=?")){s.setString(1,state);s.setString(2,error);s.setLong(3,System.currentTimeMillis());s.setString(4,id);s.executeUpdate();}
    }
    public synchronized void identities(String id,String source,String sink)throws Exception{
        try(PreparedStatement s=db.prepareStatement("UPDATE records SET source_identity=?,sink_identity=? WHERE kind='task' AND id=?")){s.setString(1,source);s.setString(2,sink);s.setString(3,id);s.executeUpdate();}
    }
    public synchronized void recoverInterrupted()throws Exception{
        try(Statement s=db.createStatement()){s.executeUpdate("UPDATE records SET state='STOPPED',error='INTERRUPTED_REVIEW_AND_RESUME' WHERE kind='task' AND state IN ('STARTING','CAPTURING','RETRYING','STOPPING','PAUSING')");}
    }
    public synchronized void audit(String action,String resource,String result)throws Exception{
        try(PreparedStatement s=db.prepareStatement("INSERT INTO audit(at,action,resource,result) VALUES(?,?,?,?)")){s.setLong(1,System.currentTimeMillis());s.setString(2,action);s.setString(3,resource);s.setString(4,result);s.executeUpdate();}
        try(Statement s=db.createStatement()){s.execute("DELETE FROM audit WHERE seq < (SELECT COALESCE(MAX(seq),0)-10000 FROM audit)");}
    }
    public synchronized List<Audit> audits()throws Exception{
        List<Audit> result=new ArrayList<>();try(Statement s=db.createStatement();ResultSet r=s.executeQuery("SELECT at,action,resource,result FROM audit ORDER BY seq DESC LIMIT 100")){while(r.next())result.add(new Audit(r.getLong(1),r.getString(2),r.getString(3),r.getString(4)));}return result;
    }
    public static void id(String id){if(id==null||!id.matches("[A-Za-z0-9_-]{1,64}"))throw new IllegalArgumentException("Invalid resource ID");}
    @Override public synchronized void close()throws SQLException{db.close();}
}
