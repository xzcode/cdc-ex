package com.cdcex.storage;

import com.cdcex.core.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
public final class SqliteJournal implements AutoCloseable {
    public record Entry(long sequence, ChangeEvent event) {}
    public record Stats(long total, long pending, long bytes, long acknowledged) {}
    private final Connection db;
    private final long byteLimit;
    private final long eventLimit;
    public SqliteJournal(Path file, long byteLimit, long eventLimit) throws Exception {
        if (byteLimit < 1 || eventLimit < 1) throw new IllegalArgumentException("Invalid journal limits");
        Files.createDirectories(file.toAbsolutePath().getParent());
        this.byteLimit=byteLimit; this.eventLimit=eventLimit;
        Class.forName("org.sqlite.JDBC");
        db=DriverManager.getConnection("jdbc:sqlite:"+file.toAbsolutePath());
        try(Statement s=db.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL"); s.execute("PRAGMA synchronous=FULL");
            s.execute("PRAGMA busy_timeout=5000");
            s.execute("CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, bytes INTEGER NOT NULL, op TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS metadata(k TEXT PRIMARY KEY, v TEXT NOT NULL)");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) VALUES ('ack','0')");
        } catch(Exception e) { db.close(); throw e; }
    }
    public synchronized String metadata(String k) throws SQLException {
        try(PreparedStatement s=db.prepareStatement("SELECT v FROM metadata WHERE k=?")) {
            s.setString(1,k); try(ResultSet r=s.executeQuery()) { return r.next()?r.getString(1):null; }
        }
    }
    public synchronized void initialize(String fingerprint) throws SQLException {
        String old=metadata("fingerprint");
        if(old!=null && !old.equals(fingerprint)) throw new IllegalStateException("Task configuration changed; use a new generation");
        try(PreparedStatement s=db.prepareStatement("INSERT OR IGNORE INTO metadata(k,v) VALUES ('fingerprint',?)")) {
            s.setString(1,fingerprint); s.executeUpdate();
        }
    }
    public synchronized boolean hasStreamingEvent() throws SQLException {
        try(Statement s=db.createStatement(); ResultSet r=s.executeQuery("SELECT 1 FROM events WHERE op IN ('INSERT','UPDATE','DELETE') LIMIT 1")) { return r.next(); }
    }
    public synchronized void append(List<ChangeEvent> events) throws Exception {
        db.setAutoCommit(false);
        try {
            try(PreparedStatement s=db.prepareStatement("INSERT INTO events(id,payload,bytes,op) VALUES(?,?,?,?) ON CONFLICT(id) DO NOTHING")) {
                for(ChangeEvent event:events) {
                    String json=EventCodec.json(event);
                    s.setString(1,event.id()); s.setString(2,json);
                    s.setLong(3,json.getBytes(StandardCharsets.UTF_8).length); s.setString(4,event.operation().name()); s.addBatch();
                }
                s.executeBatch();
            }
            Stats st=stats();
            if(st.bytes()>byteLimit || st.total()>eventLimit) throw new IllegalStateException("Journal capacity reached; capture blocked without offset acknowledgement");
            db.commit();
        } catch(Exception e) { db.rollback(); throw e; } finally { db.setAutoCommit(true); }
    }
    public synchronized List<Entry> next(int limit) throws SQLException {
        if(limit<1) throw new IllegalArgumentException("Invalid batch size");
        long ack=Long.parseLong(metadata("ack"));
        List<Entry> result=new ArrayList<>();
        try(PreparedStatement s=db.prepareStatement("SELECT seq,payload FROM events WHERE seq>? ORDER BY seq LIMIT ?")) {
            s.setLong(1,ack); s.setInt(2,limit);
            try(ResultSet r=s.executeQuery()) { while(r.next()) result.add(new Entry(r.getLong(1),EventCodec.decode(r.getString(2)))); }
        }
        return List.copyOf(result);
    }
    public synchronized void acknowledge(List<Entry> batch) throws SQLException {
        if(batch.isEmpty()) return;
        List<Entry> expected=next(batch.size());
        if(expected.size()!=batch.size()) throw new IllegalStateException("Acknowledgement exceeds pending queue");
        for(int i=0;i<batch.size();i++) if(batch.get(i).sequence()!=expected.get(i).sequence() || !batch.get(i).event().id().equals(expected.get(i).event().id()))
            throw new IllegalStateException("Noncontiguous acknowledgement");
        try(PreparedStatement s=db.prepareStatement("UPDATE metadata SET v=? WHERE k='ack'")) {
            s.setString(1,Long.toString(batch.getLast().sequence())); s.executeUpdate();
        }
    }
    public synchronized Stats stats() throws SQLException {
        long ack=Long.parseLong(metadata("ack"));
        try(PreparedStatement s=db.prepareStatement("SELECT count(*),coalesce(sum(CASE WHEN seq>? THEN 1 ELSE 0 END),0),coalesce(sum(bytes),0) FROM events")) {
            s.setLong(1,ack); try(ResultSet r=s.executeQuery()){r.next();return new Stats(r.getLong(1),r.getLong(2),r.getLong(3),ack);}
        }
    }
    @Override public synchronized void close() throws SQLException { db.close(); }
}
