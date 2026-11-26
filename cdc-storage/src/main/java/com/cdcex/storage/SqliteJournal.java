package com.cdcex.storage;

import com.cdcex.core.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
public final class SqliteJournal implements AutoCloseable {
    public static final class CapacityException extends IllegalStateException {
        public CapacityException(){super("Journal capacity reached; no source offset was acknowledged");}
    }
    public record Entry(long sequence, ChangeEvent event) {}
    public record Stats(long total, long pending, long bytes, long acknowledged) {}
    private final Connection db;
    private final long byteLimit;
    private final long eventLimit;
    private final Path file;
    public SqliteJournal(Path file, long byteLimit, long eventLimit) throws Exception {
        if (byteLimit < 1 || eventLimit < 1) throw new IllegalArgumentException("Invalid journal limits");
        Files.createDirectories(file.toAbsolutePath().getParent());
        this.byteLimit=byteLimit; this.eventLimit=eventLimit;this.file=file.toAbsolutePath();
        Class.forName("org.sqlite.JDBC");
        db=DriverManager.getConnection("jdbc:sqlite:"+file.toAbsolutePath());
        try(Statement s=db.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL"); s.execute("PRAGMA synchronous=FULL");
            s.execute("PRAGMA busy_timeout=5000");
            s.execute("CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, bytes INTEGER NOT NULL, op TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS metadata(k TEXT PRIMARY KEY, v TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY, digest TEXT NOT NULL, bytes INTEGER NOT NULL)");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) VALUES ('ack','0')");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) SELECT 'event_count',count(*) FROM events");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) SELECT 'event_bytes',coalesce(sum(bytes),0) FROM events");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) SELECT 'consumed_count',count(*) FROM events WHERE seq<=CAST((SELECT v FROM metadata WHERE k='ack') AS INTEGER)");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) VALUES ('receipt_bytes','0')");
            s.execute("INSERT OR IGNORE INTO metadata(k,v) SELECT 'has_streamed',EXISTS(SELECT 1 FROM events WHERE op IN ('INSERT','UPDATE','DELETE'))");
            s.execute("CREATE TRIGGER IF NOT EXISTS events_account_insert AFTER INSERT ON events BEGIN UPDATE metadata SET v=CAST(v AS INTEGER)+1 WHERE k='event_count'; UPDATE metadata SET v=CAST(v AS INTEGER)+NEW.bytes WHERE k='event_bytes'; END");
            s.execute("CREATE TRIGGER IF NOT EXISTS events_account_delete AFTER DELETE ON events BEGIN UPDATE metadata SET v=CAST(v AS INTEGER)-OLD.bytes WHERE k='event_bytes'; END");
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
        return "1".equals(metadata("has_streamed"));
    }
    public synchronized void append(List<ChangeEvent> events) throws Exception {
        long physical=Files.size(file);Path wal=Path.of(file+"-wal");if(Files.exists(wal))physical+=Files.size(wal);
        if(Files.getFileStore(file).getUsableSpace()<64L*1024*1024||physical>Math.addExact(Math.multiplyExact(byteLimit,3),16L*1024*1024))throw new CapacityException();
        db.setAutoCommit(false);
        try {
            try(PreparedStatement s=db.prepareStatement("INSERT INTO events(id,payload,bytes,op) VALUES(?,?,?,?) ON CONFLICT(id) DO NOTHING");
                PreparedStatement existing=db.prepareStatement("SELECT payload FROM events WHERE id=?");
                PreparedStatement receipt=db.prepareStatement("SELECT digest FROM receipts WHERE id=?")) {
                for(ChangeEvent event:events) {
                    String json=EventCodec.json(event);
                    receipt.setString(1,event.id());
                    try(ResultSet r=receipt.executeQuery()){
                        if(r.next()){
                            if(!r.getString(1).equals(EventCodec.payloadDigest(event)))throw new IllegalStateException("Event identity collision after compaction");
                            continue;
                        }
                    }
                    s.setString(1,event.id()); s.setString(2,json);
                    s.setLong(3,json.getBytes(StandardCharsets.UTF_8).length); s.setString(4,event.operation().name());
                    if(s.executeUpdate()==0) {
                        existing.setString(1,event.id());
                        try(ResultSet r=existing.executeQuery()) {
                            if(!r.next() || !r.getString(1).equals(json))
                                throw new IllegalStateException("Event identity collision; capture blocked");
                        }
                    }
                }
            }
            if(events.stream().anyMatch(e->Set.of(ChangeEvent.Operation.INSERT,ChangeEvent.Operation.UPDATE,ChangeEvent.Operation.DELETE).contains(e.operation())))
                try(Statement s=db.createStatement()){s.execute("UPDATE metadata SET v='1' WHERE k='has_streamed'");}
            Stats st=stats();
            if(st.bytes()>byteLimit || st.total()>eventLimit) throw new CapacityException();
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
        db.setAutoCommit(false);
        try(PreparedStatement s=db.prepareStatement("UPDATE metadata SET v=? WHERE k='ack'");
            PreparedStatement count=db.prepareStatement("UPDATE metadata SET v=CAST(v AS INTEGER)+? WHERE k='consumed_count'")) {
            s.setString(1,Long.toString(batch.getLast().sequence())); s.executeUpdate();
            count.setInt(1,batch.size()); count.executeUpdate();
            db.commit();
        } catch(SQLException | RuntimeException e) {db.rollback();throw e;} finally {db.setAutoCommit(true);}
    }
    public synchronized Stats stats() throws SQLException {
        long ack=Long.parseLong(metadata("ack"));
        long total=Long.parseLong(metadata("event_count")),consumed=Long.parseLong(metadata("consumed_count"));
        long bytes=Long.parseLong(metadata("event_bytes"))+Long.parseLong(metadata("receipt_bytes"));
        if(consumed>total) throw new IllegalStateException("Journal accounting inconsistency");
        return new Stats(total,total-consumed,bytes,ack);
    }
    /** Drop committed payloads while retaining durable identities for source replay. Never expires identities implicitly. */
    public synchronized void compact() throws SQLException {
        long ack=Long.parseLong(metadata("ack")),added=0;
        db.setAutoCommit(false);
        try(PreparedStatement rows=db.prepareStatement("SELECT id,payload FROM events WHERE seq<=?");
            PreparedStatement put=db.prepareStatement("INSERT INTO receipts(id,digest,bytes) VALUES(?,?,?)");
            PreparedStatement delete=db.prepareStatement("DELETE FROM events WHERE seq<=?");
            PreparedStatement count=db.prepareStatement("UPDATE metadata SET v=CAST(v AS INTEGER)+? WHERE k='receipt_bytes'")){
            rows.setLong(1,ack);
            try(ResultSet r=rows.executeQuery()){
                while(r.next()){
                    String id=r.getString(1);long bytes=id.getBytes(StandardCharsets.UTF_8).length+96L;
                    put.setString(1,id);put.setString(2,EventCodec.payloadDigest(EventCodec.decode(r.getString(2))));put.setLong(3,bytes);put.executeUpdate();added+=bytes;
                }
            }
            delete.setLong(1,ack);delete.executeUpdate();count.setLong(1,added);count.executeUpdate();
            if(stats().bytes()>byteLimit){db.rollback();return;}
            db.commit();
        }catch(SQLException|RuntimeException e){db.rollback();throw e;}finally{db.setAutoCommit(true);}
        try(Statement s=db.createStatement()){s.execute("PRAGMA wal_checkpoint(TRUNCATE)");}
    }
    @Override public synchronized void close() throws SQLException { db.close(); }
}
