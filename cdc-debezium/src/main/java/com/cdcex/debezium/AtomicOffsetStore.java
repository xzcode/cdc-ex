package com.cdcex.debezium;

import com.cdcex.core.EventCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

/** Kafka Connect offset storage with atomic replacement, fsync, and fail-closed corruption handling. */
public final class AtomicOffsetStore extends MemoryOffsetBackingStore {
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final long MAX_BYTES=16L*1024*1024;
    private Path file;
    public AtomicOffsetStore() {}
    public AtomicOffsetStore(Converter ignored) {}
    @Override public void configure(WorkerConfig config) {
        super.configure(config);
        Object value=config.originals().get("offset.storage.file.filename");
        if(value==null) throw new ConnectException("Offset file is required");
        file=Path.of(value.toString()).toAbsolutePath();
    }
    @Override public synchronized void start() {
        if(file==null) throw new ConnectException("Offset store is not configured");
        data=new HashMap<>();
        if(Files.exists(file)) {
            try { for(var entry:readRaw(file).entrySet()) data.put(ByteBuffer.wrap(Base64.getDecoder().decode(entry.getKey())),entry.getValue()==null?null:ByteBuffer.wrap(Base64.getDecoder().decode(entry.getValue()))); }
            catch(Exception e) { throw new ConnectException("Offset state is corrupt; refusing to reset",e); }
        }
        super.start();
    }
    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy=value.duplicate(); byte[] result=new byte[copy.remaining()]; copy.get(result); return result;
    }
    @Override protected void save() {
        Map<String,String> raw=new TreeMap<>();
        for(var entry:data.entrySet()) {
            if(entry.getKey()==null) throw new ConnectException("Null offset key");
            raw.put(Base64.getEncoder().encodeToString(bytes(entry.getKey())),entry.getValue()==null?null:Base64.getEncoder().encodeToString(bytes(entry.getValue())));
        }
        try { writeAtomic(file,JSON.writeValueAsBytes(Map.of("version",1,"entries",raw,"sha256",EventCodec.digest(raw)))); }
        catch(Exception e) { throw new ConnectException("Cannot durably save source offset",e); }
    }
    public static void writeAtomic(Path target,byte[] bytes) throws java.io.IOException {
        if(bytes.length>MAX_BYTES) throw new java.io.IOException("Checkpoint too large");
        Path parent=target.toAbsolutePath().getParent(); Files.createDirectories(parent);
        Path tmp=Files.createTempFile(parent,".cdc-offset-",".tmp");
        try {
            try(var c=FileChannel.open(tmp,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer=ByteBuffer.wrap(bytes); while(buffer.hasRemaining()) c.write(buffer); c.force(true);
            }
            Files.move(tmp,target,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            try(var dir=FileChannel.open(parent,StandardOpenOption.READ)) { dir.force(true); }
        } finally { Files.deleteIfExists(tmp); }
    }
    private static Map<String,String> readRaw(Path path) throws java.io.IOException {
        long size=Files.size(path); if(size<1||size>MAX_BYTES) throw new java.io.IOException("Invalid offset state length");
        JsonNode node=JSON.readTree(Files.readAllBytes(path));
        if(node==null||node.path("version").asInt()!=1||!node.path("entries").isObject()) throw new java.io.IOException("Invalid offset state format");
        Map<String,String> result=JSON.convertValue(node.get("entries"),new TypeReference<>(){});
        if(!EventCodec.digest(result).equals(node.path("sha256").asText())) throw new java.io.IOException("Offset checkpoint checksum mismatch");
        for(var e:result.entrySet()) { Base64.getDecoder().decode(e.getKey()); if(e.getValue()!=null) Base64.getDecoder().decode(e.getValue()); }
        return result;
    }
    public static List<Map<String,Object>> readOffsets(Path path) throws java.io.IOException {
        List<Map<String,Object>> offsets=new ArrayList<>();
        for(String encoded:readRaw(path).values()) {
            if(encoded==null) continue;
            JsonNode value=JSON.readTree(Base64.getDecoder().decode(encoded));
            if(value!=null&&value.has("payload")&&value.has("schema")) value=value.get("payload");
            if(value==null||!value.isObject()||value.isEmpty()) throw new java.io.IOException("Invalid source offset payload");
            offsets.add(JSON.convertValue(value,new TypeReference<>(){}));
        }
        if(offsets.isEmpty()) throw new java.io.IOException("Missing durable source offsets");
        return List.copyOf(offsets);
    }
    public static boolean snapshotComplete(Path path) throws java.io.IOException {
        for(var offset:readOffsets(path)) {
            Object snapshot=offset.get("snapshot");
            boolean active=snapshot!=null&&!Boolean.FALSE.equals(snapshot)&&!"false".equalsIgnoreCase(snapshot.toString());
            if(active&&!Boolean.TRUE.equals(offset.get("snapshot_completed"))&&!Boolean.TRUE.equals(offset.get("last_snapshot_record"))) return false;
        }
        return true;
    }
    @Override public Set<Map<String,Object>> connectorPartitions(String connectorName) {
        Set<Map<String,Object>> partitions=new HashSet<>();
        for(ByteBuffer rawKey:data.keySet()) {
            if(rawKey==null) continue;
            try {
                JsonNode key=JSON.readTree(bytes(rawKey));
                if(key.has("payload")&&key.has("schema")) key=key.get("payload");
                if(key.isArray()&&key.size()==2&&connectorName.equals(key.get(0).asText())&&key.get(1).isObject()) partitions.add(JSON.convertValue(key.get(1),new TypeReference<>(){}));
            } catch(Exception e) { throw new ConnectException("Invalid connector partition key",e); }
        }
        return partitions;
    }
}
