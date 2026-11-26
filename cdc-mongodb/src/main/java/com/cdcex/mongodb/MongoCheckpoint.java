package com.cdcex.mongodb;

import com.cdcex.core.EventCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import org.bson.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

/** Atomic, forced checkpoint; it is advanced only after durable journal acceptance. */
final class MongoCheckpoint {
    private static final ObjectMapper JSON=new ObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private record Saved(int format,Map<String,Object> state,String sha256){}
    record State(String identity,String mode,String phase,BsonTimestamp boundary,BsonDocument token) {
        State {
            if(identity==null || !java.util.Set.of("initial","no_data").contains(mode)
                || !java.util.Set.of("snapshot","stream").contains(phase) || boundary==null)
                throw new IllegalArgumentException("Invalid MongoDB checkpoint");
        }
        State stream(BsonDocument nextToken){return new State(identity,mode,"stream",boundary,nextToken);}
    }
    private final Path path;
    MongoCheckpoint(Path directory){this.path=directory.resolve("mongodb-checkpoint.json");}
    boolean exists(){return Files.isRegularFile(path);}
    State read() throws Exception {
        long size=Files.size(path);
        if(size<1 || size>1_048_576)throw new IllegalStateException("Invalid MongoDB checkpoint length");
        var document=JSON.readTree(Files.readString(path));
        if(!document.isObject() || !document.hasNonNull("format") || !document.hasNonNull("state") || !document.hasNonNull("sha256"))
            throw new IllegalStateException("Unsupported MongoDB checkpoint format; explicit reconciliation required");
        Saved saved=JSON.treeToValue(document,Saved.class);
        if(saved.format()!=1 || saved.state()==null || !EventCodec.digest(saved.state()).equals(saved.sha256()))
            throw new IllegalStateException("MongoDB checkpoint checksum mismatch; refusing offset reset");
        BsonDocument d=BsonDocument.parse(EventCodec.json(saved.state()));
        if(d.getInt32("version").getValue()!=1)throw new IllegalStateException("Unknown MongoDB checkpoint version");
        return new State(d.getString("identity").getValue(),d.getString("mode").getValue(),
            d.getString("phase").getValue(),d.getTimestamp("boundary"),d.getDocument("token",null));
    }
    void write(State state) throws Exception {
        Files.createDirectories(path.getParent());
        BsonDocument d=new BsonDocument("version",new BsonInt32(1))
            .append("identity",new BsonString(state.identity())).append("mode",new BsonString(state.mode()))
            .append("phase",new BsonString(state.phase())).append("boundary",state.boundary());
        if(state.token()!=null)d.append("token",state.token());
        Map<String,Object> payload=JSON.readValue(d.toJson(BsonValues.CANONICAL),new TypeReference<LinkedHashMap<String,Object>>(){});
        byte[] bytes=EventCodec.json(new Saved(1,payload,EventCodec.digest(payload))).getBytes(StandardCharsets.UTF_8);
        if(bytes.length>1_048_576)throw new IllegalStateException("MongoDB checkpoint is too large");
        Path temp=Files.createTempFile(path.getParent(),".mongo-checkpoint-",".tmp");
        try {
            if(Files.getFileStore(temp).supportsFileAttributeView("posix"))
                Files.setPosixFilePermissions(temp,PosixFilePermissions.fromString("rw-------"));
            try(FileChannel file=FileChannel.open(temp,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())file.write(buffer);file.force(true);
            }
            Files.move(temp,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            try(FileChannel directory=FileChannel.open(path.getParent(),StandardOpenOption.READ)){directory.force(true);}
        } finally {Files.deleteIfExists(temp);}
    }
}
