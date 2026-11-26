package com.cdcex.clickhouse;

import com.cdcex.core.EventCodec;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

final class ClickHouseCheckpoint {
    private record Saved(int format,State state,String sha256){}
    record State(int version,String identity,String schema,String mode,String phase,String generation,List<String> cursor) {
        State {
            if(version!=1 || identity==null || schema==null || generation==null
                || !Set.of("snapshot","cursor","changelog").contains(mode)
                || !Set.of("ready","running","complete","stream").contains(phase))
                throw new IllegalArgumentException("Invalid ClickHouse checkpoint");
            cursor=cursor==null?null:List.copyOf(cursor);
        }
        State phase(String phase){return new State(version,identity,schema,mode,phase,generation,cursor);}
        State cursor(List<String> next){return new State(version,identity,schema,mode,"stream",generation,next);}
    }
    private final Path file;
    ClickHouseCheckpoint(Path directory){file=directory.resolve("clickhouse-checkpoint.json");}
    boolean exists(){return Files.isRegularFile(file);}
    State read()throws Exception {
        long size=Files.size(file);
        if(size<1 || size>1_048_576)throw new IllegalStateException("Invalid ClickHouse checkpoint length");
        var document=ClickHouseHttp.JSON.readTree(Files.readString(file));
        if(!document.isObject() || !document.hasNonNull("format") || !document.hasNonNull("state") || !document.hasNonNull("sha256"))
            throw new IllegalStateException("Unsupported ClickHouse checkpoint format; explicit reconciliation required");
        Saved saved=ClickHouseHttp.JSON.treeToValue(document,Saved.class);
        if(saved.format()!=1 || saved.state()==null || !EventCodec.digest(saved.state()).equals(saved.sha256()))
            throw new IllegalStateException("ClickHouse checkpoint checksum mismatch; refusing cursor reset");
        return saved.state();
    }
    void write(State state)throws Exception {
        byte[] bytes=EventCodec.json(new Saved(1,state,EventCodec.digest(state))).getBytes(StandardCharsets.UTF_8);
        if(bytes.length>1_048_576)throw new IllegalStateException("ClickHouse checkpoint too large");
        Files.createDirectories(file.getParent());Path temporary=Files.createTempFile(file.getParent(),".clickhouse-",".tmp");
        try {
            if(Files.getFileStore(temporary).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(temporary,PosixFilePermissions.fromString("rw-------"));
            try(var channel=FileChannel.open(temporary,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer=ByteBuffer.wrap(bytes);
                while(buffer.hasRemaining())channel.write(buffer);channel.force(true);
            }
            Files.move(temporary,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            try(var directory=FileChannel.open(file.getParent(),StandardOpenOption.READ)){directory.force(true);}
        } finally {Files.deleteIfExists(temporary);}
    }
}
