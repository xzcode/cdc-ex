package com.cdcex.clickhouse;

import com.cdcex.core.EventCodec;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

final class ClickHouseCheckpoint {
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
        if(Files.size(file)>1_048_576)throw new IllegalStateException("ClickHouse checkpoint too large");
        return ClickHouseHttp.JSON.readValue(Files.readString(file),State.class);
    }
    void write(State state)throws Exception {
        Files.createDirectories(file.getParent());Path temporary=Files.createTempFile(file.getParent(),".clickhouse-",".tmp");
        try {
            if(Files.getFileStore(temporary).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(temporary,PosixFilePermissions.fromString("rw-------"));
            try(var channel=FileChannel.open(temporary,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer=ByteBuffer.wrap(EventCodec.json(state).getBytes(StandardCharsets.UTF_8));
                while(buffer.hasRemaining())channel.write(buffer);channel.force(true);
            }
            Files.move(temporary,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            try(var directory=FileChannel.open(file.getParent(),StandardOpenOption.READ)){directory.force(true);}
        } finally {Files.deleteIfExists(temporary);}
    }
}
