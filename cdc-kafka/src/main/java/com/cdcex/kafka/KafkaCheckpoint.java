package com.cdcex.kafka;

import com.cdcex.core.EventCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import static java.nio.file.StandardOpenOption.*;

/** Local offsets are authoritative; Kafka group commits are only a durable progress mirror. */
final class KafkaCheckpoint {
    record State(int version,KafkaMetadata metadata,String groupId,String startOffset,Map<Integer,Long> nextOffsets) {
        State { nextOffsets=Collections.unmodifiableMap(new TreeMap<>(nextOffsets)); }
    }
    private final Path path;
    private State state;
    KafkaCheckpoint(Path path,KafkaMetadata metadata,String groupId,String startOffset,boolean fresh) throws Exception {
        this.path=path;
        if(Files.exists(path)) {
            state=new ObjectMapper().readValue(Files.readString(path),State.class);
            if(fresh)throw new IllegalStateException("Orphaned Kafka checkpoint exists in a new task directory");
            if(state.version()!=1||!groupId.equals(state.groupId())||!startOffset.equals(state.startOffset()))
                throw new IllegalStateException("Kafka checkpoint configuration changed");
            state.metadata().requireSameTopic(metadata);
            for(var e:state.nextOffsets().entrySet())if(e.getKey()<0||e.getValue()<0)throw new IllegalStateException("Invalid Kafka checkpoint");
        } else {
            if(!fresh)throw new IllegalStateException("Kafka checkpoint missing; refusing automatic offset reset");
            state=new State(1,metadata,groupId,startOffset,Map.of());
        }
    }
    State state(){return state;}
    void save(KafkaMetadata metadata,Map<Integer,Long> offsets) throws Exception {
        state.metadata().requireSameTopic(metadata);
        var next=new TreeMap<>(state.nextOffsets());
        for(var e:offsets.entrySet()) {
            Long previous=next.get(e.getKey());
            if(e.getKey()<0||e.getValue()<0||(previous!=null&&e.getValue()<previous))
                throw new IllegalStateException("Kafka checkpoint cannot move backwards");
            next.put(e.getKey(),e.getValue());
        }
        State updated=new State(1,metadata,state.groupId(),state.startOffset(),next);
        writeJson(path,updated);
        state=updated;
    }
    static void writeJson(Path path,Object value) throws Exception {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Path temporary=path.resolveSibling(path.getFileName()+".tmp");
        byte[] bytes=EventCodec.json(value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try(FileChannel file=FileChannel.open(temporary,CREATE,TRUNCATE_EXISTING,WRITE)){
            ByteBuffer b=ByteBuffer.wrap(bytes);while(b.hasRemaining())file.write(b);file.force(true);
        }
        Files.move(temporary,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
        try(FileChannel directory=FileChannel.open(path.toAbsolutePath().getParent(),READ)){directory.force(true);}
    }
    static void requireRetained(long offset,long beginning,long end){
        if(offset<beginning||offset>end)throw new IllegalStateException("Kafka checkpoint is outside retained offsets; explicit reconciliation is required");
    }
}
