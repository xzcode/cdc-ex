package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.mysql.*;
import com.cdcex.runtime.TaskRuntime;
import com.cdcex.storage.SqliteJournal;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
@SpringBootApplication
public class CdcApplication {
    private static String required(Map<String,String> env,String key){String value=env.get(key);if(value==null||value.isBlank())throw new IllegalArgumentException("Missing "+key);return value;}
    private static MysqlTaskConfig.Endpoint endpoint(Map<String,String> env,String p){
        return new MysqlTaskConfig.Endpoint(required(env,p+"_HOST"),Integer.parseInt(env.getOrDefault(p+"_PORT","3306")),required(env,p+"_USER"),required(env,p+"_PASSWORD"),required(env,p+"_DATABASE"),required(env,p+"_TABLE"),env.getOrDefault(p+"_SSL_MODE","VERIFY_IDENTITY"));
    }
    public static void main(String[] args) throws Exception {
        Map<String,String> env=System.getenv();
        if(!"true".equalsIgnoreCase(env.get("CDC_RUN"))){System.out.println("CDC EX M1 preview. Set CDC_RUN=true and configure a dedicated task; see docs/M1_RUNBOOK.md.");return;}
        Path dir=Path.of(required(env,"CDC_DATA_DIR")).toAbsolutePath().normalize();
        Files.createDirectories(dir);
        if(Files.getFileStore(dir).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(dir,PosixFilePermissions.fromString("rwx------"));
        var cfg=new MysqlTaskConfig(endpoint(env,"CDC_SOURCE"),endpoint(env,"CDC_TARGET"),Long.parseLong(required(env,"CDC_REPLICATION_SERVER_ID")),dir,env.getOrDefault("CDC_SNAPSHOT_MODE","initial"),Long.parseLong(env.getOrDefault("CDC_JOURNAL_BYTES","268435456")),Long.parseLong(env.getOrDefault("CDC_JOURNAL_EVENTS","1000000")));
        try(FileChannel lockFile=FileChannel.open(dir.resolve("task.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);FileLock lock=lockFile.tryLock()){
            if(lock==null)throw new IllegalStateException("Task directory already owned by another process");
            boolean fresh=!Files.exists(dir.resolve("journal.db"));
            if(fresh&&(Files.exists(dir.resolve("offsets.dat"))||Files.exists(dir.resolve("schema-history.dat"))))throw new IllegalStateException("Orphaned connector state");
            var schemas=MysqlSchema.preflight(cfg,fresh);
            if("true".equalsIgnoreCase(env.get("CDC_PREFLIGHT_ONLY"))){System.out.println("Preflight passed; no source capture started");return;}
            try(var journal=new SqliteJournal(dir.resolve("journal.db"),cfg.journalBytes(),cfg.journalEvents())){
                if(!fresh){
                    if(journal.metadata("fingerprint")==null)throw new IllegalStateException("Uninitialized journal; operator review required");
                    for(String name:new String[]{"offsets.dat","schema-history.dat"})if(!Files.isRegularFile(dir.resolve(name))||Files.size(dir.resolve(name))==0)throw new IllegalStateException("Missing connector recovery state; refusing to reset");
                    if("initial".equals(cfg.snapshotMode())&&!journal.hasStreamingEvent())throw new IllegalStateException("Initial snapshot recovery not proven; operator reconciliation required");
                }
                var schema=schemas.target();journal.initialize(EventCodec.digest(java.util.List.of(cfg.fingerprint(),schemas.source(),schema)));
                var runtime=new TaskRuntime(new MysqlSource(cfg),new MysqlSink(cfg,schema),journal,128,12);
                var stopped=new java.util.concurrent.CountDownLatch(1);
                Thread hook=new Thread(()->{runtime.requestStop();try{stopped.await(45,java.util.concurrent.TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}},"cdc-stop");
                Runtime.getRuntime().addShutdownHook(hook);
                try{runtime.run();}finally{stopped.countDown();try{Runtime.getRuntime().removeShutdownHook(hook);}catch(IllegalStateException ignored){}}
            }
        }
    }
}
