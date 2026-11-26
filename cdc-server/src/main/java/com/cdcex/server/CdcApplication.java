package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.runtime.*;
import com.fasterxml.jackson.databind.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

@SpringBootApplication
public class CdcApplication {
    private static final java.util.logging.Logger BINLOG_LOGGER=java.util.logging.Logger.getLogger("com.github.shyiko.mysql.binlog.BinaryLogClient");
    static final ObjectMapper JSON=new ObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    static String required(Map<String,String> env,String key){String v=env.get(key);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing "+key);return v;}
    private static Endpoint legacyEndpoint(Map<String,String> env,String prefix){
        Map<String,String> options=new LinkedHashMap<>();
        for(String name:List.of("HOST","PORT","USER","PASSWORD","DATABASE","TABLE","SSL_MODE")){
            String value=env.get(prefix+"_"+name);if(value!=null)options.put(name.equals("SSL_MODE")?"sslMode":name.toLowerCase(Locale.ROOT),value);
        }
        if(prefix.equals("CDC_SOURCE"))options.put("replicationServerId",required(env,"CDC_REPLICATION_SERVER_ID"));
        return new Endpoint(env.getOrDefault(prefix+"_TYPE","MYSQL"),options);
    }
    static TaskSpec task(Map<String,String> env)throws Exception{
        if(env.containsKey("CDC_TASK_FILE")){
            Path file=Path.of(env.get("CDC_TASK_FILE"));if(Files.size(file)>1048576)throw new IllegalArgumentException("Task file exceeds one MiB");
            return JSON.readValue(Files.readString(file),TaskSpec.class);
        }
        return new TaskSpec("cli",legacyEndpoint(env,"CDC_SOURCE"),legacyEndpoint(env,"CDC_TARGET"),env.getOrDefault("CDC_SNAPSHOT_MODE","initial"),null,
            Long.parseLong(env.getOrDefault("CDC_JOURNAL_BYTES","268435456")),Long.parseLong(env.getOrDefault("CDC_JOURNAL_EVENTS","1000000")),128,12);
    }
    public static void main(String[] args){
        BINLOG_LOGGER.setLevel(java.util.logging.Level.OFF);
        try{run(args);}catch(Exception e){Failure f=Failure.describe(e);System.err.println("CDC EX blocked ["+f.code()+"] "+f.message()+" Diagnostic location: "+f.location());System.exit(2);}
    }
    static void run(String[] args)throws Exception{
        Map<String,String> env=System.getenv();
        if("server".equalsIgnoreCase(env.get("CDC_MODE"))){
            String address=env.getOrDefault("CDC_BIND","127.0.0.1");
            if(!Set.of("127.0.0.1","::1","localhost").contains(address)&&!"true".equalsIgnoreCase(env.get("CDC_ALLOW_REMOTE")))throw new IllegalArgumentException("Remote binding requires explicit opt-in and a TLS reverse proxy");
            var app=new SpringApplication(CdcApplication.class);
            // CDC_BIND is the sole binding setting; generic Spring flags cannot bypass the remote opt-in.
            app.addInitializers(context->context.getEnvironment().getPropertySources().addFirst(new org.springframework.core.env.MapPropertySource("cdc-enforced-bind",Map.of("server.address",address,"server.port",env.getOrDefault("CDC_PORT","8080")))));
            app.setDefaultProperties(Map.of("server.address",address,"server.port",env.getOrDefault("CDC_PORT","8080"),"spring.main.banner-mode","off","server.error.include-message","never","server.error.include-stacktrace","never","spring.jackson.deserialization.fail-on-unknown-properties","true"));
            app.run(args);return;
        }
        if(!"true".equalsIgnoreCase(env.get("CDC_RUN"))){System.out.println("CDC EX: set CDC_MODE=server for the management UI, or CDC_RUN=true CDC_TASK_FILE=task.json CDC_DATA_DIR=/persistent/task for a single task. See docs/RUNBOOK.md.");return;}
        TaskSpec spec=task(env);Path dir=Path.of(required(env,"CDC_DATA_DIR")).toAbsolutePath().normalize();var registry=new ConnectorRegistry();
        if("true".equalsIgnoreCase(env.get("CDC_PREFLIGHT_ONLY"))){TaskRunner.validate(spec,registry,!Files.exists(dir.resolve("journal.db")));System.out.println("Preflight passed; no source capture started");return;}
        try(var runner=TaskRunner.open(spec,dir,registry)){
            var stopped=new CountDownLatch(1);
            Thread hook=new Thread(()->{runner.runtime().requestStop();try{stopped.await(45,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}},"cdc-stop");
            Runtime.getRuntime().addShutdownHook(hook);
            try{System.out.println("CDC EX task started: "+spec.id());runner.run();}
            finally{stopped.countDown();try{Runtime.getRuntime().removeShutdownHook(hook);}catch(IllegalStateException ignored){}}
        }
    }
}
