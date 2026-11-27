package com.cdcex.server;
import com.cdcex.storage.*;
import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

/** Bind external metadata to a local directory, not a distributed HA lease. */
final class ControlBinding {
    static final String FILE="control-store.binding";
    record Binding(String type,String owner,boolean ready){}
    static Binding read(Path file)throws Exception{
        if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)||Files.size(file)>256)throw new IllegalStateException("Invalid control store binding");
        List<String> parts=Files.readAllLines(file);
        if(parts.size()!=3||!Set.of("SQLITE","MYSQL","MONGODB").contains(parts.get(0))||!Set.of("PENDING","READY").contains(parts.get(2)))throw new IllegalStateException("Invalid control store binding");
        UUID.fromString(parts.get(1));return new Binding(parts.get(0),parts.get(1),parts.get(2).equals("READY"));
    }
    static void save(Path file,Binding b)throws Exception{
        Path temp=Files.createTempFile(file.getParent(),".control-binding-", ".tmp");
        try{
            if(Files.getFileStore(temp).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(temp,PosixFilePermissions.fromString("rw-------"));
            byte[] bytes=(b.type()+"\n"+b.owner()+"\n"+(b.ready()?"READY":"PENDING")+"\n").getBytes(StandardCharsets.UTF_8);
            try(FileChannel channel=FileChannel.open(temp,StandardOpenOption.WRITE)){ByteBuffer data=ByteBuffer.wrap(bytes);while(data.hasRemaining())channel.write(data);channel.force(true);}
            Files.move(temp,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            if(Files.getFileStore(file).supportsFileAttributeView("posix"))try(FileChannel parent=FileChannel.open(file.getParent(),StandardOpenOption.READ)){parent.force(true);}
        }finally{Files.deleteIfExists(temp);}
    }
    static String type(Map<String,String> env){
        String type=env.getOrDefault("CDC_CONTROL_DB_TYPE","SQLITE").trim().toUpperCase(Locale.ROOT);
        if(!Set.of("SQLITE","MYSQL","MONGODB").contains(type))throw new IllegalArgumentException("Unknown control database type");
        return type;
    }
    static ControlRepository open(Path directory,Map<String,String> env,byte[] key)throws Exception{
        return open(directory,env,key,false);
    }
    static ControlRepository openExisting(Path directory,Map<String,String> env,byte[] key)throws Exception{
        return open(directory,env,key,true);
    }
    private static ControlRepository open(Path directory,Map<String,String> env,byte[] key,boolean existing)throws Exception{
        String type=type(env);
        Path file=directory.resolve(FILE);boolean hasBinding=Files.exists(file,LinkOption.NOFOLLOW_LINKS);
        if(!hasBinding&&type.equals("SQLITE")){
            Path legacy=directory.resolve("control.db");
            // A missing control DB next to old task state is not a new deployment.
            boolean initialize=!existing&&!Files.exists(legacy,LinkOption.NOFOLLOW_LINKS)&&!Files.exists(directory.resolve("tasks"),LinkOption.NOFOLLOW_LINKS);
            return new ControlStore(legacy,key,null,initialize);
        }
        Binding binding;
        if(hasBinding){
            binding=read(file);if(!binding.type().equals(type))throw new IllegalStateException("Control store type changed; migration required");
            if(existing&&!binding.ready())throw new IllegalStateException("Finish the original control store initialization before migration");
        }else{
            if(existing)throw new IllegalStateException("Existing control store binding is missing");
            for(String path:List.of("control.db","control.db-wal","control.db-shm","tasks"))if(Files.exists(directory.resolve(path),LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Existing local control/task state requires migration");
            binding=new Binding(type,UUID.randomUUID().toString(),false);save(file,binding);
        }
        ControlRepository store=openBound(directory,env,key,binding,!binding.ready());
        try{if(!binding.ready())save(file,new Binding(type,binding.owner(),true));return store;}
        catch(Exception e){store.close();throw e;}
    }
    static ControlRepository openBound(Path directory,Map<String,String> env,byte[] key,Binding binding,boolean initialize)throws Exception{
        String type=binding.type();
        if(!type.equals(type(env)))throw new IllegalArgumentException("Control store configuration does not match binding");
        if(type.equals("SQLITE")){
            Path local=directory.resolve("control-"+binding.owner()+".db");
            if(initialize&&Files.exists(local,LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Migration destination already exists");
            return new ControlStore(local,key,binding.owner(),initialize);
        }
        boolean insecure="true".equalsIgnoreCase(env.get("CDC_CONTROL_ALLOW_INSECURE"));
        try{
            if(type.equals("MYSQL")){
                String mode=env.getOrDefault("CDC_CONTROL_MYSQL_SSL_MODE","VERIFY_IDENTITY").toUpperCase(Locale.ROOT);
                if(!mode.equals("VERIFY_IDENTITY")&&!insecure)throw new IllegalArgumentException("Non-verifying control MySQL TLS requires explicit opt-in");
                return ControlStore.mysql(CdcApplication.required(env,"CDC_CONTROL_MYSQL_HOST"),Integer.parseInt(env.getOrDefault("CDC_CONTROL_MYSQL_PORT","3306")),CdcApplication.required(env,"CDC_CONTROL_MYSQL_DATABASE"),CdcApplication.required(env,"CDC_CONTROL_MYSQL_USER"),env.getOrDefault("CDC_CONTROL_MYSQL_PASSWORD",""),mode,key,binding.owner(),initialize);
            }
            String tlsValue=env.getOrDefault("CDC_CONTROL_MONGO_TLS","true");
            if(!Set.of("true","false").contains(tlsValue.toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("Invalid control MongoDB TLS option");
            boolean tls=Boolean.parseBoolean(tlsValue);
            if(!tls&&!insecure)throw new IllegalArgumentException("Unencrypted control MongoDB requires explicit opt-in");
            return new MongoControlStore(CdcApplication.required(env,"CDC_CONTROL_MONGO_URI"),CdcApplication.required(env,"CDC_CONTROL_MONGO_DATABASE"),tls,key,binding.owner(),initialize);
        }catch(Exception failure){
            throw new IllegalStateException("External control store initialization failed; check credentials, TLS, database ownership, deployment key and retained metadata");
        }
    }
}
