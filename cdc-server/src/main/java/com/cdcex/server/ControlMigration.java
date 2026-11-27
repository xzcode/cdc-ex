package com.cdcex.server;

import com.cdcex.storage.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Offline, copy/verify/switch. A failed copy cannot activate its destination. */
final class ControlMigration {
    private static final Set<String> INACTIVE=Set.of("DRAFT","PAUSED","STOPPED","BLOCKED","FAILED");
    static void run(Map<String,String> env)throws Exception{
        if(!"true".equalsIgnoreCase(env.get("CDC_MIGRATE_CONFIRM")))throw new IllegalArgumentException("Explicit offline control migration confirmation required");
        Path directory=Path.of(CdcApplication.required(env,"CDC_DATA_DIR")).toAbsolutePath().normalize();
        if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS))throw new IllegalArgumentException("Existing data directory required");
        Map<String,String> target=new HashMap<>();
        env.forEach((k,v)->{if(k.startsWith("CDC_MIGRATE_"))target.put("CDC_CONTROL_"+k.substring("CDC_MIGRATE_".length()),v);});
        CdcApplication.required(target,"CDC_CONTROL_DB_TYPE");
        String from=ControlBinding.type(env),to=ControlBinding.type(target);
        if(from.equals(to))throw new IllegalArgumentException("Choose a different management backend");
        byte[] key=ServerConfiguration.masterKey(env);
        try(var owner=new ServerConfiguration.Ownership(directory);var locks=new TaskLocks(directory)){
            ControlRepository.Snapshot snapshot;
            try(var source=ControlBinding.openExisting(directory,env,key)){snapshot=source.snapshot();}
            Set<String> ids=new HashSet<>();
            for(var r:snapshot.records()){
                if(!Set.of("connection","task").contains(r.kind()))throw new IllegalStateException("Unsupported control record kind");
                if(r.kind().equals("task")){
                    if(!INACTIVE.contains(r.state()))throw new IllegalStateException("Pause and stop all tasks before migration");
                    ids.add(r.id());
                }
            }
            if(!ids.containsAll(locks.ids))throw new IllegalStateException("Unregistered local task state; review before migration");
            var binding=new ControlBinding.Binding(to,UUID.randomUUID().toString(),true);
            // Do not write a pending binding or touch the old repository. Failed destinations
            // retain their unique owner marker and cannot be accidentally opened as a fresh store.
            try(var destination=ControlBinding.openBound(directory,target,key,binding,true)){
                destination.importSnapshot(snapshot);
                if(!snapshot.equals(destination.snapshot()))throw new IllegalStateException("Control migration verification failed");
            }
            // Reopen to verify durable data and encrypted owner check before activating.
            try(var destination=ControlBinding.openBound(directory,target,key,binding,false)){
                if(!snapshot.equals(destination.snapshot()))throw new IllegalStateException("Control migration durable verification failed");
            }
            ControlBinding.save(directory.resolve(ControlBinding.FILE),binding);
            System.out.println("Control migration complete: "+from+" -> "+to+"; "+snapshot.records().size()+" records, "+snapshot.audit().size()+" audit entries. Local task state unchanged. Start with the destination configuration.");
        }finally{Arrays.fill(key,(byte)0);}
    }
    /** Also reject independently running task workers sharing this directory. */
    private static final class TaskLocks implements AutoCloseable{
        final Set<String> ids=new HashSet<>();
        final List<FileChannel> channels=new ArrayList<>();
        final List<FileLock> locks=new ArrayList<>();
        TaskLocks(Path directory)throws Exception{
            Path tasks=directory.resolve("tasks");
            if(!Files.exists(tasks,LinkOption.NOFOLLOW_LINKS))return;
            if(!Files.isDirectory(tasks,LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Invalid task state directory");
            try(var paths=Files.list(tasks)){
                for(Path path:paths.toList()){
                    if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Invalid task directory");
                    String id=path.getFileName().toString();ControlStore.id(id);ids.add(id);
                    Path lockFile=path.resolve("task.lock");
                    if(Files.isSymbolicLink(lockFile))throw new IllegalStateException("Invalid task lock");
                    var c=FileChannel.open(lockFile,StandardOpenOption.CREATE,StandardOpenOption.WRITE);channels.add(c);
                    var lock=c.tryLock();if(lock==null)throw new IllegalStateException("Task directory is already owned");locks.add(lock);
                }
            }catch(Exception e){close();throw e;}
        }
        public void close(){
            for(var lock:locks)try{lock.close();}catch(Exception ignored){}
            for(var channel:channels)try{channel.close();}catch(Exception ignored){}
        }
    }
}
