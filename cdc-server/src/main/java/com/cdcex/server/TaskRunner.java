package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.runtime.*;
import com.cdcex.storage.SqliteJournal;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

public final class TaskRunner implements AutoCloseable {
    public record Validation(ConnectorProvider.Inspection source,ConnectorProvider.Inspection sink){}
    private final FileChannel lockFile;
    private final FileLock lock;
    private final SqliteJournal journal;
    private final TaskRuntime runtime;
    private boolean ran;
    private TaskRunner(FileChannel lockFile,FileLock lock,SqliteJournal journal,TaskRuntime runtime){this.lockFile=lockFile;this.lock=lock;this.journal=journal;this.runtime=runtime;}
    public TaskRuntime runtime(){return runtime;}
    public SqliteJournal journal(){return journal;}
    public static void privateDirectory(Path dir)throws Exception{
        Files.createDirectories(dir);
        if(Files.isSymbolicLink(dir))throw new IllegalArgumentException("Data directory cannot be a symbolic link");
        if(Files.getFileStore(dir).supportsFileAttributeView("posix"))Files.setPosixFilePermissions(dir,PosixFilePermissions.fromString("rwx------"));
    }
    public static Validation validate(TaskSpec spec,ConnectorRegistry registry,boolean fresh)throws Exception{
        var source=registry.get(spec.source().type());var sink=registry.get(spec.sink().type());
        Object modes=source.capabilities().get("sourceModes");
        if(!(modes instanceof List<?> list)||(!list.contains(spec.snapshotMode())&&!(spec.source().type().equals("KAFKA")&&spec.snapshotMode().equals("initial"))))throw new IllegalArgumentException("Unsupported source capture mode");
        var a=source.inspect(spec.source(),ConnectorProvider.Role.SOURCE);var b=sink.inspect(spec.sink(),ConnectorProvider.Role.SINK);
        if(a.identity().equals(b.identity()))throw new IllegalArgumentException("Source and target are the same resource");
        spec.mapping().validate(a,b);
        if(fresh&&Set.of("initial","snapshot","earliest").contains(spec.snapshotMode())&&Boolean.FALSE.equals(b.details().get("empty")))
            throw new IllegalStateException("A fresh initial snapshot requires an empty destination");
        return new Validation(a,b);
    }
    public static TaskRunner open(TaskSpec spec,Path directory,ConnectorRegistry registry)throws Exception{
        privateDirectory(directory);FileChannel lockFile=FileChannel.open(directory.resolve("task.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock lock=null;SqliteJournal journal=null;SinkConnector sink=null;SourceConnector source=null;
        try{
            lock=lockFile.tryLock();if(lock==null)throw new IllegalStateException("Task directory is already owned");
            boolean fresh=!Files.exists(directory.resolve("journal.db"));
            if(fresh){try(var files=Files.list(directory)){if(files.anyMatch(p->!p.getFileName().toString().equals("task.lock")))throw new IllegalStateException("Orphaned task state; explicit recovery is required");}}
            Validation validation=validate(spec,registry,fresh);
            journal=new SqliteJournal(directory.resolve("journal.db"),spec.journalBytes(),spec.journalEvents());
            if(!fresh&&journal.metadata("fingerprint")==null)throw new IllegalStateException("Uninitialized journal; explicit recovery is required");
            journal.initialize(spec.fingerprint(validation.source(),validation.sink()));
            source=registry.get(spec.source().type()).openSource(spec.source(),new ConnectorProvider.SourceContext(directory,spec.id(),spec.snapshotMode(),fresh));
            sink=registry.get(spec.sink().type()).openSink(spec.sink(),new ConnectorProvider.SinkContext(directory,spec.id(),fresh));
            return new TaskRunner(lockFile,lock,journal,new TaskRuntime(source,new MappedSink(sink,spec.mapping(),spec.sink()),journal,spec.batchSize(),spec.maxFailures()));
        }catch(Exception e){
            if(source!=null)try{source.close();}catch(Exception ignored){}
            if(sink!=null)try{sink.close();}catch(Exception ignored){}
            if(journal!=null)try{journal.close();}catch(Exception ignored){}
            if(lock!=null)lock.close();lockFile.close();throw e;
        }
    }
    public void run()throws Exception{ran=true;runtime.run();}
    @Override public void close()throws Exception{
        runtime.requestStop();
        try{journal.close();}finally{try{lock.close();}finally{lockFile.close();}}
    }
}
