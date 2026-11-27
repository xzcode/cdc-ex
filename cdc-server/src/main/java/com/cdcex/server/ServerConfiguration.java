package com.cdcex.server;

import com.cdcex.storage.ControlRepository;
import org.springframework.context.annotation.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

@Configuration
public class ServerConfiguration {
    public static final class Ownership implements AutoCloseable{
        final Path directory;final FileChannel channel;final FileLock lock;
        Ownership(Path directory)throws Exception{
            this.directory=directory;TaskRunner.privateDirectory(directory);channel=FileChannel.open(directory.resolve("server.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
            FileLock candidate;try{candidate=channel.tryLock();}catch(Exception e){channel.close();throw e;}
            if(candidate==null){channel.close();throw new IllegalStateException("Another server owns this data directory");}lock=candidate;
        }
        public void close()throws Exception{try{lock.close();}finally{channel.close();}}
    }
    @Bean(destroyMethod="close") Ownership ownership()throws Exception{return new Ownership(Path.of(CdcApplication.required(System.getenv(),"CDC_DATA_DIR")).toAbsolutePath().normalize());}
    @Bean ConnectorRegistry connectorRegistry(){return new ConnectorRegistry();}
    @Bean(destroyMethod="close") ControlRepository controlStore(Ownership owner)throws Exception{
        Map<String,String> env=System.getenv();String value;
        if(env.containsKey("CDC_MASTER_KEY_FILE")){
            Path file=Path.of(env.get("CDC_MASTER_KEY_FILE"));if(Files.size(file)>4096)throw new IllegalArgumentException("Invalid master key file");
            if(Files.getFileStore(file).supportsFileAttributeView("posix")){
                var perms=Files.getPosixFilePermissions(file);if(perms.stream().anyMatch(p->p.name().startsWith("GROUP_")||p.name().startsWith("OTHERS_")))throw new IllegalArgumentException("Master key file must be readable only by its owner");
            }value=Files.readString(file).trim();
        }else value=CdcApplication.required(env,"CDC_MASTER_KEY");
        byte[] key=Base64.getDecoder().decode(value);try{return ControlBinding.open(owner.directory,env,key);}finally{Arrays.fill(key,(byte)0);}
    }
    @Bean(destroyMethod="close") TaskManager taskManager(ControlRepository store,ConnectorRegistry registry,Ownership owner)throws Exception{return new TaskManager(store,registry,owner.directory);}
    @Bean ApiAuthFilter apiAuthFilter(){return new ApiAuthFilter(CdcApplication.required(System.getenv(),"CDC_ADMIN_TOKEN"));}
}
