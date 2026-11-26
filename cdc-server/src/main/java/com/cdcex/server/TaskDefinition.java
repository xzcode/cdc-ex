package com.cdcex.server;

import com.cdcex.runtime.*;
import com.cdcex.core.*;
import com.cdcex.storage.ControlStore;

public record TaskDefinition(String id,String name,String sourceConnectionId,String sinkConnectionId,String snapshotMode,
                             MappingSpec mapping,long journalBytes,long journalEvents,int batchSize,int maxFailures){
    public TaskDefinition{
        ControlStore.id(id);ControlStore.id(sourceConnectionId);ControlStore.id(sinkConnectionId);
        if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid task name");
        mapping=mapping==null?MappingSpec.identity():mapping;snapshotMode=snapshotMode==null?"initial":snapshotMode;
    }
    TaskSpec resolve(Endpoint source,Endpoint sink){return new TaskSpec(id,source,sink,snapshotMode,mapping,journalBytes,journalEvents,batchSize,maxFailures);}
}
