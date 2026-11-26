package com.cdcex.core;

import java.util.*;

/** Public diagnostics are selected constants. Driver messages and row payloads never escape this boundary. */
public record Failure(String code,String message,String location) {
    public static Failure describe(Throwable error){
        String code="OPERATION_FAILED",message="Check the connection prerequisites and task recovery runbook.",location="unavailable";
        Set<Throwable> seen=Collections.newSetFromMap(new IdentityHashMap<>());
        for(Throwable cause=error;cause!=null&&seen.add(cause);cause=cause.getCause()){
            StackTraceElement own=Arrays.stream(cause.getStackTrace()).filter(f->f.getClassName().startsWith("com.cdcex.")).findFirst().orElse(null);
            if(own!=null)location=own.getClassName().substring("com.cdcex.".length())+"."+own.getMethodName()+":"+own.getLineNumber();
            // Only classify our own exception messages; emit no part of the original text.
            if(cause.getStackTrace().length==0||!cause.getStackTrace()[0].getClassName().startsWith("com.cdcex."))continue;
            String text=Objects.toString(cause.getMessage(),"").toLowerCase(Locale.ROOT);
            if(text.contains("schema")||text.contains("ddl")){code="SCHEMA_REVIEW_REQUIRED";message="The schema is incompatible or changed. Restore the expected schema or create a reviewed new generation.";}
            else if(text.contains("configuration changed")||text.contains("generation")){code="GENERATION_REVIEW_REQUIRED";message="The saved task generation cannot be reused with this configuration. Review its mapping, identity and snapshot state.";}
            else if(text.contains("empty")&&text.contains("target")||text.contains("empty destination")){code="DESTINATION_NOT_EMPTY";message="A fresh snapshot or versioned task requires an empty destination.";}
            else if(text.contains("checkpoint")||text.contains("recovery state")||text.contains("offset")||text.contains("slot")||text.contains("expired")||text.contains("history")||text.contains("snapshot")){code="RECOVERY_STATE_REVIEW_REQUIRED";message="Capture state or retained source history is unavailable or incompatible. Repair it or reconcile a new generation; no offsets were reset.";}
            else if(text.contains("capacity")||text.contains("byte limit")){code="CAPACITY_REACHED";message="The configured resource budget was reached. Check free space, queue progress, and the task limits before resuming.";}
            else if(text.contains("collision")){code="EVENT_IDENTITY_COLLISION";message="The same source identity produced different content. Capture stopped for investigation.";}
            else if(text.contains("cycle")||text.contains("same resource")){code="REPLICATION_CYCLE";message="The task would create a replication loop. Choose a different destination.";}
            else if(text.contains("own")||text.contains("already running")){code="RESOURCE_OWNED";message="Another task or process owns this resource. Stop that owner before retrying.";}
            else if(text.contains("conversion")||text.contains("precision")||text.contains("range")||text.contains("mapping")||text.contains("key")){code="MAPPING_REVIEW_REQUIRED";message="The value or key cannot be represented safely by the selected mapping. Review field types and key rules.";}
        }
        return new Failure(code,message,location);
    }
}
