package com.cdcex.storage;
import java.util.List;
/** Management metadata only; capture journals and checkpoints remain local. */
public interface ControlRepository extends AutoCloseable {
    void create(String kind,String id,String name,String json)throws Exception;
    void replace(String kind,String id,String name,String json)throws Exception;
    ControlStore.Record get(String kind,String id)throws Exception;
    List<ControlStore.Record> list(String kind)throws Exception;
    void state(String id,String state,String error)throws Exception;
    void identities(String id,String source,String sink)throws Exception;
    void recoverInterrupted()throws Exception;
    void audit(String action,String resource,String result)throws Exception;
    List<ControlStore.Audit> audits()throws Exception;
    @Override void close()throws Exception;
}
