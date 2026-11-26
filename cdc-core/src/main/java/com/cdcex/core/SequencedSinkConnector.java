package com.cdcex.core;

import java.util.List;

/** For versioned targets: the durable task-local journal sequence survives replay. */
public interface SequencedSinkConnector extends SinkConnector {
    record Delivery(long sequence,ChangeEvent event){}
    void applySequenced(List<Delivery> events) throws Exception;
    @Override default void apply(List<ChangeEvent> events)throws Exception{
        throw new IllegalStateException("A durable journal sequence is required for this target");
    }
}
