package com.cdcex.core;

import java.util.List;
public interface SourceConnector extends AutoCloseable {
    @FunctionalInterface interface DurableReceiver { void accept(List<ChangeEvent> batch) throws Exception; }
    /** Blocks until stopped or failed. Acknowledge native offsets only after receiver returns successfully. */
    void run(DurableReceiver receiver) throws Exception;
    /** Finite snapshot readers may finish; the runtime then drains the durable queue. */
    default boolean finite() { return false; }
    @Override void close() throws Exception;
}
