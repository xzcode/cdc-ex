package com.cdcex.core;

import java.util.List;
public interface SinkConnector extends AutoCloseable {
    /** Returns only when the entire ordered batch is durably committed. Throws on ambiguous commit. */
    void apply(List<ChangeEvent> events) throws Exception;
    @Override default void close() throws Exception {}
}
