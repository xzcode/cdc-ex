package com.cdcex.core;

import java.util.*;
public record ChangeEvent(String id, String database, String table, Operation operation,
                          String position, Map<String,Object> key, Map<String,Object> before,
                          Map<String,Object> after, long sourceTimeMillis) {
    public enum Operation { SNAPSHOT, INSERT, UPDATE, DELETE, TRUNCATE, UNSUPPORTED }
    public ChangeEvent {
        if (id == null || id.isBlank() || database == null || table == null || operation == null || position == null)
            throw new IllegalArgumentException("Missing event identity");
        key = immutable(key); before = immutable(before); after = immutable(after);
    }
    private static Map<String,Object> immutable(Map<String,Object> value) {
        return value == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(value));
    }
}
