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
        if(value==null)return null;
        Map<String,Object> copy=new LinkedHashMap<>();
        value.forEach((k,v)->copy.put(Objects.requireNonNull(k),freeze(v)));
        return Collections.unmodifiableMap(copy);
    }
    private static Object freeze(Object value) {
        if(value instanceof Map<?,?> map){
            Map<String,Object> copy=new LinkedHashMap<>();
            map.forEach((k,v)->{if(!(k instanceof String s))throw new IllegalArgumentException("Document keys must be strings");copy.put(s,freeze(v));});
            return Collections.unmodifiableMap(copy);
        }
        if(value instanceof List<?> list)return Collections.unmodifiableList(list.stream().map(ChangeEvent::freeze).toList());
        if(value==null||value instanceof String||value instanceof Number||value instanceof Boolean)return value;
        throw new IllegalArgumentException("Unsupported event value; use an explicit typed JSON representation");
    }
}
