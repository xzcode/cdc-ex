package com.cdcex.core;

import java.util.*;

/** Runtime-only connection settings. Persist only through the encrypted connection store. */
public record Endpoint(String type, Map<String,String> options) {
    public Endpoint {
        type = Objects.requireNonNull(type).toUpperCase(Locale.ROOT);
        options = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(options)));
        if(options.size()>64)throw new IllegalArgumentException("Too many connection options");
        options.forEach((k,v)->{if(k==null||!k.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}")||v==null||v.length()>8192)throw new IllegalArgumentException("Invalid connection option");});
    }
    public String require(String name) {
        String value=options.get(name);
        if(value==null || value.isBlank()) throw new IllegalArgumentException("Missing connection option: "+name);
        return value;
    }
    public String option(String name,String fallback) { return options.getOrDefault(name,fallback); }
    public int integer(String name,int fallback) { return Integer.parseInt(option(name,Integer.toString(fallback))); }
    public boolean flag(String name,boolean fallback) {
        String v=option(name,Boolean.toString(fallback));
        if(!Set.of("true","false").contains(v)) throw new IllegalArgumentException("Invalid boolean connection option: "+name);
        return Boolean.parseBoolean(v);
    }
    @Override public String toString() { return "Endpoint[type="+type+", options=redacted]"; }
}
