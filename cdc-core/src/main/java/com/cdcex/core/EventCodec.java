package com.cdcex.core;

import com.fasterxml.jackson.databind.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
public final class EventCodec {
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
    private static final ObjectMapper CANONICAL=JSON.copy().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    private EventCodec() {}
    public static String json(Object value) {
        try { return JSON.writeValueAsString(value); } catch(Exception e) { throw new IllegalArgumentException("Cannot encode event", e); }
    }
    public static ChangeEvent decode(String value) {
        try { return JSON.readValue(value, ChangeEvent.class); } catch(Exception e) { throw new IllegalArgumentException("Cannot decode event", e); }
    }
    public static String canonicalJson(Object value) {
        try{return CANONICAL.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("Cannot encode canonical value",e);}
    }
    public static String digest(Object value) {
        return hash(canonicalJson(value));
    }
    /** Unlike identity hashing, payload hashing preserves order inside BSON documents. */
    public static String payloadDigest(Object value){return hash(json(value));}
    private static String hash(String value){
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch(Exception e) { throw new IllegalStateException("Cannot hash event", e); }
    }
}
