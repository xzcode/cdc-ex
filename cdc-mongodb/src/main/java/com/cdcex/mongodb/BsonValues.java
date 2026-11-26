package com.cdcex.mongodb;

import com.cdcex.core.EventCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.*;
import org.bson.json.*;
import org.bson.types.Decimal128;
import java.math.*;
import java.util.*;

/** BSON crosses the journal as canonical Extended JSON, never relaxed JSON. */
public final class BsonValues {
    static final JsonWriterSettings CANONICAL=JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final Set<String> TAGS=Set.of("$numberInt","$numberLong","$numberDouble","$numberDecimal",
        "$oid","$binary","$date","$timestamp","$regularExpression","$minKey","$maxKey");
    private BsonValues() {}

    public static Map<String,Object> map(BsonDocument document) {
        if(document==null) return null;
        validate(document);
        try{return JSON.readValue(document.toJson(CANONICAL),new TypeReference<LinkedHashMap<String,Object>>(){});}
        catch(Exception e){throw new IllegalArgumentException("Cannot encode BSON document");}
    }
    public static BsonDocument document(Map<String,Object> map) {
        if(map==null) return null;
        BsonDocument result=new BsonDocument();
        for(var entry:map.entrySet()) {
            field(entry.getKey());
            result.put(entry.getKey(),value(entry.getValue()));
        }
        validate(result);
        return result;
    }
    private static BsonValue value(Object value) {
        if(value==null) return BsonNull.VALUE;
        if(value instanceof String v) return new BsonString(v);
        if(value instanceof Boolean v) return BsonBoolean.valueOf(v);
        if(value instanceof Byte || value instanceof Short || value instanceof Integer) return new BsonInt32(((Number)value).intValue());
        if(value instanceof Long v) return new BsonInt64(v);
        if(value instanceof BigInteger v) return new BsonInt64(v.longValueExact());
        if(value instanceof BigDecimal v) return new BsonDecimal128(new Decimal128(v));
        if(value instanceof Float || value instanceof Double) {
            double v=((Number)value).doubleValue();
            if(!Double.isFinite(v))throw new IllegalArgumentException("Non-finite numbers require explicit BSON encoding");
            return new BsonDouble(v);
        }
        if(value instanceof List<?> list) {
            BsonArray result=new BsonArray();
            list.forEach(v->result.add(value(v)));return result;
        }
        if(value instanceof Map<?,?> map) {
            if(map.size()==1 && TAGS.contains(map.keySet().iterator().next())) {
                try {return BsonDocument.parse("{\"value\":"+EventCodec.json(map)+"}").get("value");}
                catch(Exception e){throw new IllegalArgumentException("Invalid canonical BSON value");}
            }
            Map<String,Object> fields=new LinkedHashMap<>();
            map.forEach((k,v)->{if(!(k instanceof String s))throw new IllegalArgumentException("BSON field name must be a string");fields.put(s,v);});
            return document(fields);
        }
        throw new IllegalArgumentException("Unsupported BSON mapping value");
    }
    private static void field(String field) {
        if(field==null || field.isEmpty() || field.indexOf('\0')>=0 || field.startsWith("$"))
            throw new IllegalArgumentException("Dollar-prefixed or empty BSON field names are outside the canonical document contract");
    }
    private static void validate(BsonValue value) {
        switch(value.getBsonType()) {
            case DOCUMENT -> value.asDocument().forEach((k,v)->{field(k);validate(v);});
            case ARRAY -> value.asArray().forEach(BsonValues::validate);
            case STRING,INT32,INT64,DOUBLE,DECIMAL128,BOOLEAN,NULL,OBJECT_ID,BINARY,DATE_TIME,TIMESTAMP,REGULAR_EXPRESSION,MIN_KEY,MAX_KEY -> {}
            default -> throw new IllegalArgumentException("Unsupported BSON type: "+value.getBsonType());
        }
    }
    static Map<String,Object> key(BsonDocument document) {
        if(document==null || !document.containsKey("_id") || document.get("_id").isNull())
            throw new IllegalArgumentException("MongoDB replication requires a non-null _id");
        if(document.get("_id").isDocument() || document.get("_id").isArray())
            throw new IllegalArgumentException("MongoDB V1 replication requires a scalar _id");
        return map(new BsonDocument("_id",document.get("_id")));
    }
}
