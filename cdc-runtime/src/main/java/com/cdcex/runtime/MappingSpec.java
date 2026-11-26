package com.cdcex.runtime;

import com.cdcex.core.*;
import java.math.*;
import java.time.Instant;
import java.util.*;

/** Deterministic, explicit field projection. Mutable value filters and scripts are deliberately rejected. */
public record MappingSpec(Map<String,String> rename, Map<String,String> conversions, List<String> exclude) {
    public MappingSpec {
        rename=rename==null?Map.of():Map.copyOf(rename);
        conversions=conversions==null?Map.of():Map.copyOf(conversions);
        exclude=exclude==null?List.of():List.copyOf(exclude);
        if(new HashSet<>(rename.values()).size()!=rename.size())throw new IllegalArgumentException("Two fields cannot map to the same target field");
        rename.forEach((a,b)->{field(a);field(b);});conversions.keySet().forEach(MappingSpec::field);exclude.forEach(MappingSpec::field);
        conversions.values().forEach(v->{if(!Set.of("STRING","LONG","INTEGER","DECIMAL","BOOLEAN","JSON","BSON_INT64","BSON_DECIMAL","BSON_OBJECT_ID").contains(v))throw new IllegalArgumentException("Unknown conversion");});
    }
    public static MappingSpec identity(){return new MappingSpec(Map.of(),Map.of(),List.of());}
    private static void field(String v){if(v==null||v.isBlank()||v.length()>128||v.indexOf('\0')>=0)throw new IllegalArgumentException("Invalid mapping field");}
    public String targetField(String source){return rename.getOrDefault(source,source);}
    public Map<String,Object> row(Map<String,Object> row,boolean key){
        if(row==null)return null;Map<String,Object> out=new LinkedHashMap<>();
        row.forEach((name,value)->{
            if(exclude.contains(name)){if(key)throw new IllegalArgumentException("A key field cannot be excluded");return;}
            String target=targetField(name);
            if(out.containsKey(target))throw new IllegalArgumentException("Mapping creates duplicate fields");
            out.put(target,convert(value,conversions.get(target)));
        });return out;
    }
    public ChangeEvent apply(ChangeEvent event,Endpoint sink){
        return new ChangeEvent(event.id(),sink.option("database",event.database()),sink.option("table",event.table()),event.operation(),event.position(),
            row(event.key(),true),row(event.before(),false),row(event.after(),false),event.sourceTimeMillis());
    }
    public void validate(ConnectorProvider.Inspection source,ConnectorProvider.Inspection sink){
        if(!source.keyFields().isEmpty()&&!sink.keyFields().isEmpty()){
            for(String key:source.keyFields())if(exclude.contains(key))throw new IllegalArgumentException("A key field cannot be excluded");
            if(!new HashSet<>(source.keyFields().stream().map(this::targetField).toList()).equals(new HashSet<>(sink.keyFields())))throw new IllegalArgumentException("Source and target keys require an explicit compatible mapping");
        }
        if(!source.fields().containsKey("*")&&!sink.fields().containsKey("*")&&!source.fields().isEmpty()&&!sink.fields().isEmpty()){
            Set<String> fields=new HashSet<>();
            for(String f:source.fields().keySet())if(!exclude.contains(f)&&!fields.add(targetField(f)))throw new IllegalArgumentException("Mapping creates duplicate fields");
            if(!fields.equals(sink.fields().keySet()))throw new IllegalArgumentException("Mapped fields do not match target schema");
            for(var entry:source.fields().entrySet()){
                String target=targetField(entry.getKey());
                if(exclude.contains(entry.getKey())||conversions.containsKey(target))continue;
                String a=family(entry.getValue()),b=family(sink.fields().get(target));
                if(!a.equals(b))throw new IllegalArgumentException("Field types need an explicit conversion: "+target);
                if(a.equals("decimal")){
                    int[] from=decimalShape(entry.getValue()),to=decimalShape(sink.fields().get(target));
                    if(from==null||to==null||to[1]<from[1]||to[0]-to[1]<from[0]-from[1])throw new IllegalArgumentException("Decimal precision or scale needs an explicit conversion: "+target);
                }
                if(a.equals("text")){
                    int from=textWidth(entry.getValue()),to=textWidth(sink.fields().get(target));
                    if(to<from)throw new IllegalArgumentException("Target text width needs an explicit conversion: "+target);
                }
            }
        }
    }
    private static String family(String value){
        String v=value.toLowerCase(Locale.ROOT).replaceAll("\\s+(not\\s+)?null$","");
        if(v.matches(".*(tinyint|smallint|mediumint|bigint|integer|int2|int4|int8).*" )||v.equals("int"))return "integer";
        if(v.startsWith("decimal")||v.startsWith("numeric"))return "decimal";
        if(v.contains("char")||v.equals("text")||v.equals("string"))return "text";
        return v;
    }
    private static int[] decimalShape(String value){var m=java.util.regex.Pattern.compile("(?i)(?:numeric|decimal)\\((\\d+),\\s*(\\d+)\\)").matcher(value);return m.find()?new int[]{Integer.parseInt(m.group(1)),Integer.parseInt(m.group(2))}:null;}
    private static int textWidth(String value){var m=java.util.regex.Pattern.compile("(?i)(?:var)?char(?:acter varying)?\\((\\d+)\\)").matcher(value);return m.find()?Integer.parseInt(m.group(1)):Integer.MAX_VALUE;}
    public static Object convert(Object input,String conversion){
        if(input==null||conversion==null)return input;
        if(conversion.equals("JSON"))return EventCodec.json(input);
        Object value=bsonScalar(input);
        return switch(conversion){
            case "STRING" -> {if(value instanceof Map||value instanceof List)throw new IllegalArgumentException("Object-to-text requires JSON conversion");yield value.toString();}
            case "LONG" -> number(value).toBigIntegerExact().longValueExact();
            case "INTEGER" -> number(value).toBigIntegerExact().intValueExact();
            case "DECIMAL" -> number(value);
            case "BOOLEAN" -> {if(value instanceof Boolean)yield value;if("true".equals(value)||"false".equals(value))yield Boolean.parseBoolean((String)value);throw new IllegalArgumentException("Boolean conversion accepts true or false only");}
            case "BSON_INT64" -> Map.of("$numberLong",Long.toString(number(value).toBigIntegerExact().longValueExact()));
            case "BSON_DECIMAL" -> Map.of("$numberDecimal",number(value).toPlainString());
            case "BSON_OBJECT_ID" -> {String s=value.toString();if(!s.matches("[0-9a-fA-F]{24}"))throw new IllegalArgumentException("ObjectId requires 24 hexadecimal characters");yield Map.of("$oid",s.toLowerCase(Locale.ROOT));}
            default -> throw new IllegalArgumentException("Unknown conversion");
        };
    }
    private static BigDecimal number(Object value){
        if(value instanceof Boolean||value instanceof Map||value instanceof List)throw new IllegalArgumentException("Numeric conversion requires a scalar");
        return new BigDecimal(value.toString());
    }
    public static Object bsonScalar(Object value){
        if(value instanceof Map<?,?> m&&m.size()==1){
            if(m.containsKey("$numberInt"))return new BigInteger(m.get("$numberInt").toString());
            if(m.containsKey("$numberLong"))return new BigInteger(m.get("$numberLong").toString());
            if(m.containsKey("$numberDecimal"))return new BigDecimal(m.get("$numberDecimal").toString());
            if(m.containsKey("$oid"))return m.get("$oid").toString();
            if(m.containsKey("$date")){Object date=bsonScalar(m.get("$date"));return date instanceof Number n?Instant.ofEpochMilli(number(n).toBigIntegerExact().longValueExact()).toString():date;}
        }return value;
    }
}
