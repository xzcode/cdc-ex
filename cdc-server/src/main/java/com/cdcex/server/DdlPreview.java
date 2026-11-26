package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.runtime.*;
import java.util.*;

final class DdlPreview {
    static Map<String,Object> preview(TaskSpec spec,ConnectorProvider.Inspection source){
        String type=spec.sink().type();if(!Set.of("MYSQL","MARIADB","POSTGRESQL").contains(type)||source.fields().containsKey("*"))return Map.of("sql","","executable",false,"message","Use the explicit connector schema contract; this source has no fixed SQL row schema.");
        boolean postgres=type.equals("POSTGRESQL");String namespace=spec.sink().option(postgres?"schema":"database",postgres?"public":"");String table=spec.sink().require("table");
        List<String> definitions=new ArrayList<>();for(var field:source.fields().entrySet()){
            if(spec.mapping().exclude().contains(field.getKey()))continue;String target=spec.mapping().targetField(field.getKey());String conversion=spec.mapping().conversions().get(target);String value=field.getValue().toLowerCase(Locale.ROOT);
            String targetType;
            if(conversion!=null)targetType=switch(conversion){case "LONG"->"BIGINT";case "INTEGER"->"INTEGER";case "DECIMAL"->"DECIMAL(38,10)";case "BOOLEAN"->postgres?"BOOLEAN":"TINYINT";case "STRING","JSON"->"TEXT";default->throw new IllegalArgumentException("Conversion cannot be represented by a SQL preview");};
            else if(value.matches(".*(int|long).*"))targetType="BIGINT";
            else if(value.startsWith("numeric")||value.startsWith("decimal")){var m=java.util.regex.Pattern.compile("(?:numeric|decimal)\\((\\d+),(\\d+)\\)").matcher(value);targetType=m.find()?"DECIMAL("+m.group(1)+","+m.group(2)+")":"DECIMAL(38,10)";}
            else if(value.contains("char")||value.contains("text"))targetType="VARCHAR(4096)";
            else if(value.startsWith("boolean"))targetType=postgres?"BOOLEAN":"TINYINT";
            else throw new IllegalArgumentException("Source type requires an explicit conversion before DDL preview");
            definitions.add("  "+quote(target,postgres)+" "+targetType+(source.keyFields().contains(field.getKey())?" NOT NULL":""));
        }
        definitions.add("  PRIMARY KEY ("+String.join(", ",source.keyFields().stream().map(spec.mapping()::targetField).map(k->quote(k,postgres)).toList())+")");
        String sql="CREATE TABLE "+quote(namespace,postgres)+"."+quote(table,postgres)+" (\n"+String.join(",\n",definitions)+"\n)"+(postgres?";":" ENGINE=InnoDB;");
        return Map.of("sql",sql,"executable",false,"message","Review widths, precision, collation and nullability. CDC EX never executes this preview.");
    }
    private static String quote(String name,boolean postgres){if(!name.matches("[A-Za-z_][A-Za-z0-9_]{0,62}"))throw new IllegalArgumentException("Unsupported identifier");return postgres?"\""+name+"\"":"`"+name+"`";}
}
