package com.cdcex.mysql;

import com.cdcex.core.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import static com.cdcex.mysql.MysqlTaskConfig.quote;
public final class MysqlSink implements SinkConnector {
    private final MysqlTaskConfig config;
    private final MysqlSchema schema;
    private final List<String> columns,keys;
    private final Set<String> columnSet,keySet;
    private final String upsert,delete;
    public MysqlSink(MysqlTaskConfig config,MysqlSchema schema){
        this.config=config;this.schema=schema;columns=schema.columns().stream().map(MysqlSchema.Column::name).toList();keys=schema.keys();
        columnSet=Set.copyOf(columns);keySet=Set.copyOf(keys);
        String cols=columns.stream().map(MysqlTaskConfig::quote).collect(Collectors.joining(","));
        String params=String.join(",",Collections.nCopies(columns.size(),"?"));
        String updates=columns.stream().map(c->quote(c)+"=VALUES("+quote(c)+")").collect(Collectors.joining(","));
        upsert="INSERT INTO "+config.target().qualified()+" ("+cols+") VALUES ("+params+") ON DUPLICATE KEY UPDATE "+updates;
        delete="DELETE FROM "+config.target().qualified()+" WHERE "+keys.stream().map(k->quote(k)+"=?").collect(Collectors.joining(" AND "));
    }
    private void validateKey(Map<String,Object> k){if(k==null||!keySet.equals(k.keySet())||k.values().stream().anyMatch(Objects::isNull))throw new IllegalStateException("Missing or incompatible primary key");validateValues(k,false);}
    private static void bind(PreparedStatement s,List<String> cols,Map<String,Object> row) throws SQLException{
        for(int i=0;i<cols.size();i++)s.setObject(i+1,row.get(cols.get(i)));
    }
    private void validateRow(Map<String,Object> row){validateValues(row,true);}
    private void validateValues(Map<String,Object> row,boolean complete){
        for(var column:schema.columns()){
            if(!complete&&!row.containsKey(column.name()))continue;
            Object value=row.get(column.name());String type=column.type();
            if(value==null){if(!column.nullable())throw new IllegalArgumentException("Null supplied for required target column");continue;}
            if(type.startsWith("decimal(")){
                if(!(value instanceof Number))throw new IllegalArgumentException("Decimal mapping requires a numeric value");
                String[] dimensions=type.substring(8,type.length()-1).split(",");int precision=Integer.parseInt(dimensions[0]),scale=Integer.parseInt(dimensions[1]);
                var exact=new java.math.BigDecimal(value.toString()).setScale(scale,java.math.RoundingMode.UNNECESSARY);
                if(exact.precision()>precision)throw new IllegalArgumentException("Decimal target precision would overflow");
            }else if(type.matches("tinyint|smallint|mediumint|int|bigint")){
                if(!(value instanceof Number)&&!(value instanceof Boolean))throw new IllegalArgumentException("Integer mapping requires a numeric value");
                var n=value instanceof Boolean b?java.math.BigInteger.valueOf(b?1:0):new java.math.BigDecimal(value.toString()).toBigIntegerExact();
                int bits=switch(type){case "tinyint"->8;case "smallint"->16;case "mediumint"->24;case "int"->32;default->64;};
                if(n.compareTo(java.math.BigInteger.ONE.shiftLeft(bits-1).negate())<0||n.compareTo(java.math.BigInteger.ONE.shiftLeft(bits-1).subtract(java.math.BigInteger.ONE))>0)throw new IllegalArgumentException("Integer target range would overflow");
            }else{
                if(!(value instanceof String text))throw new IllegalArgumentException("Text mapping requires a string");
                if((keys.contains(column.name())||type.startsWith("char("))&&text.endsWith(" "))throw new IllegalArgumentException("Trailing spaces are outside the SQL text key and fixed-char contract");
                if(type.startsWith("varchar(")||type.startsWith("char(")){
                    int width=Integer.parseInt(type.substring(type.indexOf('(')+1,type.length()-1));
                    if(text.codePointCount(0,text.length())>width)throw new IllegalArgumentException("Text target width would truncate");
                }
            }
        }
    }
    public void apply(List<ChangeEvent> batch) throws Exception {
        try(Connection c=MysqlSchema.connect(config.target())){
            MysqlSchema actual=MysqlSchema.read(c,config.target(),true);
            if(!schema.equals(actual))throw new IllegalStateException("Target schema or server changed");
            try(Statement s=c.createStatement()){s.execute("SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'");}
            c.setAutoCommit(false);
            try(PreparedStatement put=c.prepareStatement(upsert);PreparedStatement del=c.prepareStatement(delete)){
                for(ChangeEvent e:batch){
                    if(!config.source().database().equals(e.database())||!config.source().table().equals(e.table()))throw new IllegalStateException("Unexpected source namespace");
                    validateKey(e.key());
                    switch(e.operation()){
                        case DELETE -> {bind(del,keys,e.key());del.executeUpdate();}
                        case SNAPSHOT,INSERT,UPDATE -> {
                            if(e.after()==null||!columnSet.equals(e.after().keySet()))throw new IllegalStateException("Incompatible row shape");
                            validateRow(e.after());
                            if(e.operation()==ChangeEvent.Operation.UPDATE){
                                if(e.before()==null||!columnSet.equals(e.before().keySet()))throw new IllegalStateException("Full before image required");
                                Map<String,Object> old=new LinkedHashMap<>();keys.forEach(k->old.put(k,e.before().get(k)));validateKey(old);
                                if(!old.equals(e.key())){bind(del,keys,old);del.executeUpdate();}
                            }
                            for(String k:keys)if(!Objects.equals(e.key().get(k),e.after().get(k)))throw new IllegalStateException("Key and row mismatch");
                            bind(put,columns,e.after());put.executeUpdate();
                        }
                        default -> throw new IllegalStateException("Unsupported event operation");
                    }
                }
                c.commit();
            }catch(Exception e){try{c.rollback();}catch(SQLException ignored){}throw e;}
        }
    }
}
