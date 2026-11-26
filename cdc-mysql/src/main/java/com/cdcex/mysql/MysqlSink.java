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
    private void validateKey(Map<String,Object> k){if(k==null||!keySet.equals(k.keySet())||k.values().stream().anyMatch(Objects::isNull))throw new IllegalStateException("Missing or incompatible primary key");}
    private static void bind(PreparedStatement s,List<String> cols,Map<String,Object> row) throws SQLException{
        for(int i=0;i<cols.size();i++)s.setObject(i+1,row.get(cols.get(i)));
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
