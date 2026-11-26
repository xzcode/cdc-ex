package com.cdcex.postgresql;

import com.cdcex.core.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import static com.cdcex.postgresql.PostgresConfig.quote;

final class PostgresSink implements SinkConnector {
    private final PostgresConfig cfg;
    private final PostgresSchema schema;
    private final Map<String,PostgresSchema.Column> columns=new LinkedHashMap<>();
    private final String putSql,deleteSql;
    PostgresSink(PostgresConfig cfg) throws Exception {
        this.cfg=cfg;try(Connection c=cfg.connect()){schema=PostgresSchema.read(c,cfg,true);}
        for(var column:schema.columns())columns.put(column.name(),column);
        String fields=columns.keySet().stream().map(PostgresConfig::quote).collect(Collectors.joining(","));
        String placeholders=String.join(",",Collections.nCopies(columns.size(),"?"));
        String keys=schema.keys().stream().map(PostgresConfig::quote).collect(Collectors.joining(","));
        String updates=columns.keySet().stream().map(k->quote(k)+"=EXCLUDED."+quote(k)).collect(Collectors.joining(","));
        putSql="INSERT INTO "+cfg.qualified()+" ("+fields+") VALUES ("+placeholders+") ON CONFLICT ("+keys+") DO UPDATE SET "+updates;
        deleteSql="DELETE FROM "+cfg.qualified()+" WHERE "+schema.keys().stream().map(k->quote(k)+"=?").collect(Collectors.joining(" AND "));
    }
    private void key(Map<String,Object> key) {if(key==null||!key.keySet().equals(Set.copyOf(schema.keys()))||key.values().stream().anyMatch(Objects::isNull))throw new IllegalStateException("Missing or incompatible PostgreSQL key");}
    private void bind(PreparedStatement statement,List<String> names,Map<String,Object> row) throws SQLException {
        for(int i=0;i<names.size();i++)PostgresValues.bind(statement,i+1,columns.get(names.get(i)),row.get(names.get(i)));
    }
    @Override public void apply(List<ChangeEvent> events) throws Exception {
        try(Connection c=cfg.connect()) {
            schema.requireSame(PostgresSchema.read(c,cfg,true));c.setAutoCommit(false);
            try(PreparedStatement put=c.prepareStatement(putSql);PreparedStatement delete=c.prepareStatement(deleteSql)) {
                for(ChangeEvent event:events) {
                    if(!cfg.database().equals(event.database())||!cfg.table().equals(event.table()))throw new IllegalStateException("Unmapped PostgreSQL target namespace");
                    key(event.key());
                    switch(event.operation()) {
                        case DELETE -> {bind(delete,schema.keys(),event.key());delete.executeUpdate();}
                        case SNAPSHOT,INSERT,UPDATE -> {
                            if(event.after()==null||!event.after().keySet().equals(columns.keySet()))throw new IllegalStateException("PostgreSQL full row shape mismatch");
                            if(event.operation()==ChangeEvent.Operation.UPDATE&&event.before()!=null) {
                                Map<String,Object> old=new LinkedHashMap<>();for(String k:schema.keys())old.put(k,event.before().get(k));key(old);
                                if(!old.equals(event.key())){bind(delete,schema.keys(),old);delete.executeUpdate();}
                            }
                            for(String k:schema.keys())if(!Objects.equals(event.key().get(k),event.after().get(k)))throw new IllegalStateException("PostgreSQL key and row mismatch");
                            bind(put,new ArrayList<>(columns.keySet()),event.after());put.executeUpdate();
                        }
                        default -> throw new IllegalStateException("Unsupported PostgreSQL target operation");
                    }
                }
                c.commit();
            } catch(Exception e) {try{c.rollback();}catch(SQLException ignored){}throw e;}
        }
    }
}
