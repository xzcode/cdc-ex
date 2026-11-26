package com.cdcex.postgresql;

import com.cdcex.core.*;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/** Single-table PostgreSQL native pgoutput source and transactional keyed sink. */
public final class PostgresProvider implements ConnectorProvider {
    @Override public String type(){return "POSTGRESQL";}
    @Override public Map<String,Object> capabilities(){return Map.of("sourceModes",List.of("initial","no_data"),"sinkModes",List.of("upsert"),"nativeCdc",true,"keyRequired",true,"snapshot",true,"types",List.of("signed integers","fixed numeric","boolean","text","date/time/timestamp","uuid","json/jsonb","bytea"),"limits",List.of("one permanent table per task","replica identity FULL","pre-created slot/publication","no automatic DDL","no arrays/domains/floats"));}
    @Override public Inspection inspect(Endpoint endpoint,Role role) throws Exception {
        PostgresConfig cfg=new PostgresConfig(endpoint);
        try(Connection c=cfg.connect()) {
            var schema=PostgresSchema.read(c,cfg,role==Role.SINK);
            if(role==Role.SOURCE)PostgresSchema.sourcePreflight(c,cfg,schema,true);
            var inspection=schema.inspection();Map<String,Object> details=new LinkedHashMap<>(inspection.details());
            if(role==Role.SINK)details.put("empty","t".equals(PostgresSchema.one(c,"SELECT NOT EXISTS (SELECT 1 FROM "+cfg.qualified()+" LIMIT 1)")));
            return new Inspection(inspection.identity(),inspection.fields(),inspection.keyFields(),details);
        }
    }
    @Override public SourceConnector openSource(Endpoint endpoint,SourceContext context) throws Exception {return new PostgresSource(new PostgresConfig(endpoint),context);}
    @Override public SinkConnector openSink(Endpoint endpoint,SinkContext context) throws Exception {return new PostgresSink(new PostgresConfig(endpoint));}
    @Override public List<Map<String,Object>> readRows(Endpoint endpoint,int limit) throws Exception {
        if(limit<1||limit>100001)throw new IllegalArgumentException("Invalid comparison row limit");
        PostgresConfig cfg=new PostgresConfig(endpoint);
        try(Connection c=cfg.connect()) {
            var schema=PostgresSchema.readForComparison(c,cfg);
            c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
            String names=schema.columns().stream().map(v->PostgresConfig.quote(v.name())).collect(Collectors.joining(","));
            String order=schema.keys().stream().map(PostgresConfig::quote).collect(Collectors.joining(","));
            List<Map<String,Object>> rows=new ArrayList<>();
            try(var q=c.prepareStatement("SELECT "+names+" FROM "+cfg.qualified()+" ORDER BY "+order+" LIMIT ?")) {
                q.setInt(1,limit);q.setFetchSize(32);long bytes=0;
                try(var r=q.executeQuery()) {while(r.next()) {
                    Map<String,Object> row=new LinkedHashMap<>();
                    for(int i=0;i<schema.columns().size();i++){var col=schema.columns().get(i);row.put(col.name(),PostgresValues.fromJdbc(r,i+1,col.type()));}
                    bytes+=EventCodec.json(row).getBytes(StandardCharsets.UTF_8).length;
                    if(bytes>32L*1024*1024)throw new IllegalStateException("Comparison exceeds 32 MiB payload budget");
                    rows.add(row);
                }}
            }
            return rows;
        }
    }
}
