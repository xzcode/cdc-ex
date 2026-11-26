package com.cdcex.clickhouse;

import com.cdcex.core.Endpoint;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/** Narrow HTTP client: fixed query templates, typed parameters, no redirects or server-error payload logging. */
final class ClickHouseHttp implements AutoCloseable {
    static final ObjectMapper JSON=new ObjectMapper().enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    @FunctionalInterface interface RowReceiver {void accept(Map<String,Object> row)throws Exception;}
    private final Endpoint endpoint;
    private final URI base;
    private final HttpClient client;
    ClickHouseHttp(Endpoint endpoint) {
        this.endpoint=endpoint;
        boolean tls=endpoint.flag("tls",true);
        if(!tls&&!endpoint.flag("allowInsecure",false))throw new IllegalArgumentException("ClickHouse HTTP requires explicit allowInsecure=true");
        String host=endpoint.require("host");int port=endpoint.integer("port",tls?8443:8123);
        if(!host.matches("[A-Za-z0-9.-]+")||port<1||port>65535)throw new IllegalArgumentException("Invalid ClickHouse host/port");
        base=URI.create((tls?"https":"http")+"://"+host+":"+port+"/");
        client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build();
    }
    private HttpRequest request(String sql,Map<String,String> parameters,boolean insert) {
        StringBuilder query=new StringBuilder("?database=").append(encode(endpoint.require("database")))
            .append("&output_format_json_quote_64bit_integers=1&output_format_json_quote_decimals=1&output_format_json_validate_utf8=0")
            .append("&max_execution_time=30");
        if(insert)query.append("&async_insert=0&wait_for_async_insert=1&wait_end_of_query=1");
        parameters.forEach((k,v)->{if(!k.matches("[A-Za-z0-9_]+"))throw new IllegalArgumentException("Invalid query parameter");query.append("&param_").append(k).append('=').append(encode(escapeParameter(v)));});
        return HttpRequest.newBuilder(base.resolve(query.toString())).timeout(Duration.ofSeconds(40))
            .header("X-ClickHouse-User",endpoint.option("user","default"))
            .header("X-ClickHouse-Key",endpoint.option("password",""))
            .header("Content-Type","text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(sql,StandardCharsets.UTF_8)).build();
    }
    private static String encode(String value){return URLEncoder.encode(value,StandardCharsets.UTF_8);}
    // HTTP query parameters are parsed with ClickHouse's escaped-string grammar, after URL decoding.
    static String escapeParameter(String value) {
        return value.replace("\\","\\\\").replace("\0","\\0").replace("\t","\\t").replace("\n","\\n").replace("\r","\\r");
    }
    List<Map<String,Object>> query(String sql,Map<String,String> parameters) throws Exception {
        List<Map<String,Object>> rows=new ArrayList<>();long[] bytes={0};
        scan(sql,parameters,row->{
            bytes[0]+=com.cdcex.core.EventCodec.json(row).getBytes(StandardCharsets.UTF_8).length;
            if(rows.size()>=100_001||bytes[0]>32L*1024*1024)throw new IllegalStateException("ClickHouse bounded query exceeded the row or 32 MiB payload limit");
            rows.add(row);
        });
        return rows;
    }
    void scan(String sql,Map<String,String> parameters,RowReceiver receiver)throws Exception {
        var response=client.send(request(sql+" FORMAT JSONEachRow",parameters,false),HttpResponse.BodyHandlers.ofInputStream());
        try(var body=response.body()) {
            check(response.statusCode(),response.headers());
            try(var reader=new BufferedReader(new InputStreamReader(body,StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT).onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)))) {
                String line;
                while((line=reader.readLine())!=null) {
                    if(line.isBlank())continue;
                    Map<String,Object> row;
                    try{row=JSON.readValue(line,new TypeReference<LinkedHashMap<String,Object>>(){});}
                    catch(Exception e){throw new IllegalStateException("Invalid or incomplete ClickHouse query response");}
                    receiver.accept(row);
                }
            }
        }
    }
    void insert(String sql,String rows)throws Exception {
        var response=client.send(request(sql+" FORMAT JSONEachRow\n"+rows,Map.of(),true),HttpResponse.BodyHandlers.ofString());
        check(response.statusCode(),response.headers());
        // INSERT normally has an empty body. A late HTTP error can otherwise arrive with status 200.
        if(!response.body().isBlank())throw new IllegalStateException("ClickHouse did not confirm an empty successful insert response");
    }
    private static void check(int status,HttpHeaders headers) {
        if(status!=200 || headers.firstValue("X-ClickHouse-Exception-Code").filter(v->!"0".equals(v)).isPresent())
            throw new IllegalStateException("ClickHouse request failed (HTTP "+status+")");
    }
    static Map<String,Object> object(String json) {
        if(json==null)return null;
        try{return JSON.readValue(json,new TypeReference<LinkedHashMap<String,Object>>(){});}
        catch(Exception e){throw new IllegalArgumentException("Invalid ClickHouse event JSON object");}
    }
    @Override public void close(){client.close();}
}
