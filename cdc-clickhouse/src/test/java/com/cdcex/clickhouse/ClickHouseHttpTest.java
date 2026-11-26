package com.cdcex.clickhouse;

import com.cdcex.core.Endpoint;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseHttpTest {
    @Test void lateHttp200InsertErrorsAreNotAcknowledged()throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/",exchange->{
            exchange.getRequestBody().readAllBytes();byte[] body="Code: 241. Query failed after headers".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);exchange.close();
        });server.start();
        try(var client=new ClickHouseHttp(new Endpoint("CLICKHOUSE",Map.of("host","127.0.0.1","port",Integer.toString(server.getAddress().getPort()),
            "database","test","table","events","tls","false","allowInsecure","true")))) {
            assertThrows(IllegalStateException.class,()->client.insert("INSERT INTO test.events","{}\n"));
        } finally {server.stop(0);}
    }
}
