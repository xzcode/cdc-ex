package com.cdcex.postgresql;

import com.cdcex.core.Endpoint;
import java.sql.*;
import java.util.*;

record PostgresConfig(Endpoint endpoint) {
    PostgresConfig {
        Objects.requireNonNull(endpoint);
        if(!endpoint.type().equals("POSTGRESQL")) throw new IllegalArgumentException("Expected PostgreSQL endpoint");
        if(!endpoint.require("host").matches("[A-Za-z0-9.-]+")) throw new IllegalArgumentException("Invalid PostgreSQL host");
        int port=endpoint.integer("port",5432); if(port<1||port>65535) throw new IllegalArgumentException("Invalid port");
        identifier(endpoint.require("database"));identifier(endpoint.option("schema","public"));identifier(endpoint.require("table"));
        endpoint.require("user");endpoint.require("password");
        if(!Set.of("disable","require","verify-ca","verify-full").contains(endpoint.option("sslMode","verify-full"))) throw new IllegalArgumentException("Invalid PostgreSQL TLS mode");
        if("disable".equals(endpoint.option("sslMode","verify-full"))&&!endpoint.flag("allowInsecure",false)) throw new IllegalArgumentException("Plaintext PostgreSQL requires allowInsecure=true");
    }
    static void identifier(String value) { if(value==null||!value.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")) throw new IllegalArgumentException("Unsupported PostgreSQL identifier"); }
    static String quote(String value) { identifier(value); return '"'+value+'"'; }
    String database() { return endpoint.require("database"); }
    String schema() { return endpoint.option("schema","public"); }
    String table() { return endpoint.require("table"); }
    String qualified() { return quote(schema())+"."+quote(table()); }
    String slot() { String slot=endpoint.require("slot"); if(!slot.matches("[a-z0-9_]{1,63}")) throw new IllegalArgumentException("Invalid replication slot name"); return slot; }
    String publication() { String p=endpoint.require("publication");identifier(p);return p; }
    Connection connect() throws SQLException {
        try { Class.forName("org.postgresql.Driver"); } catch(ClassNotFoundException e) { throw new SQLException("PostgreSQL driver unavailable"); }
        Properties p=new Properties();p.setProperty("user",endpoint.require("user"));p.setProperty("password",endpoint.require("password"));
        p.setProperty("sslmode",endpoint.option("sslMode","verify-full"));
        if(endpoint.options().containsKey("sslRootCert")) p.setProperty("sslrootcert",endpoint.require("sslRootCert"));
        p.setProperty("connectTimeout","5");p.setProperty("socketTimeout","20");p.setProperty("ApplicationName","cdc-ex");
        p.setProperty("options","-c TimeZone=UTC -c statement_timeout=15000 -c lock_timeout=5000");
        return DriverManager.getConnection("jdbc:postgresql://"+endpoint.require("host")+":"+endpoint.integer("port",5432)+"/"+database(),p);
    }
    @Override public String toString() { return "PostgresConfig[redacted]"; }
}
