package com.cdcex.kafka;

import com.cdcex.core.Endpoint;
import java.util.*;

/** Deliberately restricted configuration; arbitrary JAAS modules and client plugins are not accepted. */
final class KafkaSettings {
    private final Endpoint endpoint;
    KafkaSettings(Endpoint endpoint) {
        this.endpoint=Objects.requireNonNull(endpoint);
        if(!"KAFKA".equals(endpoint.type())) throw new IllegalArgumentException("Expected KAFKA endpoint");
        if(!endpoint.require("topic").matches("[A-Za-z0-9._-]{1,249}") || Set.of(".","..").contains(topic()))
            throw new IllegalArgumentException("Invalid Kafka topic");
        if(!"CDC_EX".equals(endpoint.option("format","CDC_EX")))
            throw new IllegalArgumentException("Kafka currently requires the CDC_EX version 1 envelope");
        for(String server:endpoint.require("bootstrapServers").split(",",-1)) {
            if(!server.trim().matches("(?:[A-Za-z0-9.-]+|\\[[A-Fa-f0-9:]+\\]):[0-9]{1,5}"))
                throw new IllegalArgumentException("Invalid Kafka bootstrap address");
            int port=Integer.parseInt(server.substring(server.lastIndexOf(':')+1));
            if(port<1||port>65535) throw new IllegalArgumentException("Invalid Kafka port");
        }
        if(!Set.of("earliest","latest").contains(startOffset())) throw new IllegalArgumentException("Unsupported Kafka start offset");
        if(endpoint.options().containsKey("groupId")&&!groupId().matches("[A-Za-z0-9._-]{1,180}"))
            throw new IllegalArgumentException("Invalid dedicated Kafka group");
        if(endpoint.integer("maxPollRecords",128)<1 || endpoint.integer("maxPollRecords",128)>1024)
            throw new IllegalArgumentException("maxPollRecords must be between 1 and 1024");
        if(endpoint.integer("maxMessageBytes",1048576)<1 || endpoint.integer("maxMessageBytes",1048576)>16777216)
            throw new IllegalArgumentException("maxMessageBytes must be between 1 and 16777216");
        commonProperties("validate");
    }
    String topic(){return endpoint.require("topic");}
    String groupId(){return endpoint.require("groupId");}
    String startOffset(){return endpoint.option("startOffset","earliest");}
    int maxMessageBytes(){return endpoint.integer("maxMessageBytes",1048576);}
    Properties commonProperties(String clientId) {
        Properties p=new Properties();
        p.setProperty("bootstrap.servers",endpoint.require("bootstrapServers"));
        p.setProperty("client.id","cdc-ex-"+clientId);
        p.setProperty("request.timeout.ms","10000");
        p.setProperty("default.api.timeout.ms","15000");
        String protocol=endpoint.option("securityProtocol","SSL");
        if(!Set.of("SSL","SASL_SSL","PLAINTEXT").contains(protocol))
            throw new IllegalArgumentException("Kafka security protocol must be SSL, SASL_SSL or explicitly allowed PLAINTEXT");
        if(protocol.equals("PLAINTEXT")&&!endpoint.flag("allowInsecure",false))
            throw new IllegalArgumentException("PLAINTEXT Kafka requires allowInsecure=true");
        p.setProperty("security.protocol",protocol);
        p.setProperty("ssl.endpoint.identification.algorithm","https");
        p.setProperty("ssl.enabled.protocols","TLSv1.3,TLSv1.2");
        copy(p,"sslTruststoreLocation","ssl.truststore.location");
        copy(p,"sslTruststorePassword","ssl.truststore.password");
        copy(p,"sslTruststoreType","ssl.truststore.type");
        copy(p,"sslKeystoreLocation","ssl.keystore.location");
        copy(p,"sslKeystorePassword","ssl.keystore.password");
        copy(p,"sslKeystoreType","ssl.keystore.type");
        copy(p,"sslKeyPassword","ssl.key.password");
        if(protocol.equals("SASL_SSL")) {
            String mechanism=endpoint.option("saslMechanism","SCRAM-SHA-512");
            if(!Set.of("PLAIN","SCRAM-SHA-256","SCRAM-SHA-512").contains(mechanism))
                throw new IllegalArgumentException("Unsupported Kafka SASL mechanism");
            String module=mechanism.equals("PLAIN")?"org.apache.kafka.common.security.plain.PlainLoginModule":"org.apache.kafka.common.security.scram.ScramLoginModule";
            p.setProperty("sasl.mechanism",mechanism);
            p.setProperty("sasl.jaas.config",module+" required username=\""+jaas(endpoint.require("saslUsername"))+"\" password=\""+jaas(endpoint.require("saslPassword"))+"\";");
        }
        return p;
    }
    Properties consumerProperties(String clientId) {
        Properties p=commonProperties(clientId);
        p.setProperty("group.id",groupId());
        p.setProperty("enable.auto.commit","false");
        p.setProperty("auto.offset.reset","none");
        p.setProperty("isolation.level","read_committed");
        p.setProperty("allow.auto.create.topics","false");
        p.setProperty("partition.assignment.strategy","org.apache.kafka.clients.consumer.RangeAssignor");
        p.setProperty("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.setProperty("max.poll.records",endpoint.option("maxPollRecords","128"));
        p.setProperty("max.poll.interval.ms","300000");
        p.setProperty("session.timeout.ms","10000");
        p.setProperty("heartbeat.interval.ms","3000");
        p.setProperty("metadata.max.age.ms","1000");
        p.setProperty("max.partition.fetch.bytes",Integer.toString(maxMessageBytes()));
        p.setProperty("fetch.max.bytes",Integer.toString(Math.max(maxMessageBytes(),16777216)));
        return p;
    }
    Properties producerProperties(String clientId) {
        Properties p=commonProperties(clientId);
        p.setProperty("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        p.setProperty("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        p.setProperty("acks","all");
        p.setProperty("enable.idempotence","true");
        p.setProperty("max.in.flight.requests.per.connection","1");
        p.setProperty("delivery.timeout.ms","30000");
        p.setProperty("max.block.ms","15000");
        p.setProperty("max.request.size",Integer.toString(maxMessageBytes()));
        p.setProperty("compression.type","lz4");
        return p;
    }
    private void copy(Properties p,String option,String key){String v=endpoint.options().get(option);if(v!=null&&!v.isBlank())p.setProperty(key,v);}
    private static String jaas(String v) {
        if(v.chars().anyMatch(c->c<32||c==127))throw new IllegalArgumentException("Control characters are not supported in SASL credentials");
        return v.replace("\\","\\\\").replace("\"","\\\"");
    }
}
