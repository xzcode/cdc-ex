package com.cdcex.kafka;

import com.cdcex.core.Endpoint;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaSettingsTest {
    private Map<String,String> options(){return new HashMap<>(Map.of("bootstrapServers","localhost:9092","topic","events","groupId","dedicated"));}
    @Test void secureDefaultsCannotBeOverriddenByUnsafeClientOptions(){
        var options=options();options.put("enable.auto.commit","true");options.put("acks","0");
        var settings=new KafkaSettings(new Endpoint("KAFKA",options));
        assertEquals("SSL",settings.commonProperties("test").getProperty("security.protocol"));
        assertEquals("https",settings.commonProperties("test").getProperty("ssl.endpoint.identification.algorithm"));
        assertEquals("false",settings.consumerProperties("test").getProperty("enable.auto.commit"));
        assertEquals("none",settings.consumerProperties("test").getProperty("auto.offset.reset"));
        assertEquals("read_committed",settings.consumerProperties("test").getProperty("isolation.level"));
        assertEquals("all",settings.producerProperties("test").getProperty("acks"));
    }
    @Test void plaintextRequiresExplicitConsentAndSaslModulesAreRestricted(){
        var options=options();options.put("securityProtocol","PLAINTEXT");
        assertThrows(IllegalArgumentException.class,()->new KafkaSettings(new Endpoint("KAFKA",options)));
        options.put("allowInsecure","true");assertDoesNotThrow(()->new KafkaSettings(new Endpoint("KAFKA",options)));
        options.put("securityProtocol","SASL_SSL");options.put("saslMechanism","arbitrary.module");
        assertThrows(IllegalArgumentException.class,()->new KafkaSettings(new Endpoint("KAFKA",options)));
    }
    @Test void credentialsAreEscapedAndNeverReturnedByEndpointToString(){
        var options=options();options.put("securityProtocol","SASL_SSL");options.put("saslMechanism","PLAIN");
        options.put("saslUsername","user\"name");options.put("saslPassword","unique-secret\\\"");
        var endpoint=new Endpoint("KAFKA",options);var settings=new KafkaSettings(endpoint);
        String jaas=settings.commonProperties("test").getProperty("sasl.jaas.config");
        assertTrue(jaas.contains("user\\\"name"));assertFalse(endpoint.toString().contains("unique-secret"));
    }
}
