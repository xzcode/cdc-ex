package com.cdcex.kafka;

import com.cdcex.core.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Uses only the ephemeral broker/certificates provisioned by scripts/kafka-security-smoke.sh. */
@EnabledIfEnvironmentVariable(named="CDC_TEST_KAFKA_TLS_BOOTSTRAP",matches=".+")
@Timeout(90)
class KafkaTransportTest {
    @TempDir Path directory;
    private Endpoint endpoint(String protocol,String mechanism,String topic,String group){
        Map<String,String> options=new LinkedHashMap<>();
        options.put("bootstrapServers",System.getenv(protocol.equals("SSL")?"CDC_TEST_KAFKA_TLS_BOOTSTRAP":"CDC_TEST_KAFKA_SASL_BOOTSTRAP"));
        options.put("topic",topic);options.put("groupId",group);options.put("securityProtocol",protocol);options.put("startOffset","earliest");
        options.put("sslTruststoreLocation",System.getenv("CDC_TEST_KAFKA_TRUSTSTORE"));
        options.put("sslTruststorePassword",System.getenv("CDC_TEST_KAFKA_PASSWORD"));options.put("sslTruststoreType","PKCS12");
        if(protocol.equals("SASL_SSL")){
            options.put("saslMechanism",mechanism);options.put("saslUsername","cdctest");options.put("saslPassword",System.getenv("CDC_TEST_KAFKA_PASSWORD"));
        }
        return new Endpoint("KAFKA",options);
    }
    @ParameterizedTest
    @CsvSource({"SSL,NONE","SASL_SSL,PLAIN","SASL_SSL,SCRAM-SHA-256","SASL_SSL,SCRAM-SHA-512"})
    void encryptedAuthenticatedRoundTrip(String protocol,String mechanism) throws Exception {
        String topic="cdcex-secure-"+UUID.randomUUID(),group="cdcex-secure-group-"+UUID.randomUUID();
        Endpoint endpoint=endpoint(protocol,mechanism,topic,group);KafkaSettings settings=new KafkaSettings(endpoint);
        try(Admin admin=Admin.create(settings.commonProperties("secure-test"))){
            admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(20,TimeUnit.SECONDS);
            SourceConnector source=null;SinkConnector sink=null;ExecutorService executor=Executors.newSingleThreadExecutor();
            try{
                var provider=new KafkaProvider();provider.inspect(endpoint,ConnectorProvider.Role.SOURCE);provider.inspect(endpoint,ConnectorProvider.Role.SINK);
                source=provider.openSource(endpoint,new ConnectorProvider.SourceContext(directory.resolve("reader"),"reader","earliest",true));
                sink=provider.openSink(endpoint,new ConnectorProvider.SinkContext(directory.resolve("writer"),"writer",true));
                var captured=new CompletableFuture<ChangeEvent>();SourceConnector capture=source;
                Future<?> worker=executor.submit(()->{capture.run(events->{if(!events.isEmpty())captured.complete(events.getFirst());});return null;});
                ChangeEvent event=new ChangeEvent("secure-event","source","items",ChangeEvent.Operation.INSERT,"position",Map.of("id",7),null,Map.of("id",7,"name","secure-delivery"),1);
                sink.apply(List.of(event));ChangeEvent result=captured.get(40,TimeUnit.SECONDS);
                assertEquals(EventCodec.canonicalJson(event.after()),EventCodec.canonicalJson(result.after()));
                assertTrue(result.position().contains("secure-event"));
                source.close();worker.get(20,TimeUnit.SECONDS);
            }finally{
                if(source!=null)source.close();if(sink!=null)sink.close();executor.shutdownNow();executor.awaitTermination(20,TimeUnit.SECONDS);
                try{admin.deleteConsumerGroups(List.of(group)).all().get(10,TimeUnit.SECONDS);}catch(Exception ignored){}
                admin.deleteTopics(List.of(topic)).all().get(15,TimeUnit.SECONDS);
            }
        }
    }
    @Test void untrustedCertificateAndWrongHostnameAreRejected() throws Exception {
        String topic="cdcex-tls-negative-"+UUID.randomUUID();Endpoint trusted=endpoint("SSL","NONE",topic,"security-validation");
        try(Admin admin=Admin.create(new KafkaSettings(trusted).commonProperties("tls-negative"))){
            admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(20,TimeUnit.SECONDS);
            try{
                new KafkaProvider().inspect(trusted,ConnectorProvider.Role.SOURCE);
                Map<String,String> untrusted=new LinkedHashMap<>(trusted.options());
                untrusted.remove("sslTruststoreLocation");untrusted.remove("sslTruststorePassword");untrusted.remove("sslTruststoreType");
                var badCertificate=assertThrows(Exception.class,()->new KafkaProvider().inspect(new Endpoint("KAFKA",untrusted),ConnectorProvider.Role.SOURCE));
                assertTrue(causedBy(badCertificate,SslAuthenticationException.class),"Expected TLS authentication failure");
                Map<String,String> wrongHost=new LinkedHashMap<>(trusted.options());
                // Fixture certificate is valid for IP 127.0.0.1 only, never the DNS name localhost.
                wrongHost.put("bootstrapServers",trusted.require("bootstrapServers").replace("127.0.0.1","localhost"));
                var badHostname=assertThrows(Exception.class,()->new KafkaProvider().inspect(new Endpoint("KAFKA",wrongHost),ConnectorProvider.Role.SOURCE));
                assertTrue(causedBy(badHostname,SslAuthenticationException.class),"Expected TLS hostname authentication failure");
            }finally{admin.deleteTopics(List.of(topic)).all().get(15,TimeUnit.SECONDS);}
        }
    }
    @Test void incorrectSaslPasswordIsRejected() throws Exception {
        String topic="cdcex-sasl-negative-"+UUID.randomUUID();Endpoint valid=endpoint("SASL_SSL","SCRAM-SHA-512",topic,"security-validation");
        try(Admin admin=Admin.create(new KafkaSettings(valid).commonProperties("sasl-negative"))){
            admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(20,TimeUnit.SECONDS);
            try{
                new KafkaProvider().inspect(valid,ConnectorProvider.Role.SOURCE);
                Map<String,String> options=new LinkedHashMap<>(valid.options());options.put("saslPassword","deliberately-wrong-fixture-password");
                var badPassword=assertThrows(Exception.class,()->new KafkaProvider().inspect(new Endpoint("KAFKA",options),ConnectorProvider.Role.SOURCE));
                assertTrue(causedBy(badPassword,SaslAuthenticationException.class),"Expected SASL authentication failure");
            }finally{admin.deleteTopics(List.of(topic)).all().get(15,TimeUnit.SECONDS);}
        }
    }
    private static boolean causedBy(Throwable error,Class<? extends Throwable> expected){for(Throwable e=error;e!=null;e=e.getCause())if(expected.isInstance(e))return true;return false;}
}
