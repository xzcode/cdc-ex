package com.cdcex.postgresql;

import com.cdcex.core.*;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import java.math.*;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PostgresValuesTest {
    @Test void exactNumbersRejectOverflowAndRounding() {
        assertEquals(Long.MAX_VALUE,PostgresValues.validate("bigint",new BigInteger("9223372036854775807")));
        assertThrows(ArithmeticException.class,()->PostgresValues.validate("bigint",new BigInteger("9223372036854775808")));
        assertEquals(new BigDecimal("12.30"),PostgresValues.validate("numeric(10,2)",new BigDecimal("12.3")));
        assertThrows(ArithmeticException.class,()->PostgresValues.validate("numeric(10,2)",new BigDecimal("12.345")));
        assertThrows(IllegalArgumentException.class,()->PostgresValues.validate("numeric(3,2)",new BigDecimal("12.34")));
    }
    @Test void microsecondTimestampsAreCorrectBeforeAndAfterEpoch() {
        var schema=SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").build();
        assertEquals("1969-12-31T23:59:59.999999",PostgresValues.fromConnect(schema,-1L));
        Object value=PostgresValues.fromConnect(schema,1_700_000_000_123456L);
        assertEquals(LocalDateTime.of(2023,11,14,22,13,20,123456000),PostgresValues.validate("timestamp(6) without time zone",value));
        assertThrows(IllegalArgumentException.class,()->PostgresValues.validate("timestamp(3) without time zone",value));
    }
    @Test void binaryIsLosslessAcrossDurableEventJson() {
        byte[] data={0,1,-1,10,13};Object value=PostgresValues.fromConnect(SchemaBuilder.bytes().build(),data);
        var event=new ChangeEvent("id","db","t",ChangeEvent.Operation.INSERT,"pos",Map.of("id",1),null,Map.of("id",1,"data",value),0);
        var decoded=EventCodec.decode(EventCodec.json(event));assertArrayEquals(data,(byte[])PostgresValues.validate("bytea",decoded.after().get("data")));
    }
    @Test void unavailableToastAndImplicitCoercionsAreRejected() {
        assertThrows(IllegalStateException.class,()->PostgresValues.fromConnect(SchemaBuilder.string().build(),PostgresValues.UNAVAILABLE));
        assertThrows(IllegalStateException.class,()->PostgresValues.fromConnect(SchemaBuilder.bytes().build(),PostgresValues.UNAVAILABLE.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class,()->PostgresValues.validate("integer","1"));
        assertThrows(IllegalArgumentException.class,()->PostgresValues.validate("boolean",1));
        assertThrows(IllegalArgumentException.class,()->PostgresValues.validate("character varying(2)","three"));
        assertFalse(PostgresSchema.supported("numeric"));assertFalse(PostgresSchema.supported("integer[]"));
        assertTrue(PostgresSchema.supported("numeric(18,2)"));assertTrue(PostgresSchema.supported("timestamp(6) with time zone"));
    }
    @Test void lsnComparisonIsUnsigned() {
        assertEquals(new BigInteger("18446744073709551615"),PostgresSource.lsn("FFFFFFFF/FFFFFFFF"));
        assertEquals(PostgresSource.unsigned(-1),PostgresSource.lsn("FFFFFFFF/FFFFFFFF"));
    }
    @Test void retainedWalDoesNotAuthorizeAnExternallyAdvancedSlot() {
        List<Map<String,Object>> offset=List.of(Map.of("lsn",512L,"lsn_proc",512L,"lsn_commit",480L));
        assertDoesNotThrow(()->PostgresSource.requireRetained(new PostgresSchema.Slot("0/100","0/1E0"),offset));
        assertDoesNotThrow(()->PostgresSource.requireRetained(new PostgresSchema.Slot("0/100","0/200"),offset));
        assertThrows(IllegalStateException.class,()->PostgresSource.requireRetained(new PostgresSchema.Slot("0/100","0/201"),offset));
        assertThrows(IllegalStateException.class,()->PostgresSource.requireRetained(new PostgresSchema.Slot("0/201","0/201"),offset));
        assertThrows(IllegalStateException.class,()->PostgresSource.requireRetained(new PostgresSchema.Slot("0/100",null),offset));
    }
    @Test void identifiersAndTlsRequireExplicitSafeConfiguration() {
        Map<String,String> options=new HashMap<>(Map.of("host","localhost","database","postgres","table","items","user","u","password","p","sslMode","disable"));
        assertThrows(IllegalArgumentException.class,()->new PostgresConfig(new Endpoint("POSTGRESQL",options)));
        options.put("allowInsecure","true");assertDoesNotThrow(()->new PostgresConfig(new Endpoint("POSTGRESQL",options)));
        options.put("table","items;DROP TABLE x");assertThrows(IllegalArgumentException.class,()->new PostgresConfig(new Endpoint("POSTGRESQL",options)));
    }
}
