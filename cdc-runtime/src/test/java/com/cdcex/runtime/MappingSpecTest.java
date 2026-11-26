package com.cdcex.runtime;

import com.cdcex.core.*;
import org.junit.jupiter.api.Test;
import java.math.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class MappingSpecTest {
    @Test void mapsOldAndNewKeysAndPreservesExplicitNull(){
        var mapping=new MappingSpec(Map.of("_id","id"),Map.of("id","LONG","amount","DECIMAL"),List.of("ignored"));
        Map<String,Object> before=new LinkedHashMap<>();before.put("_id",Map.of("$numberLong","1"));before.put("amount",null);before.put("ignored","x");
        var event=new ChangeEvent("id","src","items",ChangeEvent.Operation.UPDATE,"token",Map.of("_id",Map.of("$numberLong","2")),before,Map.of("_id",Map.of("$numberLong","2"),"amount",Map.of("$numberDecimal","123.4500"),"ignored","y"),1);
        var result=mapping.apply(event,new Endpoint("MYSQL",Map.of("database","dst","table","rows")));
        assertEquals(1L,result.before().get("id"));assertEquals(2L,result.key().get("id"));assertTrue(result.before().containsKey("amount"));assertNull(result.before().get("amount"));assertFalse(result.after().containsKey("ignored"));assertEquals(new BigDecimal("123.4500"),result.after().get("amount"));
    }
    @Test void rejectsImplicitDecimalNarrowingAndNeverRoundsConversion(){
        var from=new ConnectorProvider.Inspection("a",Map.of("id","bigint","value","decimal(18,4)"),List.of("id"),Map.of());
        var to=new ConnectorProvider.Inspection("b",Map.of("id","bigint","value","decimal(18,2)"),List.of("id"),Map.of());
        assertThrows(IllegalArgumentException.class,()->MappingSpec.identity().validate(from,to));
        assertEquals(new BigDecimal("1.2345"),MappingSpec.convert("1.2345","DECIMAL"));
    }
    @Test void rejectsOverflowAndKeyLossBeforeDelivery(){
        assertThrows(ArithmeticException.class,()->MappingSpec.convert(Map.of("$numberLong","9223372036854775808"),"LONG"));
        assertThrows(ArithmeticException.class,()->MappingSpec.convert(new BigDecimal("1.5"),"INTEGER"));
        assertThrows(IllegalArgumentException.class,()->new MappingSpec(Map.of(),Map.of(),List.of("id")).row(Map.of("id",1),true));
        assertThrows(IllegalArgumentException.class,()->new MappingSpec(Map.of("a","b"),Map.of(),List.of()).row(Map.of("a",1,"b",2),false));
    }
}
