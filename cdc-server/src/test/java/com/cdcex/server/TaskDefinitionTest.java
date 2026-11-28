package com.cdcex.server;
import com.cdcex.core.Endpoint;
import com.cdcex.runtime.MappingSpec;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TaskDefinitionTest {
    private final Endpoint source=new Endpoint("MYSQL",Map.of("host","db","table","original","database","src","replicationServerId","5000","password","private"));
    private final Endpoint sink=new Endpoint("MYSQL",Map.of("host","db","table","original","database","dst"));
    @Test void legacyTaskAndGroupingPreserveResolvedSpecification()throws Exception{
        TaskDefinition old=CdcApplication.JSON.readValue("{\"id\":\"task\",\"name\":\"Task\",\"sourceConnectionId\":\"source\",\"sinkConnectionId\":\"sink\"}",TaskDefinition.class);
        assertEquals("",old.group());assertEquals(old.resolve(source,sink),old.withGroup("Group").resolve(source,sink));
        assertEquals(source,old.resolve(source,sink).source());
    }
    @Test void overridesReuseSecretsButCannotChangeCredentialsOrHost(){
        var d=new TaskDefinition("task","Task","source","sink","initial",MappingSpec.identity(),0,0,0,0,"Group",Map.of("table","orders","replicationServerId","5001"),Map.of("table","orders_copy"));
        var spec=d.resolve(source,sink);
        assertEquals("orders",spec.source().require("table"));assertEquals("private",spec.source().require("password"));
        assertEquals("orders_copy",spec.sink().require("table"));assertEquals(spec,d.withGroup("Other").resolve(source,sink));
        assertEquals(d.sourceOverrides(),d.withLimits(2048,100,20,3).sourceOverrides());
        assertThrows(IllegalArgumentException.class,()->TaskDefinition.select(source,Map.of("table","orders","password","bad","replicationServerId","5001"),true));
        assertThrows(IllegalArgumentException.class,()->TaskDefinition.select(source,Map.of("table","orders"),true));
        assertThrows(IllegalArgumentException.class,()->TaskDefinition.select(source,Map.of("table","a;drop","replicationServerId","5001"),true));
    }
}
