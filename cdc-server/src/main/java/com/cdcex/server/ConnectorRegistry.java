package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.mysql.MysqlProvider;
import com.cdcex.mariadb.MariaDbProvider;
import com.cdcex.mongodb.MongoProvider;
import com.cdcex.postgresql.PostgresProvider;
import com.cdcex.kafka.KafkaProvider;
import com.cdcex.clickhouse.ClickHouseProvider;
import java.util.*;

public final class ConnectorRegistry {
    private final Map<String,ConnectorProvider> providers=new LinkedHashMap<>();
    public ConnectorRegistry(){
        for(var p:List.of(new MysqlProvider(),new MariaDbProvider(),new MongoProvider(),new PostgresProvider(),new KafkaProvider(),new ClickHouseProvider()))providers.put(p.type(),p);
    }
    public ConnectorProvider get(String type){var p=providers.get(type.toUpperCase(Locale.ROOT));if(p==null)throw new IllegalArgumentException("Unsupported connector type");return p;}
    public Map<String,Map<String,Object>> capabilities(){Map<String,Map<String,Object>> out=new LinkedHashMap<>();providers.forEach((k,p)->out.put(k,p.capabilities()));return out;}
}
