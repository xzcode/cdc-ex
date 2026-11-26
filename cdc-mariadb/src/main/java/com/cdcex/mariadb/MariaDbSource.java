package com.cdcex.mariadb;

import com.cdcex.mysql.BinlogSource;
import com.cdcex.mysql.MysqlTaskConfig;

/** Dedicated MariaDB capture; uses MariaDB-specific log decoding, GTID and TLS modes. */
public final class MariaDbSource extends BinlogSource {
    public MariaDbSource(MysqlTaskConfig config){super(config);if(config.source().dialect()!=MysqlTaskConfig.Dialect.MARIADB)throw new IllegalArgumentException("MariaDB source requires MariaDB endpoint");}
    public MariaDbSource(MysqlTaskConfig config,String taskId){super(config,taskId);if(config.source().dialect()!=MysqlTaskConfig.Dialect.MARIADB)throw new IllegalArgumentException("MariaDB source requires MariaDB endpoint");}
    @Override protected String connectorClass(){return "io.debezium.connector.mariadb.MariaDbConnector";}
}
