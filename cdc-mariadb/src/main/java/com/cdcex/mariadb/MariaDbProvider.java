package com.cdcex.mariadb;

import com.cdcex.core.SourceConnector;
import com.cdcex.mysql.*;
public final class MariaDbProvider extends MysqlProvider {
    public MariaDbProvider(){super(MysqlTaskConfig.Dialect.MARIADB);}
    @Override protected SourceConnector capture(MysqlTaskConfig cfg,String taskId){return new MariaDbSource(cfg,taskId);}
}
