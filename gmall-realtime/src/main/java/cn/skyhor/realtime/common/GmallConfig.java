package cn.skyhor.realtime.common;

/**
 * @author wbw
 */
public class GmallConfig {
    /**
     * Phoenix 库名
     */
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    /**
     * Phoenix 驱动
     */
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    /**
     * Phoenix 连接参数
     */
    public static final String PHOENIX_SERVER =
            "jdbc:phoenix:server101:2181";

    /**
     * ClickHouse_Url
     */
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://server101:8123/default";

    /**
     * ClickHouse_Driver
     */
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
