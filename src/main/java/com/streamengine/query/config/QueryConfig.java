package com.streamengine.query.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Configuration class for query parameters and constants.
 * Loads configuration from .env file or system properties.
 */
public class QueryConfig {
    
    private static final String ENV_FILE = ".env";
    private static QueryConfig instance;
    private final Properties properties;
    
    // Query condition constants
    public static final String DEFAULT_MARKET_SEGMENT = "AUTOMOBILE";
    public static final String DEFAULT_ORDER_DATE_THRESHOLD = "1995-03-13";
    public static final String DEFAULT_SHIP_DATE_THRESHOLD = "1995-03-13";
    
    // Table name constants
    public static final String TABLE_CUSTOMER = "customer";
    public static final String TABLE_ORDERS = "orders";
    public static final String TABLE_LINEITEM = "lineitem";
    
    // Field name constants
    public static final String FIELD_C_CUSTKEY = "c_custkey";
    public static final String FIELD_C_MKTSEGMENT = "c_mktsegment";
    public static final String FIELD_O_ORDERKEY = "o_orderkey";
    public static final String FIELD_O_CUSTKEY = "o_custkey";
    public static final String FIELD_O_ORDERDATE = "o_orderdate";
    public static final String FIELD_O_SHIPPRIORITY = "o_shippriority";
    public static final String FIELD_L_ORDERKEY = "l_orderkey";
    public static final String FIELD_L_EXTENDEDPRICE = "l_extendedprice";
    public static final String FIELD_L_DISCOUNT = "l_discount";
    public static final String FIELD_L_SHIPDATE = "l_shipdate";
    
    // Operation constants
    public static final String OPERATION_INSERT = "INSERT";
    public static final String OPERATION_DELETE = "DELETE";
    
    private QueryConfig() {
        properties = new Properties();
        loadConfiguration();
    }
    
    public static synchronized QueryConfig getInstance() {
        if (instance == null) {
            instance = new QueryConfig();
        }
        return instance;
    }
    
    private void loadConfiguration() {
        try {
            if (Files.exists(Paths.get(ENV_FILE))) {
                try (FileInputStream fis = new FileInputStream(ENV_FILE)) {
                    properties.load(fis);
                }
            }
        } catch (IOException e) {
            // Use defaults if .env file not found
        }
    }
    
    public String getProperty(String key, String defaultValue) {
        // First check system properties, then .env file, then default
        return System.getProperty(key, 
            properties.getProperty(key, defaultValue));
    }
    
    public String getMarketSegmentFilter() {
        return getProperty("MARKET_SEGMENT_FILTER", DEFAULT_MARKET_SEGMENT);
    }
    
    public String getOrderDateThreshold() {
        return getProperty("ORDER_DATE_THRESHOLD", DEFAULT_ORDER_DATE_THRESHOLD);
    }
    
    public String getShipDateThreshold() {
        return getProperty("SHIP_DATE_THRESHOLD", DEFAULT_SHIP_DATE_THRESHOLD);
    }
}

