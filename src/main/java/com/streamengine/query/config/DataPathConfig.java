package com.streamengine.query.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Configuration class for data file paths.
 * Loads paths from .env file or uses defaults.
 */
public class DataPathConfig {
    
    private static final String ENV_FILE = ".env";
    private static DataPathConfig instance;
    private final Properties properties;
    
    private static final String DEFAULT_INPUT_PATH = "./scripts/data/tpch_q3.tbl";
    private static final String DEFAULT_OUTPUT_PATH = "./scripts/data/output.csv";
    
    private DataPathConfig() {
        properties = new Properties();
        loadConfiguration();
    }
    
    public static synchronized DataPathConfig getInstance() {
        if (instance == null) {
            instance = new DataPathConfig();
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
        return System.getProperty(key, 
            properties.getProperty(key, defaultValue));
    }
    
    public String getInputFilePath() {
        return getProperty("DATA_INPUT_FILE", DEFAULT_INPUT_PATH);
    }
    
    public String getOutputFilePath() {
        return getProperty("DATA_OUTPUT_FILE", DEFAULT_OUTPUT_PATH);
    }
}

