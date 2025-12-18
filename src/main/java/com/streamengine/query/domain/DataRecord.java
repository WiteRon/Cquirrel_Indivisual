package com.streamengine.query.domain;

import com.streamengine.query.config.OperationEnum;
import java.util.Map;

/**
 * Interface for data records in the stream processing pipeline.
 * Provides common functionality for handling record data.
 */
public interface DataRecord {
    
    /**
     * Get the operation type for this record
     */
    OperationEnum getOperationType();
    
    /**
     * Set the operation type for this record
     */
    void setOperationType(OperationEnum operationType);
    
    /**
     * Get the key value used for grouping and joining
     */
    String getGroupKey();
    
    /**
     * Set the key value used for grouping and joining
     */
    void setGroupKey(String groupKey);
    
    /**
     * Get all fields as a map
     */
    Map<String, String> getFields();
    
    /**
     * Get the value of a specific field
     */
    String getFieldValue(String fieldName);
    
    /**
     * Set the value of a specific field
     */
    void setFieldValue(String fieldName, String value);
    
    /**
     * Merge fields from another record into this one
     */
    void mergeFields(DataRecord other);
    
    /**
     * Check equality based on fields
     */
    boolean equals(Object obj);
    
    /**
     * Get hash code based on fields
     */
    int hashCode();
}

