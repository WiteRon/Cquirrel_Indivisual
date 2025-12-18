package com.streamengine.query.config;

/**
 * Enumeration of operation types in the stream processing pipeline.
 * Defines how data records should be processed during stream operations.
 */
public enum OperationEnum {
    /** Insert a new record into the stream */
    INSERT,
    
    /** Delete a record from the stream */
    DELETE,
    
    /** Mark a record as active */
    ACTIVATE,
    
    /** Mark a record as inactive */
    DEACTIVATE,
    
    /** Mark the left side of a join as active */
    ACTIVATE_LEFT,
    
    /** Mark the right side of a join as active */
    ACTIVATE_RIGHT,
    
    /** Mark the left side of a join as inactive */
    DEACTIVATE_LEFT,
    
    /** Mark the right side of a join as inactive */
    DEACTIVATE_RIGHT,
    
    /** Perform aggregation operation */
    ACCUMULATE,
    
    /** Remove from aggregation */
    REMOVE
}

