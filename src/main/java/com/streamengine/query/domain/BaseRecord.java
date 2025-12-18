package com.streamengine.query.domain;

import com.streamengine.query.config.OperationEnum;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of DataRecord interface.
 * Provides common functionality for all record types.
 */
public abstract class BaseRecord implements DataRecord, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    protected OperationEnum operationType;
    protected String groupKey;
    protected Map<String, String> fields;
    
    public BaseRecord() {
        this.fields = new HashMap<>(32);
    }
    
    public BaseRecord(String[] rawFields) {
        this();
        parseRawFields(rawFields);
    }
    
    /**
     * Parse raw field data into record-specific fields
     */
    protected abstract void parseRawFields(String[] rawFields);
    
    @Override
    public OperationEnum getOperationType() {
        return operationType;
    }
    
    @Override
    public void setOperationType(OperationEnum operationType) {
        this.operationType = operationType;
    }
    
    @Override
    public String getGroupKey() {
        return groupKey;
    }
    
    @Override
    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }
    
    @Override
    public Map<String, String> getFields() {
        return fields;
    }
    
    @Override
    public String getFieldValue(String fieldName) {
        return fields.getOrDefault(fieldName, "");
    }
    
    @Override
    public void setFieldValue(String fieldName, String value) {
        fields.put(fieldName, value);
    }
    
    @Override
    public void mergeFields(DataRecord other) {
        if (other != null && other.getFields() != null) {
            this.fields.putAll(other.getFields());
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BaseRecord that = (BaseRecord) obj;
        return Objects.equals(fields, that.fields);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}

