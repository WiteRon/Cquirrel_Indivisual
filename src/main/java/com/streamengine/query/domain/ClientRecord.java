package com.streamengine.query.domain;

import com.streamengine.query.config.QueryConfig;
import com.streamengine.query.util.ValidationUtil;

/**
 * Record representing a client (customer) in the dataset.
 */
public class ClientRecord extends BaseRecord {
    
    public ClientRecord() {
        super();
    }
    
    public ClientRecord(String[] rawFields) {
        super(rawFields);
    }
    
    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // rawFields format: [table_name, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]
            setFieldValue(QueryConfig.FIELD_C_CUSTKEY, rawFields[1]);
            setFieldValue(QueryConfig.FIELD_C_MKTSEGMENT, rawFields[7]);
            setGroupKey(getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse client data", e);
        }
    }
    
    /**
     * Check if this client belongs to the target market segment
     */
    public boolean matchesTargetSegment() {
        String segment = getFieldValue(QueryConfig.FIELD_C_MKTSEGMENT);
        return QueryConfig.getInstance().getMarketSegmentFilter().equals(segment);
    }
}

