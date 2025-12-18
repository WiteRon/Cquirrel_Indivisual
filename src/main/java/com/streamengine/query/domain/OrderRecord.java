package com.streamengine.query.domain;

import com.streamengine.query.config.QueryConfig;
import java.time.LocalDate;

/**
 * Record representing an order in the dataset.
 */
public class OrderRecord extends BaseRecord {
    
    public OrderRecord() {
        super();
    }
    
    public OrderRecord(String[] rawFields) {
        super(rawFields);
    }
    
    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // rawFields format: [table_name, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, ...]
            setFieldValue(QueryConfig.FIELD_O_ORDERKEY, rawFields[1]);
            setFieldValue(QueryConfig.FIELD_O_CUSTKEY, rawFields[2]);
            setFieldValue(QueryConfig.FIELD_O_ORDERDATE, rawFields[5]);
            setFieldValue(QueryConfig.FIELD_O_SHIPPRIORITY, rawFields[8]);
            setGroupKey(getFieldValue(QueryConfig.FIELD_O_CUSTKEY));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse order data", e);
        }
    }
    
    /**
     * Check if the order date is before the threshold date
     */
    public boolean isDateBeforeThreshold() {
        try {
            String orderDate = getFieldValue(QueryConfig.FIELD_O_ORDERDATE);
            String threshold = QueryConfig.getInstance().getOrderDateThreshold();
            return LocalDate.parse(orderDate).isBefore(LocalDate.parse(threshold));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse order date", e);
        }
    }
}

