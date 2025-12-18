package com.streamengine.query.domain;

import com.streamengine.query.config.QueryConfig;
import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Record representing a line item in the dataset.
 */
public class ItemRecord extends BaseRecord {
    
    public ItemRecord() {
        super();
    }
    
    public ItemRecord(String[] rawFields) {
        super(rawFields);
    }
    
    @Override
    protected void parseRawFields(String[] rawFields) {
        try {
            // rawFields format: [table_name, l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, ...]
            // StreamRouter removes the operation (INSERT/DELETE) before passing to ItemRecord
            // So rawFields[0] = table_name, rawFields[1] = l_orderkey, ..., rawFields[11] = l_shipdate
            setFieldValue(QueryConfig.FIELD_L_ORDERKEY, rawFields[1]);
            setFieldValue(QueryConfig.FIELD_L_EXTENDEDPRICE, rawFields[6]);
            setFieldValue(QueryConfig.FIELD_L_DISCOUNT, rawFields[7]);
            setFieldValue(QueryConfig.FIELD_L_SHIPDATE, rawFields[11]);
            setGroupKey(getFieldValue(QueryConfig.FIELD_L_ORDERKEY));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse item data", e);
        }
    }
    
    /**
     * Check if the ship date is after the threshold date
     */
    public boolean isShipDateAfterThreshold() {
        try {
            String shipDate = getFieldValue(QueryConfig.FIELD_L_SHIPDATE);
            String threshold = QueryConfig.getInstance().getShipDateThreshold();
            return LocalDate.parse(shipDate).isAfter(LocalDate.parse(threshold));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse ship date", e);
        }
    }
    
    /**
     * Calculate the revenue for this line item
     */
    public BigDecimal computeRevenue() {
        try {
            BigDecimal extendedPrice = new BigDecimal(
                    getFieldValue(QueryConfig.FIELD_L_EXTENDEDPRICE));
            BigDecimal discount = new BigDecimal(
                    getFieldValue(QueryConfig.FIELD_L_DISCOUNT));
            return extendedPrice.multiply(BigDecimal.ONE.subtract(discount));
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate revenue", e);
        }
    }
}

