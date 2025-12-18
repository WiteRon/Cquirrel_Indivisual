package com.streamengine.query.domain;

import com.streamengine.query.config.QueryConfig;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Result record representing the final output of the query.
 */
public class QueryResult implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String orderKey;
    private String orderDate;
    private String shipPriority;
    private BigDecimal revenue;
    
    public QueryResult(DataRecord record) {
        this.orderKey = record.getFieldValue(QueryConfig.FIELD_L_ORDERKEY);
        this.orderDate = record.getFieldValue(QueryConfig.FIELD_O_ORDERDATE);
        this.shipPriority = record.getFieldValue(QueryConfig.FIELD_O_SHIPPRIORITY);
        this.revenue = BigDecimal.ZERO;
    }
    
    public String getOrderKey() {
        return orderKey;
    }
    
    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }
    
    public String getOrderDate() {
        return orderDate;
    }
    
    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }
    
    public String getShipPriority() {
        return shipPriority;
    }
    
    public void setShipPriority(String shipPriority) {
        this.shipPriority = shipPriority;
    }
    
    public BigDecimal getRevenue() {
        return revenue;
    }
    
    public void addRevenue(BigDecimal amount) {
        this.revenue = this.revenue.add(amount);
    }
    
    public void subtractRevenue(BigDecimal amount) {
        this.revenue = this.revenue.subtract(amount);
    }
    
    @Override
    public String toString() {
        return String.format("%s|%s|%s|%.4f",
                orderKey, orderDate, shipPriority, 
                revenue.setScale(4, RoundingMode.HALF_UP));
    }
}

