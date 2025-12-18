package com.streamengine.query.sink;

import com.streamengine.query.domain.QueryResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * Sink function for outputting final aggregated revenue results.
 */
public class ResultWriter extends RichSinkFunction<QueryResult> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ResultWriter.class);
    
    private Map<String, BigDecimal> aggregatedResults;
    private AsyncWriter asyncWriter;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        aggregatedResults = new HashMap<>();
        asyncWriter = AsyncWriter.getInstance();
    }
    
    @Override
    public void invoke(QueryResult result, Context context) {
        if (result == null) {
            LOG.warn("[ResultWriter] Ignoring null output data");
            return;
        }
        
        try {
            String orderKey = result.getOrderKey();
            String orderDate = result.getOrderDate();
            String shipPriority = result.getShipPriority();
            BigDecimal revenue = result.getRevenue()
                    .setScale(4, RoundingMode.HALF_UP);
            
            String groupKey = String.join("|", orderKey, orderDate, shipPriority);
            
            BigDecimal oldRevenue = aggregatedResults.get(groupKey);
            if (oldRevenue != null) {
                LOG.info("[ResultWriter] Updating existing result: groupKey={}, oldRevenue={}, newRevenue={}", 
                    groupKey, oldRevenue, revenue);
            } else {
                LOG.info("[ResultWriter] New result: groupKey={}, revenue={}", groupKey, revenue);
            }
            
            // RevenueCalculator emits the accumulated revenue after each item update
            // Since items for the same groupKey are processed sequentially in the same operator instance,
            // each emit contains the cumulative revenue up to that point.
            // We use put() to keep the latest (final) value for each groupKey.
            aggregatedResults.put(groupKey, revenue);
        } catch (Exception e) {
            LOG.error("[ResultWriter] Failed to process result", e);
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        int successCount = 0;
        
        LOG.info("Task finished, total size is {}", aggregatedResults.size());
        
        asyncWriter.start();
        
        for (Map.Entry<String, BigDecimal> entry : aggregatedResults.entrySet()) {
            String groupKey = entry.getKey();
            BigDecimal totalRevenue = entry.getValue();
            
            if (totalRevenue.compareTo(BigDecimal.ZERO) < 0) {
                continue;
            }
            
            try {
                String[] keys = groupKey.split("\\|", 3);
                if (keys.length < 3) {
                    LOG.warn("Invalid group key format: {}", groupKey);
                    continue;
                }
                
                String orderKey = keys[0];
                String orderDateStr = keys[1];
                String shipPriority = keys[2];
                
                String formattedOutput = String.format(
                        "%s, %s, %s, %s",
                        orderKey,
                        orderDateStr,
                        shipPriority,
                        totalRevenue.setScale(4, RoundingMode.HALF_UP).toPlainString()
                );
                asyncWriter.enqueue(formattedOutput);
                successCount++;
            } catch (Exception e) {
                LOG.error("Failed to process record (key: {}): {}", groupKey, e.getMessage(), e);
            }
        }
        LOG.info("Aggregation results output completed, {} valid records in total", successCount);
    }
}

