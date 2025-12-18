package com.streamengine.query.engine;

import com.streamengine.query.config.OperationEnum;
import com.streamengine.query.domain.ItemRecord;
import com.streamengine.query.domain.QueryResult;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Process function for aggregating item data to calculate revenue.
 * Maintains state for each key and updates aggregated results.
 */
public class RevenueCalculator extends KeyedProcessFunction<String, ItemRecord, QueryResult> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RevenueCalculator.class);
    
    private transient ValueState<QueryResult> resultState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        resultState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("result",
                TypeInformation.of(QueryResult.class)));
    }
    
    @Override
    public void processElement(ItemRecord item, Context ctx, Collector<QueryResult> out) throws Exception {
        try {
            String groupKey = item.getGroupKey();
            BigDecimal itemRevenue = item.computeRevenue();
            LOG.info("[RevenueCalculator] Processing: groupKey={}, itemRevenue={}, op={}", 
                groupKey, itemRevenue, item.getOperationType());
            
            if (resultState.value() == null) {
                resultState.update(new QueryResult(item));
                LOG.info("[RevenueCalculator] Initialized new result for groupKey={}", groupKey);
            }
            
            QueryResult result = resultState.value();
            BigDecimal revenueBefore = result.getRevenue();
            
            switch (item.getOperationType()) {
                case ACCUMULATE:
                    result.addRevenue(itemRevenue);
                    LOG.info("[RevenueCalculator] ACCUMULATE: revenue {} + {} = {}", 
                        revenueBefore, itemRevenue, result.getRevenue());
                    break;
                    
                case REMOVE:
                    result.subtractRevenue(itemRevenue);
                    LOG.info("[RevenueCalculator] REMOVE: revenue {} - {} = {}", 
                        revenueBefore, itemRevenue, result.getRevenue());
                    break;
                    
                default:
                    LOG.warn("[RevenueCalculator] Unsupported aggregation operation: {}", item.getOperationType());
                    return;
            }
            
            resultState.update(result);
            LOG.info("[RevenueCalculator] Emitting result: groupKey={}, revenue={}", groupKey, result.getRevenue());
            out.collect(result);
        } catch (Exception e) {
            LOG.error("[RevenueCalculator] Aggregation calculation error", e);
            throw e;
        }
    }
}

