package com.streamengine.query.processor;

import com.streamengine.query.config.OperationEnum;
import com.streamengine.query.config.QueryConfig;
import com.streamengine.query.domain.ItemRecord;
import com.streamengine.query.domain.OrderRecord;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * Join function for connecting order and item records.
 * Manages state for active orders and items.
 */
public class ItemJoiner extends KeyedCoProcessFunction<String, OrderRecord, ItemRecord, ItemRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ItemJoiner.class);
    
    private transient ValueState<Set<ItemRecord>> activeItemsState;
    private transient ValueState<Integer> counterState;
    private transient ValueState<OrderRecord> lastActiveOrderState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        activeItemsState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("activeItems",
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Set<ItemRecord>>() {})));
        
        counterState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("counter",
                org.apache.flink.api.common.typeinfo.IntegerTypeInfo.INT_TYPE_INFO));
        
        lastActiveOrderState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastActiveOrder",
                TypeInformation.of(OrderRecord.class)));
    }
    
    private void initState() throws Exception {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (activeItemsState.value() == null) {
            activeItemsState.update(new HashSet<>());
        }
    }
    
    private String generateGroupKey(ItemRecord item) {
        return item.getFieldValue(QueryConfig.FIELD_L_ORDERKEY) + "|" +
               item.getFieldValue(QueryConfig.FIELD_O_ORDERDATE) + "|" +
               item.getFieldValue(QueryConfig.FIELD_O_SHIPPRIORITY);
    }
    
    @Override
    public void processElement1(OrderRecord order, Context ctx, Collector<ItemRecord> out) throws Exception {
        try {
            initState();
            String orderKey = order.getFieldValue(QueryConfig.FIELD_O_ORDERKEY);
            String orderDate = order.getFieldValue(QueryConfig.FIELD_O_ORDERDATE);
            LOG.info("[ItemJoiner-Order] Processing order: key={}, date={}, op={}", 
                orderKey, orderDate, order.getOperationType());
            
            Set<ItemRecord> activeItems = activeItemsState.value();
            int activeItemsCount = activeItems != null ? activeItems.size() : 0;
            LOG.info("[ItemJoiner-Order] Active items count: {}", activeItemsCount);
            
            switch (order.getOperationType()) {
                case ACTIVATE_RIGHT:
                    lastActiveOrderState.update(order);
                    int newCounter = counterState.value() + 1;
                    counterState.update(newCounter);
                    LOG.info("[ItemJoiner-Order] ACTIVATE_RIGHT: counter={} -> {}", newCounter - 1, newCounter);
                    
                    // Associate with all active line items
                    // Note: activeItems only contains items that satisfy shipdate condition
                    // (filtered in processElement2 before adding to state)
                    int emittedCount = 0;
                    for (ItemRecord item : activeItems) {
                        ItemRecord newItem = new ItemRecord();
                        newItem.mergeFields(item);
                        newItem.mergeFields(order);
                        newItem.setOperationType(OperationEnum.ACCUMULATE);
                        String groupKey = generateGroupKey(newItem);
                        newItem.setGroupKey(groupKey);
                        BigDecimal revenue = newItem.computeRevenue();
                        LOG.info("[ItemJoiner-Order] Emitting ACCUMULATE: groupKey={}, revenue={}", groupKey, revenue);
                        out.collect(newItem);
                        emittedCount++;
                    }
                    LOG.info("[ItemJoiner-Order] ACTIVATE_RIGHT completed: emitted {} items", emittedCount);
                    break;
                    
                case DEACTIVATE_RIGHT:
                    lastActiveOrderState.update(null);
                    int oldCounter = counterState.value();
                    counterState.update(oldCounter - 1);
                    LOG.info("[ItemJoiner-Order] DEACTIVATE_RIGHT: counter={} -> {}", oldCounter, oldCounter - 1);
                    
                    // Disassociate from all active line items
                    // Note: activeItems only contains items that satisfy shipdate condition
                    // (filtered in processElement2 before adding to state)
                    int removedCount = 0;
                    for (ItemRecord item : activeItems) {
                        ItemRecord newItem = new ItemRecord();
                        newItem.mergeFields(item);
                        newItem.mergeFields(order);
                        newItem.setOperationType(OperationEnum.REMOVE);
                        String groupKey = generateGroupKey(newItem);
                        newItem.setGroupKey(groupKey);
                        BigDecimal revenue = newItem.computeRevenue();
                        LOG.info("[ItemJoiner-Order] Emitting REMOVE: groupKey={}, revenue={}", groupKey, revenue);
                        out.collect(newItem);
                        removedCount++;
                    }
                    LOG.info("[ItemJoiner-Order] DEACTIVATE_RIGHT completed: removed {} items", removedCount);
                    break;
                    
                default:
                    LOG.warn("[ItemJoiner-Order] Unsupported order operation: {}", order.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("[ItemJoiner-Order] Error processing order stream", e);
            throw e;
        }
    }
    
    @Override
    public void processElement2(ItemRecord item, Context ctx, Collector<ItemRecord> out) throws Exception {
        try {
            initState();
            String orderKey = item.getFieldValue(QueryConfig.FIELD_L_ORDERKEY);
            String shipDate = item.getFieldValue(QueryConfig.FIELD_L_SHIPDATE);
            BigDecimal revenue = item.computeRevenue();
            LOG.info("[ItemJoiner-Item] Processing item: orderKey={}, shipDate={}, revenue={}, op={}", 
                orderKey, shipDate, revenue, item.getOperationType());
            
            if (!item.isShipDateAfterThreshold()) {
                LOG.info("[ItemJoiner-Item] FILTERED: shipDate {} <= threshold, orderKey={}", shipDate, orderKey);
                return;
            }
            LOG.info("[ItemJoiner-Item] PASSED filter: shipDate {} > threshold", shipDate);
            
            Set<ItemRecord> activeItems = activeItemsState.value();
            OrderRecord lastOrder = lastActiveOrderState.value();
            int counter = counterState.value();
            LOG.info("[ItemJoiner-Item] State: counter={}, hasLastOrder={}, activeItemsCount={}", 
                counter, lastOrder != null, activeItems != null ? activeItems.size() : 0);
            
            switch (item.getOperationType()) {
                case INSERT:
                    activeItems.add(item);
                    activeItemsState.update(activeItems);
                    LOG.info("[ItemJoiner-Item] INSERT: Added to activeItems, new count={}", activeItems.size());
                    
                    if (counter > 0 && lastOrder != null) {
                        ItemRecord newItem = new ItemRecord();
                        newItem.mergeFields(item);
                        newItem.mergeFields(lastOrder);
                        newItem.setOperationType(OperationEnum.ACCUMULATE);
                        String groupKey = generateGroupKey(newItem);
                        newItem.setGroupKey(groupKey);
                        LOG.info("[ItemJoiner-Item] Emitting ACCUMULATE: groupKey={}, revenue={}", groupKey, revenue);
                        out.collect(newItem);
                    } else {
                        LOG.info("[ItemJoiner-Item] INSERT: No active order, not emitting (counter={}, lastOrder={})", 
                            counter, lastOrder != null);
                    }
                    break;
                    
                case DELETE:
                    // Check if the item exists in activeItems before processing DELETE
                    boolean itemExists = activeItems.contains(item);
                    
                    // Only emit REMOVE if item exists in activeItems and there's an active order
                    if (itemExists && counter > 0 && lastOrder != null) {
                        ItemRecord newItem = new ItemRecord();
                        newItem.mergeFields(item);
                        newItem.mergeFields(lastOrder);
                        newItem.setOperationType(OperationEnum.REMOVE);
                        String groupKey = generateGroupKey(newItem);
                        newItem.setGroupKey(groupKey);
                        LOG.info("[ItemJoiner-Item] Emitting REMOVE: groupKey={}, revenue={}", groupKey, revenue);
                        out.collect(newItem);
                    } else {
                        if (!itemExists) {
                            LOG.info("[ItemJoiner-Item] DELETE: Item not in activeItems, skipping REMOVE (orderKey={}, shipDate={})", 
                                orderKey, shipDate);
                        } else {
                            LOG.info("[ItemJoiner-Item] DELETE: No active order, not emitting (counter={}, lastOrder={})", 
                                counter, lastOrder != null);
                        }
                    }
                    
                    // Remove line item from active line items set (if it exists)
                    boolean removed = activeItems.remove(item);
                    activeItemsState.update(activeItems);
                    LOG.info("[ItemJoiner-Item] DELETE: Removed from activeItems, removed={}, new count={}", 
                        removed, activeItems.size());
                    break;
                    
                default:
                    LOG.warn("[ItemJoiner-Item] Unsupported item operation: {}", item.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("[ItemJoiner-Item] Error processing item stream", e);
            throw e;
        }
    }
}

