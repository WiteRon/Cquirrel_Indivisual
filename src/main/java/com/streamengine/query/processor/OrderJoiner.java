package com.streamengine.query.processor;

import com.streamengine.query.config.OperationEnum;
import com.streamengine.query.config.QueryConfig;
import com.streamengine.query.domain.ClientRecord;
import com.streamengine.query.domain.DataRecord;
import com.streamengine.query.domain.OrderRecord;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Join function for connecting client and order records.
 * Manages state for active clients and orders.
 */
public class OrderJoiner extends KeyedCoProcessFunction<String, ClientRecord, OrderRecord, OrderRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(OrderJoiner.class);
    
    private transient ValueState<Set<OrderRecord>> activeOrdersState;
    private transient ValueState<Integer> counterState;
    private transient ValueState<ClientRecord> lastActiveClientState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        activeOrdersState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("activeOrders",
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Set<OrderRecord>>() {})));
        
        counterState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("counter",
                org.apache.flink.api.common.typeinfo.IntegerTypeInfo.INT_TYPE_INFO));
        
        lastActiveClientState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastActiveClient",
                TypeInformation.of(ClientRecord.class)));
    }
    
    private void initState() throws Exception {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (activeOrdersState.value() == null) {
            activeOrdersState.update(new HashSet<>());
        }
    }
    
    @Override
    public void processElement1(ClientRecord client, Context ctx, Collector<OrderRecord> out) throws Exception {
        try {
            initState();
            LOG.debug("Processing client stream: {}", 
                client.getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
            
            Set<OrderRecord> activeOrders = activeOrdersState.value();
            
            switch (client.getOperationType()) {
                case ACTIVATE:
                    lastActiveClientState.update(client);
                    counterState.update(counterState.value() + 1);
                    
                    for (OrderRecord order : activeOrders) {
                        OrderRecord newOrder = new OrderRecord();
                        newOrder.mergeFields(order);
                        newOrder.mergeFields(client);
                        newOrder.setOperationType(OperationEnum.ACTIVATE_RIGHT);
                        newOrder.setGroupKey(newOrder.getFieldValue(QueryConfig.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }
                    break;
                    
                case DEACTIVATE:
                    lastActiveClientState.update(null);
                    counterState.update(counterState.value() - 1);
                    
                    for (OrderRecord order : activeOrders) {
                        OrderRecord newOrder = new OrderRecord();
                        newOrder.mergeFields(order);
                        newOrder.mergeFields(client);
                        newOrder.setOperationType(OperationEnum.DEACTIVATE_RIGHT);
                        newOrder.setGroupKey(newOrder.getFieldValue(QueryConfig.FIELD_O_ORDERKEY));
                        out.collect(newOrder);
                    }
                    break;
                    
                default:
                    LOG.warn("Unsupported client operation: {}", client.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("Error processing client stream", e);
            throw e;
        }
    }
    
    @Override
    public void processElement2(OrderRecord order, Context ctx, Collector<OrderRecord> out) throws Exception {
        try {
            initState();
            String orderKey = order.getFieldValue(QueryConfig.FIELD_O_ORDERKEY);
            String orderDate = order.getFieldValue(QueryConfig.FIELD_O_ORDERDATE);
            LOG.info("[OrderJoiner-Order] Processing order: key={}, date={}, op={}", 
                orderKey, orderDate, order.getOperationType());
            
            if (!order.isDateBeforeThreshold()) {
                LOG.info("[OrderJoiner-Order] FILTERED: orderDate {} >= threshold, orderKey={}", orderDate, orderKey);
                return;
            }
            LOG.info("[OrderJoiner-Order] PASSED filter: orderDate {} < threshold", orderDate);
            
            Set<OrderRecord> activeOrders = activeOrdersState.value();
            ClientRecord lastClient = lastActiveClientState.value();
            int counter = counterState.value();
            LOG.info("[OrderJoiner-Order] State: counter={}, hasLastClient={}, activeOrdersCount={}", 
                counter, lastClient != null, activeOrders != null ? activeOrders.size() : 0);
            
            switch (order.getOperationType()) {
                case INSERT:
                    activeOrders.add(order);
                    activeOrdersState.update(activeOrders);
                    LOG.info("[OrderJoiner-Order] INSERT: Added to activeOrders, new count={}", activeOrders.size());
                    
                    if (counter > 0 && lastClient != null) {
                        OrderRecord newOrder = new OrderRecord();
                        newOrder.mergeFields(order);
                        newOrder.mergeFields(lastClient);
                        newOrder.setOperationType(OperationEnum.ACTIVATE_RIGHT);
                        newOrder.setGroupKey(newOrder.getFieldValue(QueryConfig.FIELD_O_ORDERKEY));
                        LOG.info("[OrderJoiner-Order] Emitting ACTIVATE_RIGHT: orderKey={}", orderKey);
                        out.collect(newOrder);
                    } else {
                        LOG.info("[OrderJoiner-Order] INSERT: No active client, not emitting (counter={}, lastClient={})", 
                            counter, lastClient != null);
                    }
                    break;
                    
                case DELETE:
                    if (counter > 0 && lastClient != null) {
                        OrderRecord newOrder = new OrderRecord();
                        newOrder.mergeFields(order);
                        newOrder.mergeFields(lastClient);
                        newOrder.setOperationType(OperationEnum.DEACTIVATE_RIGHT);
                        newOrder.setGroupKey(newOrder.getFieldValue(QueryConfig.FIELD_O_ORDERKEY));
                        LOG.info("[OrderJoiner-Order] Emitting DEACTIVATE_RIGHT: orderKey={}", orderKey);
                        out.collect(newOrder);
                    } else {
                        LOG.info("[OrderJoiner-Order] DELETE: No active client, not emitting (counter={}, lastClient={})", 
                            counter, lastClient != null);
                    }
                    
                    activeOrders.remove(order);
                    activeOrdersState.update(activeOrders);
                    LOG.info("[OrderJoiner-Order] DELETE: Removed from activeOrders, new count={}", activeOrders.size());
                    break;
                    
                default:
                    LOG.warn("[OrderJoiner-Order] Unsupported order operation: {}", order.getOperationType());
            }
        } catch (Exception e) {
            LOG.error("[OrderJoiner-Order] Error processing order stream", e);
            throw e;
        }
    }
}

