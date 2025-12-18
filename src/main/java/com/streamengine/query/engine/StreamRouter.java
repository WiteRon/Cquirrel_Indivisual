package com.streamengine.query.engine;

import com.streamengine.query.config.OperationEnum;
import com.streamengine.query.config.QueryConfig;
import com.streamengine.query.domain.ClientRecord;
import com.streamengine.query.domain.DataRecord;
import com.streamengine.query.domain.ItemRecord;
import com.streamengine.query.domain.OrderRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Router function that splits input stream into separate streams for each record type.
 * Uses OutputTag to route different record types to different streams.
 */
public class StreamRouter extends ProcessFunction<String, DataRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamRouter.class);
    
    public static final OutputTag<ClientRecord> CLIENT_TAG = new OutputTag<ClientRecord>("client") {};
    public static final OutputTag<OrderRecord> ORDER_TAG = new OutputTag<OrderRecord>("order") {};
    public static final OutputTag<ItemRecord> ITEM_TAG = new OutputTag<ItemRecord>("item") {};
    
    @Override
    public void processElement(String value, Context ctx, Collector<DataRecord> out) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            LOG.warn("Ignoring empty data row");
            return;
        }
        
        String[] parts = value.split("\\|");
        if (parts.length < 2) {
            LOG.warn("Invalid data format: {}", value);
            return;
        }
        
        String operation = parts[0];
        String tableName = parts[1];
        
        DataRecord record = createRecord(tableName, parts);
        if (record == null) {
            LOG.warn("Unsupported table name: {}", tableName);
            return;
        }
        
        if (QueryConfig.OPERATION_INSERT.equalsIgnoreCase(operation)) {
            record.setOperationType(OperationEnum.INSERT);
        } else if (QueryConfig.OPERATION_DELETE.equalsIgnoreCase(operation)) {
            record.setOperationType(OperationEnum.DELETE);
        } else {
            LOG.warn("Unsupported operation type: {}", operation);
            return;
        }
        
        // Route to appropriate output stream
        if (record instanceof ClientRecord) {
            ctx.output(CLIENT_TAG, (ClientRecord) record);
        } else if (record instanceof OrderRecord) {
            ctx.output(ORDER_TAG, (OrderRecord) record);
        } else if (record instanceof ItemRecord) {
            ctx.output(ITEM_TAG, (ItemRecord) record);
        }
        
        // Log routing progress for monitoring
        LOG.info("Routed data: Table={}, Operation={}", tableName, operation);
    }
    
    private DataRecord createRecord(String tableName, String[] values) {
        String[] recordFields = new String[values.length - 1];
        System.arraycopy(values, 1, recordFields, 0, recordFields.length);
        
        String tableNameLower = tableName.toLowerCase();
        if (QueryConfig.TABLE_CUSTOMER.equals(tableNameLower)) {
            return new ClientRecord(recordFields);
        } else if (QueryConfig.TABLE_ORDERS.equals(tableNameLower)) {
            return new OrderRecord(recordFields);
        } else if (QueryConfig.TABLE_LINEITEM.equals(tableNameLower)) {
            return new ItemRecord(recordFields);
        } else {
            return null;
        }
    }
}

