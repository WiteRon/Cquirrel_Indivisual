package com.streamengine.query.processor;

import com.streamengine.query.config.OperationEnum;
import com.streamengine.query.config.QueryConfig;
import com.streamengine.query.domain.ClientRecord;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter function for processing client records.
 * Filters clients based on market segment and transforms operation types.
 */
public class ClientFilter extends RichFilterFunction<ClientRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ClientFilter.class);
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
    
    @Override
    public boolean filter(ClientRecord client) throws Exception {
        try {
            if (!client.matchesTargetSegment()) {
                LOG.debug("Filtering non-target segment client: {}", 
                    client.getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
                return false;
            }
            
            switch (client.getOperationType()) {
                case INSERT:
                    client.setOperationType(OperationEnum.ACTIVATE);
                    client.setGroupKey(client.getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
                    LOG.debug("Processing client INSERT: {}", 
                        client.getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
                    return true;
                    
                case DELETE:
                    client.setOperationType(OperationEnum.DEACTIVATE);
                    client.setGroupKey(client.getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
                    LOG.debug("Processing client DELETE: {}", 
                        client.getFieldValue(QueryConfig.FIELD_C_CUSTKEY));
                    return true;
                    
                default:
                    LOG.warn("Unsupported client operation: {}", client.getOperationType());
                    return false;
            }
        } catch (Exception e) {
            LOG.error("Error processing client data", e);
            throw e;
        }
    }
}

