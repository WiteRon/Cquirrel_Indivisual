package com.streamengine.query.util;

import com.streamengine.query.domain.ClientRecord;
import com.streamengine.query.domain.ItemRecord;
import com.streamengine.query.domain.OrderRecord;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector utility classes for grouping operations.
 */
public class KeySelectors {
    
    public static class ClientKeySelector implements KeySelector<ClientRecord, String> {
        @Override
        public String getKey(ClientRecord value) throws Exception {
            return value.getGroupKey();
        }
    }
    
    public static class OrderKeySelector implements KeySelector<OrderRecord, String> {
        @Override
        public String getKey(OrderRecord value) throws Exception {
            return value.getGroupKey();
        }
    }
    
    public static class ItemKeySelector implements KeySelector<ItemRecord, String> {
        @Override
        public String getKey(ItemRecord value) throws Exception {
            return value.getGroupKey();
        }
    }
}

