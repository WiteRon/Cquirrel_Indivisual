package com.streamengine.query.engine;

import com.streamengine.query.config.DataPathConfig;
import com.streamengine.query.domain.ClientRecord;
import com.streamengine.query.domain.DataRecord;
import com.streamengine.query.domain.ItemRecord;
import com.streamengine.query.domain.OrderRecord;
import com.streamengine.query.domain.QueryResult;
import com.streamengine.query.processor.ClientFilter;
import com.streamengine.query.processor.ItemJoiner;
import com.streamengine.query.processor.OrderJoiner;
import com.streamengine.query.sink.AsyncWriter;
import com.streamengine.query.sink.ResultWriter;
import com.streamengine.query.util.KeySelectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main engine class for executing the stream query processing job.
 * Sets up the Flink streaming environment and orchestrates the processing pipeline.
 */
public class QueryEngine {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEngine.class);
    
    public static void main(String[] args) throws Exception {
        // Initialize Flink streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Create file source
        String inputPath = DataPathConfig.getInstance().getInputFilePath();
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
                .build();
        DataStream<String> dataStream = env.fromSource(source, 
            WatermarkStrategy.noWatermarks(), "File Source");
        
        // Route data stream into separate streams
        SingleOutputStreamOperator<DataRecord> mainDataStream = dataStream
                .process(new StreamRouter())
                .name("Data Router")
                .setParallelism(1);
        
        // Extract individual entity streams from the split output
        DataStream<ClientRecord> clientStream = mainDataStream.getSideOutput(StreamRouter.CLIENT_TAG);
        DataStream<OrderRecord> orderStream = mainDataStream.getSideOutput(StreamRouter.ORDER_TAG);
        DataStream<ItemRecord> itemStream = mainDataStream.getSideOutput(StreamRouter.ITEM_TAG);
        
        // Initialize key selectors
        KeySelectors.ClientKeySelector clientKeySelector = new KeySelectors.ClientKeySelector();
        KeySelectors.OrderKeySelector orderKeySelector = new KeySelectors.OrderKeySelector();
        KeySelectors.ItemKeySelector itemKeySelector = new KeySelectors.ItemKeySelector();
        
        // Process client data - filter for target market segment
        DataStream<ClientRecord> processedClientStream = clientStream
                .keyBy(clientKeySelector)
                .filter(new ClientFilter())
                .name("Client Filter");
        
        // Join client and order data
        DataStream<OrderRecord> processedOrderStream = processedClientStream
                .connect(orderStream)
                .keyBy(clientKeySelector, orderKeySelector)
                .process(new OrderJoiner())
                .name("Order Joiner");
        
        // Join order and item data
        DataStream<ItemRecord> processedItemStream = processedOrderStream
                .connect(itemStream)
                .keyBy(orderKeySelector, itemKeySelector)
                .process(new ItemJoiner())
                .name("Item Joiner");
        
        // Aggregate data to calculate revenue
        DataStream<QueryResult> resultStream = processedItemStream
                .keyBy(itemKeySelector)
                .process(new RevenueCalculator())
                .name("Revenue Calculator");
        
        // Output results to sink
        resultStream.addSink(new ResultWriter())
                .name("Result Writer");
        
        // Execute the Flink job
        env.execute("Stream Query Processing Engine");
        LOGGER.info("Stream query processing job completed successfully");
        AsyncWriter.getInstance().stop();
    }
}

