package com.streamengine.query.sink;

import com.streamengine.query.config.DataPathConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronously writes results to a file using a background thread.
 */
public class AsyncWriter implements java.io.Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AsyncWriter.class);
    
    private final BlockingQueue<String> dataQueue;
    private final String outputFilePath;
    private final AtomicBoolean isRunning;
    private Thread writerThread;
    private PrintWriter printWriter;
    
    private static final String HEADER = "l_orderkey, o_orderdate, o_shippriority, revenue";
    
    private static AsyncWriter instance;
    
    private AsyncWriter() {
        this.dataQueue = new LinkedBlockingQueue<>(10000);
        this.outputFilePath = DataPathConfig.getInstance().getOutputFilePath();
        this.isRunning = new AtomicBoolean(false);
    }
    
    public static synchronized AsyncWriter getInstance() {
        if (instance == null) {
            instance = new AsyncWriter();
        }
        return instance;
    }
    
    public synchronized void start() throws IOException {
        if (isRunning.get()) {
            LOG.warn("Async writer thread already started");
            return;
        }
        
        printWriter = new PrintWriter(new FileWriter(outputFilePath, false));
        printWriter.println(HEADER);
        printWriter.flush();
        
        isRunning.set(true);
        writerThread = new Thread(this::writeLoop, "Async-File-Writer");
        writerThread.start();
        LOG.info("Async file writer thread started, output path: {}", outputFilePath);
    }
    
    public void enqueue(String data) {
        if (!isRunning.get()) {
            LOG.error("Async writer thread not started, cannot add data");
            return;
        }
        
        try {
            dataQueue.put(data);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Adding data to queue was interrupted", e);
        }
    }
    
    public void stop() {
        if (!isRunning.get()) {
            return;
        }
        
        isRunning.set(false);
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Waiting for writer thread to finish was interrupted", e);
            }
        }
        
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
        }
        LOG.info("Async file writer thread stopped");
    }
    
    private void writeLoop() {
        while (isRunning.get() || !dataQueue.isEmpty()) {
            try {
                String data = dataQueue.poll();
                if (data != null) {
                    printWriter.println(data);
                    if (dataQueue.size() % 100 == 0) {
                        printWriter.flush();
                    }
                } else {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Writer thread was interrupted");
                break;
            } catch (Exception e) {
                LOG.error("Failed to write to file", e);
            }
        }
        if (printWriter != null) {
            printWriter.flush();
        }
    }
}

