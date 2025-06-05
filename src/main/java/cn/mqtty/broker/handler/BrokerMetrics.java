package cn.mqtty.broker.handler;

import cn.mqtty.service.impl.MqttLoggerService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class BrokerMetrics {
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private static final Logger metricLogger = LoggerFactory.getLogger("metricLogger");

    // 接收连接事件
    public void incrementConnection() {
        connectionCount.incrementAndGet();
    }

    public void decrementConnection() {
        connectionCount.decrementAndGet();
    }

    // 接收消息事件
    public void incrementMessage() {
        messageCount.incrementAndGet();
    }

    // 每分钟统计一次 QPS 和连接数
    @PostConstruct
    public void startLoggingTask() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            long totalMessages = messageCount.getAndSet(0); // 清零
            int currentConnections = connectionCount.get();
            double qps = totalMessages / 60.0;
            DecimalFormat df = new DecimalFormat("#.00");
            metricLogger.info("[Metrics] Connections: {}, TotalMsg: {}, Avg QPS: {}",
                    currentConnections, totalMessages,  df.format(qps));
        }, 0, 1, TimeUnit.MINUTES);
    }
}
