package cn.mqtty.broker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "mqttwk.broker")
public class BrokerProperties {
    private String id = "mqtt-broker-1"; // Broker唯一标识
    private String host; // 服务启动的IP
    private int port = 8885; // 端口号, 默认8885
    private String username; //用户名
    private String password; //密码
//    private boolean clusterEnabled = false; // 是否开启集群模式
    private int websocketPort = 9995; // WebSocket端口号, 默认9995
    private boolean websocketEnabled = true; // WebSocket是否启用
    private String websocketPath = "/mqtt"; // WebSocket访问路径, 默认/mqtt
    private boolean sslEnabled = true; // SSL是否启用
//    private String sslPassword; // SSL密钥文件密码
    private int keepAlive = 120; // 心跳时间(秒), 默认60秒
    private boolean useEpoll = false; // 是否开启Epoll模式
    private int soBacklog = 511; // Socket参数, 最大队列长度
    private boolean soKeepAlive = true; // 是否开启心跳保活机制
    private String producerTopic = "mqtt_publish"; // MQ转发topic
    private boolean mqttPasswordMust = true; // Connect消息必须通过用户名密码验证
//    private boolean kafkaBrokerEnabled = false; // 是否启用Kafka消息转发
    private int bossGroup_nThreads; // bossGroup线程数
    private int workerGroup_nThreads; // workerGroup线程数
    private boolean amqp_enable;
    private String session_storage_type = "caffeine";
}
