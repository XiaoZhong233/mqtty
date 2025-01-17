package cn.mqtty.broker;

import cn.mqtty.broker.codec.MqttWebSocketCodec;
import cn.mqtty.broker.config.BrokerProperties;
import cn.mqtty.broker.config.Cert;
import cn.mqtty.broker.handler.BrokerHandler;
import cn.mqtty.broker.handler.IdleReadStateHandler;
import cn.mqtty.broker.handler.MqttWsCustomHandler;
import cn.mqtty.broker.handler.OptionalSslHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.nutz.lang.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class BrokerServer {
//    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServer.class);
    @Autowired
    private BrokerProperties brokerProperties;
    @Autowired
    BrokerHandler brokerHandler;
    @Autowired
    IdleReadStateHandler idleReadStateHandler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private SslContext sslContext;
    private Channel channel;
    private Channel websocketChannel;
    @PostConstruct
    public void init() throws IOException {
        log.info("Initializing {} MQTT Broker ...", "[" + brokerProperties.getId() + "]");
        bossGroup = brokerProperties.isUseEpoll() ? new EpollEventLoopGroup(brokerProperties.getBossGroup_nThreads()) : new NioEventLoopGroup(brokerProperties.getBossGroup_nThreads());
        workerGroup = brokerProperties.isUseEpoll() ? new EpollEventLoopGroup(brokerProperties.getWorkerGroup_nThreads()) : new NioEventLoopGroup(brokerProperties.getWorkerGroup_nThreads());
    }

    public void start() throws Exception {
        if (brokerProperties.isSslEnabled()) {
            sslContext = Cert.build();
        }
        mqttServer();
        if (brokerProperties.isWebsocketEnabled()) {
            websocketServer();
            log.info("MQTT Broker {} is up and running. Open Port: {} WebSocketPort: {}", "[" + brokerProperties.getId() + "]", brokerProperties.getPort(), brokerProperties.getWebsocketPort());
        } else {
            log.info("MQTT Broker {} is up and running. Open Port: {} ", "[" + brokerProperties.getId() + "]", brokerProperties.getPort());
        }
    }

    public void stop() {
        log.info("Shutdown {} MQTT Broker ...", "[" + brokerProperties.getId() + "]");
        bossGroup.shutdownGracefully();
        bossGroup = null;
        workerGroup.shutdownGracefully();
        workerGroup = null;
        channel.closeFuture().syncUninterruptibly();
        channel = null;
        websocketChannel.closeFuture().syncUninterruptibly();
        websocketChannel = null;
        log.info("MQTT Broker {} shutdown finish.", "[" + brokerProperties.getId() + "]");
    }

    private void mqttServer() throws Exception {
        ServerBootstrap sb = new ServerBootstrap();
        sb.group(bossGroup, workerGroup)
                .channel(brokerProperties.isUseEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                // handler在初始化时就会执行
                .handler(new LoggingHandler(LogLevel.WARN))
                // childHandler会在客户端成功connect后才执行
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline channelPipeline = socketChannel.pipeline();
                        // Netty提供的SSL处理
//                        if (brokerProperties.isSslEnabled()) {
//                            SSLEngine sslEngine = sslContext.newEngine(socketChannel.alloc());
//                            sslEngine.setUseClientMode(false);
//                            sslEngine.setNeedClientAuth(true);
//                            channelPipeline.addLast("ssl", new SslHandler(sslEngine));
//                        }
                        channelPipeline.addLast("optionalSSL", new OptionalSslHandler(sslContext));

                        // Netty提供的心跳检测
                        socketChannel.pipeline().addLast("idleStateHandler", new IdleStateHandler(0,0,
                                        brokerProperties.getKeepAlive(), TimeUnit.SECONDS))
                                .addLast(idleReadStateHandler);

                        channelPipeline.addLast("decoder", new MqttDecoder());
                        channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                        channelPipeline.addLast("broker", brokerHandler);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, brokerProperties.getSoBacklog())
                .option(ChannelOption.SO_RCVBUF, 10 * 1024 * 1024)  // 设置接收缓冲区大小为 10MB
                .option(ChannelOption.SO_SNDBUF, 10 * 1024 * 1024)  // 设置发送缓冲区大小为 10MB
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(4 * 1024 * 1024, 10 * 1024 * 1024))
//                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(3*1024 * 1024, 5*1024 * 1024))
                .childOption(ChannelOption.SO_KEEPALIVE, brokerProperties.isSoKeepAlive())
                .childOption(ChannelOption.SO_RCVBUF, 10 * 1024 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 10 * 1024 * 1024);
//                .childOption(ChannelOption.TCP_NODELAY, true);
        if (Strings.isNotBlank(brokerProperties.getHost())) {
            channel = sb.bind(brokerProperties.getHost(), brokerProperties.getPort()).sync().channel();
        } else {
            channel = sb.bind(brokerProperties.getPort()).sync().channel();
        }
    }

    private void websocketServer() throws Exception {
        ServerBootstrap sb = new ServerBootstrap();
        sb.group(bossGroup, workerGroup)
                .channel(brokerProperties.isUseEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                // handler在初始化时就会执行
                .handler(new LoggingHandler(LogLevel.DEBUG))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline channelPipeline = socketChannel.pipeline();
                        // Netty提供的心跳检测
                        channelPipeline.addFirst("idle", new IdleStateHandler(0, 0, brokerProperties.getKeepAlive()));
                        // Netty提供的SSL处理
//                        channelPipeline.addLast("optionalSSL", new OptionalSslHandler(sslContext));
                        // 将请求和应答消息编码或解码为HTTP消息
                        channelPipeline.addLast("http-codec", new HttpServerCodec());
                        // 将HTTP消息的多个部分合成一条完整的HTTP消息
                        channelPipeline.addLast("aggregator", new HttpObjectAggregator(1048576));
                        // 将HTTP消息进行压缩编码
                        channelPipeline.addLast("compressor ", new HttpContentCompressor());
                        channelPipeline.addLast("protocol", new WebSocketServerProtocolHandler(brokerProperties.getWebsocketPath(), "mqtt,mqttv3.1,mqttv3.1.1", true, 65536));
                        channelPipeline.addLast("mqttWebSocket", new MqttWebSocketCodec());
                        channelPipeline.addLast("decoder", new MqttDecoder());
                        channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                        channelPipeline.addLast("mqtt-over-websocket-custom-layer",  new MqttWsCustomHandler());
                        channelPipeline.addLast("broker", brokerHandler);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, brokerProperties.isSoKeepAlive());
        if (Strings.isNotBlank(brokerProperties.getHost())) {
            websocketChannel = sb.bind(brokerProperties.getHost(), brokerProperties.getWebsocketPort()).sync().channel();
        } else {
            websocketChannel = sb.bind(brokerProperties.getWebsocketPort()).sync().channel();
        }
    }
}
