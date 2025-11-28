/**
 * Copyright (c) 2018, Mr.Wang (recallcode@aliyun.com) All rights reserved.
 */

package cn.mqtty.broker.protocol;

import cn.hutool.core.util.StrUtil;
import cn.mqtty.broker.config.BrokerProperties;
import cn.mqtty.broker.handler.enums.ProtocolType;
import cn.mqtty.broker.handler.enums.SslStatus;
import cn.mqtty.common.auth.IAuthService;
import cn.mqtty.common.message.DupPubRelMessageStore;
import cn.mqtty.common.message.DupPublishMessageStore;
import cn.mqtty.common.message.IDupPubRelMessageStoreService;
import cn.mqtty.common.message.IDupPublishMessageStoreService;
import cn.mqtty.common.session.ISessionStoreService;
import cn.mqtty.common.session.SessionStore;
import cn.mqtty.common.subscribe.ISubscribeStoreService;
import cn.mqtty.service.evt.enums.Action;
import cn.mqtty.service.impl.MqttLoggerService;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static cn.mqtty.broker.handler.OptionalSslHandler.PROTOCOL_TYPE_ATTRIBUTE_KEY;
import static cn.mqtty.broker.handler.OptionalSslHandler.SSL_STATUS;

/**
 * CONNECT连接处理
 */
@Component
@Slf4j
public class Connect {

//    private static final Logger LOGGER = LoggerFactory.getLogger(Connect.class);
    private final MqttLoggerService loggerService;

    private final ISessionStoreService sessionStoreService;

    private final ISubscribeStoreService subscribeStoreService;

    private final IDupPublishMessageStoreService dupPublishMessageStoreService;

    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;

    private final IAuthService authService;

    private final BrokerProperties brokerProperties;

    private final ChannelGroup channelGroup;

    private final Map<String, ChannelId> channelIdMap;


    public Connect(ISessionStoreService sessionStoreService, ISubscribeStoreService subscribeStoreService, IDupPublishMessageStoreService dupPublishMessageStoreService, IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                   IAuthService authService, BrokerProperties brokerProperties, ChannelGroup channelGroup,
                   Map<String, ChannelId> channelIdMap, MqttLoggerService mqttLoggerService) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.authService = authService;
        this.brokerProperties = brokerProperties;
        this.channelGroup = channelGroup;
        this.channelIdMap = channelIdMap;
        this.loggerService = mqttLoggerService;
    }

    public void processConnect(Channel channel, MqttConnectMessage msg) {
        // 消息解码器出现异常
        if (msg.decoderResult().isFailure()) {
            Throwable cause = msg.decoderResult().cause();
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // 不支持的协议版本
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // 不合格的clientId
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            }
            channel.close();
            return;
        }
        // clientId为空或null的情况, 这里要求客户端必须提供clientId, 不管cleanSession是否为1, 此处没有参考标准协议实现
        // clientId设备应该传的是sn
        if (StrUtil.isBlank(msg.payload().clientIdentifier())) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false), null);
            channel.writeAndFlush(connAckMessage);
            channel.close();
            return;
        }
        SslStatus sslStatus = channel.attr(SSL_STATUS).get();
        ProtocolType protocolType = channel.attr(PROTOCOL_TYPE_ATTRIBUTE_KEY).get();
        if (protocolType == ProtocolType.WS || (brokerProperties.isMqttPasswordMust() && sslStatus != SslStatus.ENABLED)) {
            // 用户名和密码验证, 这里要求客户端连接时必须提供用户名和密码, 不管是否设置用户名标志和密码标志为1, 此处没有参考标准协议实现
            String username = msg.payload().userName();
            String password = msg.payload().passwordInBytes() == null ? null : new String(msg.payload().passwordInBytes(), CharsetUtil.UTF_8);
            if (!authService.checkValid(username, password, protocolType)) {
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            }
            if(protocolType == ProtocolType.WS){
                log.info("WS客户端连接; channel:{}", channel.id());
            }
        }
        // 如果会话中已存储这个新连接的clientId, 就关闭之前该clientId的连接
        if (sessionStoreService.containsKey(msg.payload().clientIdentifier())) {
            SessionStore sessionStore = sessionStoreService.get(msg.payload().clientIdentifier());
            boolean cleanSession = sessionStore.isCleanSession();
            if (cleanSession) {
                sessionStoreService.remove(msg.payload().clientIdentifier());
                subscribeStoreService.removeForClient(msg.payload().clientIdentifier());
                dupPublishMessageStoreService.removeByClient(msg.payload().clientIdentifier());
                dupPubRelMessageStoreService.removeByClient(msg.payload().clientIdentifier());
            }
            try {
                ChannelId channelId = channelIdMap.get(sessionStore.getBrokerId() + "_" + sessionStore.getChannelId());
                if (channelId != null) {
                    Channel previous = channelGroup.find(channelId);
                    if (previous != null) previous.close();
                }
            } catch (Exception e) {
                //e.printStackTrace();
            }
        } else {
            //如果不存在session，则清除之前的其他缓存
            subscribeStoreService.removeForClient(msg.payload().clientIdentifier());
            dupPublishMessageStoreService.removeByClient(msg.payload().clientIdentifier());
            dupPubRelMessageStoreService.removeByClient(msg.payload().clientIdentifier());
        }
        // 处理连接心跳包，会话过期时间统一设置成120秒
        int expire = Math.round(brokerProperties.getKeepAlive() * 1.5f);
        if (msg.variableHeader().keepAliveTimeSeconds() > 0) {
//            if (channel.pipeline().names().contains("idle")) {
//                channel.pipeline().remove("idle");
//            }
//            expire = Math.round(msg.variableHeader().keepAliveTimeSeconds() * 1.5f);
//            channel.pipeline().addFirst("idle", new IdleStateHandler(0, 0, expire));
        }
        // 处理遗嘱信息
        SessionStore sessionStore = new SessionStore(brokerProperties.getId(), msg.payload().clientIdentifier(), channel.id().asLongText(), msg.variableHeader().isCleanSession(),
                null, expire);
        if (msg.variableHeader().isWillFlag()) {
            MqttPublishMessage willMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(msg.variableHeader().willQos()), msg.variableHeader().isWillRetain(), 0),
                    new MqttPublishVariableHeader(msg.payload().willTopic(), 0), Unpooled.buffer().writeBytes(msg.payload().willMessageInBytes()));
            sessionStore.setWillMessage(willMessage);
        }
        // 至此存储会话信息及返回接受客户端连接
        sessionStoreService.put(msg.payload().clientIdentifier(), sessionStore, expire);
        // 将clientId存储到channel的map中
        channel.attr(AttributeKey.valueOf("clientId")).set(msg.payload().clientIdentifier());
        boolean sessionPresent = sessionStoreService.containsKey(msg.payload().clientIdentifier()) && !msg.variableHeader().isCleanSession();
        MqttConnAckMessage okResp = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent), null);
        channel.writeAndFlush(okResp);
        loggerService.info("CONNECT - channelId:{} clientId: {}, cleanSession: {}", channel.id(), msg.payload().clientIdentifier(), msg.variableHeader().isCleanSession());
        // 如果cleanSession为0, 需要重发同一clientId存储的未完成的QoS1和QoS2的DUP消息
        if (!msg.variableHeader().isCleanSession()) {
            List<DupPublishMessageStore> dupPublishMessageStoreList = dupPublishMessageStoreService.get(msg.payload().clientIdentifier());
            List<DupPubRelMessageStore> dupPubRelMessageStoreList = dupPubRelMessageStoreService.get(msg.payload().clientIdentifier());
            dupPublishMessageStoreList.forEach(dupPublishMessageStore -> {
                MqttPublishMessage publishMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBLISH, true, MqttQoS.valueOf(dupPublishMessageStore.getMqttQoS()), false, 0),
                        new MqttPublishVariableHeader(dupPublishMessageStore.getTopic(), dupPublishMessageStore.getMessageId()), Unpooled.buffer().writeBytes(dupPublishMessageStore.getMessageBytes()));
                channel.writeAndFlush(publishMessage);
            });
            dupPubRelMessageStoreList.forEach(dupPubRelMessageStore -> {
                MqttMessage pubRelMessage = MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBREL, true, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(dupPubRelMessageStore.getMessageId()), null);
                channel.writeAndFlush(pubRelMessage);
            });
        }
    }

}
