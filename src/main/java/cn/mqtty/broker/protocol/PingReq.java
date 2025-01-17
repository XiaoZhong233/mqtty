/**
 * Copyright (c) 2018, Mr.Wang (recallcode@aliyun.com) All rights reserved.
 */

package cn.mqtty.broker.protocol;


import cn.mqtty.broker.config.BrokerProperties;
import cn.mqtty.common.session.ISessionStoreService;
import cn.mqtty.common.session.SessionStore;
import cn.mqtty.service.impl.MqttLoggerService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * PINGREQ连接处理
 */
@Component
@Slf4j
public class PingReq {

//    private static final Logger LOGGER = LoggerFactory.getLogger(PingReq.class);
    private ISessionStoreService sessionStoreService;
    private BrokerProperties brokerProperties;
    private ChannelGroup channelGroup;
    private Map<String, ChannelId> channelIdMap;
    private MqttLoggerService loggerService;

    public PingReq(ISessionStoreService sessionStoreService, BrokerProperties brokerProperties,
                   ChannelGroup channelGroup, Map<String, ChannelId> channelIdMap,
                   MqttLoggerService mqttLoggerService) {
        this.sessionStoreService = sessionStoreService;
        this.brokerProperties = brokerProperties;
        this.channelGroup = channelGroup;
        this.channelIdMap = channelIdMap;
        this.loggerService = mqttLoggerService;
    }

    public void processPingReq(Channel channel, MqttMessage msg) {
        String clientId = (String) channel.attr(AttributeKey.valueOf("clientId")).get();
        if (sessionStoreService.containsKey(clientId)) {
            SessionStore sessionStore = sessionStoreService.get(clientId);
            ChannelId channelId = channelIdMap.get(sessionStore.getBrokerId() + "_" + sessionStore.getChannelId());
            if (brokerProperties.getId().equals(sessionStore.getBrokerId()) && channelId != null) {
                sessionStoreService.expire(clientId, sessionStore.getExpire());
                MqttMessage pingRespMessage = MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0), null, null);
                log.info("PINGREQ - clientId: {}", clientId);
                channel.writeAndFlush(pingRespMessage);
            }
        }
    }

}
