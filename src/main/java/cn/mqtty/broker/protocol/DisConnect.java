/**
 * Copyright (c) 2018, Mr.Wang (recallcode@aliyun.com) All rights reserved.
 */

package cn.mqtty.broker.protocol;


import cn.mqtty.common.message.IDupPubRelMessageStoreService;
import cn.mqtty.common.message.IDupPublishMessageStoreService;
import cn.mqtty.common.session.ISessionStoreService;
import cn.mqtty.common.session.SessionStore;
import cn.mqtty.common.subscribe.ISubscribeStoreService;
import cn.mqtty.common.subscribe.SubscribeStore;
import cn.mqtty.service.DeviceChannelService;
import cn.mqtty.service.impl.MqttLoggerService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import static cn.mqtty.broker.constants.ChannelAttrs.DISCONNECT_FLAG;


/**
 * DISCONNECT连接处理
 */
@Component
@Slf4j
public class DisConnect {

    private ISessionStoreService sessionStoreService;
    private ISubscribeStoreService subscribeStoreService;
    private IDupPublishMessageStoreService dupPublishMessageStoreService;
    private IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private MqttLoggerService loggerService;
    DeviceChannelService deviceChannelService;
//    private ChannelGroup channelGroup;
//    private Map<String, ChannelId> channelIdMap;

    public DisConnect(ISessionStoreService sessionStoreService, ISubscribeStoreService subscribeStoreService,
                      IDupPublishMessageStoreService dupPublishMessageStoreService,
                      IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                      MqttLoggerService mqttLoggerService, DeviceChannelService deviceChannelService) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.loggerService = mqttLoggerService;
        this.deviceChannelService = deviceChannelService;
//        this.channelGroup = channelGroup;
//        this.channelIdMap = channelIdMap;
    }

    public void processDisConnect(Channel channel) {
        String clientId = (String) channel.attr(AttributeKey.valueOf("clientId")).get();
        SessionStore sessionStore = sessionStoreService.get(clientId);
        channel.attr(DISCONNECT_FLAG).set(Boolean.TRUE);
//        if (sessionStore != null && sessionStore.isCleanSession()) {
//            subscribeStoreService.removeForClient(clientId);
//            dupPublishMessageStoreService.removeByClient(clientId);
//            dupPubRelMessageStoreService.removeByClient(clientId);
//        }
        loggerService.info("DISCONNECT - clientId: {}, cleanSession: {}", clientId, sessionStore.isCleanSession());
//        sessionStoreService.remove(clientId);
        channel.close();
    }


}
