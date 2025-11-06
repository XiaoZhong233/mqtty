/**
 * Copyright (c) 2018, Mr.Wang (recallcode@aliyun.com) All rights reserved.
 */

package cn.mqtty.broker.protocol;


import cn.hutool.core.util.StrUtil;
import cn.mqtty.amqp.InternalMessage;
import cn.mqtty.amqp.RelayService;
import cn.mqtty.broker.config.BrokerProperties;
import cn.mqtty.broker.msg.ActionMsg;
import cn.mqtty.common.message.*;
import cn.mqtty.common.session.ISessionStoreService;
import cn.mqtty.common.session.SessionStore;
import cn.mqtty.common.subscribe.ISubscribeStoreService;
import cn.mqtty.common.subscribe.SubscribeStore;
import cn.mqtty.service.evt.DeviceActionEvt;
import cn.mqtty.service.evt.enums.Action;
import cn.mqtty.service.impl.MqttLoggerService;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * PUBLISH连接处理
 */
@Component
@Slf4j
public class Publish {

//    private static final Logger LOGGER = LoggerFactory.getLogger(Publish.class);
    private final MqttLoggerService loggerService;

    private ISessionStoreService sessionStoreService;

    private ISubscribeStoreService subscribeStoreService;

    private IMessageIdService messageIdService;

    private IRetainMessageStoreService retainMessageStoreService;

    private IDupPublishMessageStoreService dupPublishMessageStoreService;

    private RelayService relayService;

    private ChannelGroup channelGroup;

    private Map<String, ChannelId> channelIdMap;

    private BrokerProperties brokerProperties;

    private final ApplicationContext applicationContext;

    ObjectMapper mapper = new ObjectMapper();

    public Publish(ISessionStoreService sessionStoreService, ISubscribeStoreService subscribeStoreService, IMessageIdService messageIdService, IRetainMessageStoreService retainMessageStoreService, IDupPublishMessageStoreService dupPublishMessageStoreService,
                   ChannelGroup channelGroup, Map<String, ChannelId> channelIdMap, BrokerProperties brokerProperties,
                   MqttLoggerService mqttLoggerService, RelayService relayService, ApplicationContext applicationContext) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.messageIdService = messageIdService;
        this.retainMessageStoreService = retainMessageStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
//        this.internalCommunication = internalCommunication;
        this.channelGroup = channelGroup;
        this.channelIdMap = channelIdMap;
        this.brokerProperties = brokerProperties;
        this.loggerService = mqttLoggerService;
        this.relayService = relayService;
        this.applicationContext = applicationContext;
    }

    public void processPublish(Channel channel, MqttPublishMessage msg) {
        String clientId = (String) channel.attr(AttributeKey.valueOf("clientId")).get();
        // publish 延长session失效时间
        if (sessionStoreService.containsKey(clientId)) {
            SessionStore sessionStore = sessionStoreService.get(clientId);
            ChannelId channelId = channelIdMap.get(sessionStore.getBrokerId() + "_" + sessionStore.getChannelId());
            if (brokerProperties.getId().equals(sessionStore.getBrokerId()) && channelId != null) {
                sessionStoreService.expire(clientId, sessionStore.getExpire());
            }
        }
        String topicName = msg.variableHeader().topicName();
        String sn = "Unknown";
        if(!topicName.startsWith("$action/operation")){
            Object o = channel.attr(AttributeKey.valueOf("sn")).get();
            if(clientId.startsWith("dev-reg-service")){
                sn = "平台";
            }
            if(o!=null){
                sn = (String) o;
            }
            log.info("[PUBLISH] sn:[{}]-clientId: [{}], channel:[{}], topic: {}, Qos: {}", sn,clientId, channel.id(),
                    msg.variableHeader().topicName(), msg.fixedHeader().qosLevel().value());
        }
        // QoS=0
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE) {
            byte[] messageBytes = new byte[msg.payload().readableBytes()];
            msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
            InternalMessage internalMessage = new InternalMessage(sn, clientId, topicName, msg.fixedHeader().qosLevel().value()
            , messageBytes);
            //二进制报文通过MQ转发
            if(topicName.startsWith("$remote/client2server")|| topicName.startsWith("$log/operation")){
                relayService.send(internalMessage);
            }
            this.sendPublishMessage(msg.variableHeader().topicName(), msg.fixedHeader().qosLevel(), messageBytes, false, false);
        }
        // QoS=1
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            byte[] messageBytes = new byte[msg.payload().readableBytes()];
            msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
            //特殊处理登录报文
            if(topicName.startsWith("$action/operation")){
                try {
                    ActionMsg actionMsg = mapper.readValue(messageBytes, ActionMsg.class);
                    if(StrUtil.isNotBlank(actionMsg.getSn())){
                        if(actionMsg.getMsg().get("action").asInt()==1){
                            applicationContext.publishEvent(new DeviceActionEvt(clientId, actionMsg.getSn(), channel, Action.ONLINE, actionMsg));
                            channel.attr(AttributeKey.valueOf("sn")).set(actionMsg.getSn());
                        } else if (actionMsg.getMsg().get("action").asInt()==0) {
                            applicationContext.publishEvent(new DeviceActionEvt(clientId, actionMsg.getSn(), channel, Action.OFFLINE, actionMsg));
                        }
                    }
                }catch (Exception e){
                    log.error("设备登录报文解析错误! clientId:{}, 错误原因: {}, ", clientId, e.getMessage(), e);
                }
            }

            if(topicName.startsWith("$msg/push")){
                try {
                    ActionMsg actionMsg = mapper.readValue(messageBytes, ActionMsg.class);
                    if(StrUtil.isNotBlank(actionMsg.getSn())){
                        applicationContext.publishEvent(new DeviceActionEvt(clientId, actionMsg.getSn(), channel, Action.PUSH_MSG, actionMsg));
                    }
                }catch (Exception e){
                    log.error("设备个推报文解析错误! clientId:{}, 错误原因: {}, ", clientId, e.getMessage(), e);
                }
            }

//            internalCommunication.internalSend(internalMessage);
            this.sendPublishMessage(msg.variableHeader().topicName(), msg.fixedHeader().qosLevel(), messageBytes, false, false);
            this.sendPubAckMessage(channel, msg.variableHeader().packetId());
        }
        // QoS=2
        if (msg.fixedHeader().qosLevel() == MqttQoS.EXACTLY_ONCE) {
            byte[] messageBytes = new byte[msg.payload().readableBytes()];
            msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
            this.sendPublishMessage(msg.variableHeader().topicName(), msg.fixedHeader().qosLevel(), messageBytes, false, false);
            this.sendPubRecMessage(channel, msg.variableHeader().packetId());
        }
        // retain=1, 保留消息
        if (msg.fixedHeader().isRetain()) {
            byte[] messageBytes = new byte[msg.payload().readableBytes()];
            msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
            if (messageBytes.length == 0) {
                retainMessageStoreService.remove(msg.variableHeader().topicName());
            } else {
                RetainMessageStore retainMessageStore = new RetainMessageStore().setTopic(msg.variableHeader().topicName()).setMqttQoS(msg.fixedHeader().qosLevel().value())
                        .setMessageBytes(messageBytes);
                retainMessageStoreService.put(msg.variableHeader().topicName(), retainMessageStore);
            }
        }
    }

    public void sendPublishMessage(String topic, MqttQoS mqttQoS, byte[] messageBytes, boolean retain, boolean dup) {
        List<SubscribeStore> subscribeStores = subscribeStoreService.search(topic);
        subscribeStores.parallelStream().forEach(subscribeStore -> {
            if (sessionStoreService.containsKey(subscribeStore.getClientId())) {
                // 订阅者收到MQTT消息的QoS级别, 最终取决于发布消息的QoS和主题订阅的QoS
                MqttQoS respQoS = mqttQoS.value() > subscribeStore.getMqttQoS() ? MqttQoS.valueOf(subscribeStore.getMqttQoS()) : mqttQoS;
                if (respQoS == MqttQoS.AT_MOST_ONCE) {
                    MqttPublishMessage publishMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.PUBLISH, dup, respQoS, retain, 0),
                            new MqttPublishVariableHeader(topic, 0), Unpooled.buffer().writeBytes(messageBytes));
//                    loggerService.info("PUBLISH - clientId: {}, topic: {}, Qos: {}", subscribeStore.getClientId(), topic, respQoS.value());
                    SessionStore sessionStore = sessionStoreService.get(subscribeStore.getClientId());
                    ChannelId channelId = channelIdMap.get(sessionStore.getBrokerId() + "_" + sessionStore.getChannelId());
                    if (channelId != null) {
                        Channel channel = channelGroup.find(channelId);
                        if (channel != null) channel.writeAndFlush(publishMessage);
                    }
                }
                if (respQoS == MqttQoS.AT_LEAST_ONCE) {
                    int messageId = messageIdService.getNextMessageId();
                    MqttPublishMessage publishMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.PUBLISH, dup, respQoS, retain, 0),
                            new MqttPublishVariableHeader(topic, messageId), Unpooled.buffer().writeBytes(messageBytes));
//                    loggerService.info("PUBLISH - clientId: {}, topic: {}, Qos: {}, messageId: {}", subscribeStore.getClientId(), topic, respQoS.value(), messageId);
                    DupPublishMessageStore dupPublishMessageStore = new DupPublishMessageStore().setClientId(subscribeStore.getClientId())
                            .setTopic(topic).setMqttQoS(respQoS.value()).setMessageBytes(messageBytes).setMessageId(messageId);
                    dupPublishMessageStoreService.put(subscribeStore.getClientId(), dupPublishMessageStore);
                    SessionStore sessionStore = sessionStoreService.get(subscribeStore.getClientId());
                    ChannelId channelId = channelIdMap.get(sessionStore.getBrokerId() + "_" + sessionStore.getChannelId());
                    if (channelId != null) {
                        Channel channel = channelGroup.find(channelId);
                        if (channel != null) channel.writeAndFlush(publishMessage);
                    }
                }
                if (respQoS == MqttQoS.EXACTLY_ONCE) {
                    int messageId = messageIdService.getNextMessageId();
                    MqttPublishMessage publishMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.PUBLISH, dup, respQoS, retain, 0),
                            new MqttPublishVariableHeader(topic, messageId), Unpooled.buffer().writeBytes(messageBytes));
//                    loggerService.info("PUBLISH - clientId: {}, topic: {}, Qos: {}, messageId: {}", subscribeStore.getClientId(), topic, respQoS.value(), messageId);
                    DupPublishMessageStore dupPublishMessageStore = new DupPublishMessageStore().setClientId(subscribeStore.getClientId())
                            .setTopic(topic).setMqttQoS(respQoS.value()).setMessageBytes(messageBytes).setMessageId(messageId);
                    dupPublishMessageStoreService.put(subscribeStore.getClientId(), dupPublishMessageStore);
                    SessionStore sessionStore = sessionStoreService.get(subscribeStore.getClientId());
                    ChannelId channelId = channelIdMap.get(sessionStore.getBrokerId() + "_" + sessionStore.getChannelId());
                    if (channelId != null) {
                        Channel channel = channelGroup.find(channelId);
                        if (channel != null) channel.writeAndFlush(publishMessage);
                    }
                }
            }
        });
    }

    private void sendPubAckMessage(Channel channel, int messageId) {
        MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId), null);
        channel.writeAndFlush(pubAckMessage);
    }

    private void sendPubRecMessage(Channel channel, int messageId) {
        MqttMessage pubRecMessage = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId), null);
        channel.writeAndFlush(pubRecMessage);
    }

}
