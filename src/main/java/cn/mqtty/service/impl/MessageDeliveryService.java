package cn.mqtty.service.impl;
import cn.mqtty.broker.msg.ActionMsg;
import cn.mqtty.broker.protocol.ProtocolProcess;
import cn.mqtty.service.vo.Message;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Service
public class MessageDeliveryService {

    @Autowired
    private BindSNService relayService;

    @Autowired
    private ProtocolProcess protocolProcess;

    @Autowired
    private StringRedisTemplate redisTemplate;

    ObjectMapper mapper = new ObjectMapper();

    private static final String redisKey = "device:offline:msg:";
    private static final String topicPrefix = "$msg/push/";
    private static final Logger resendLogger = LoggerFactory.getLogger("resendLogger");
    // 本地内存缓存（短期）
//    private final Map<String, Queue<Message<String>>> offlineMsgCache = new ConcurrentHashMap<>();

    public ActionMsg fromJson(String json) {
        try {
            return mapper.readValue(json, ActionMsg.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse ActionMsg JSON", e);
        }
    }

    public byte[] toJson(ActionMsg actionMsg) {
        try {
            return mapper.writeValueAsBytes(actionMsg);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ActionMsg", e);
        }
    }

    /**
     * 投递消息：在线则发送，离线则缓存
     */
    public void deliverMessage(String sn, ActionMsg message) {
        Channel channel = relayService.getChannel(sn);
        if (channel != null && channel.isActive()) {
            // 在线 → 立即发送
            sendToChannel(channel, sn, message);
        } else {
            // 离线 → 缓存
            cacheMessage(sn, message);
        }
    }

    /**
     * 缓存离线消息（同时写入Redis）
     */
    public void cacheMessage(String sn, ActionMsg message) {
//        offlineMsgCache
//                .computeIfAbsent(sn, k -> new ConcurrentLinkedQueue<>())
//                .add(message);

        String key = redisKey + sn;
        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        redisTemplate.opsForList().rightPush(key, jsonString);
        redisTemplate.expire(key, java.time.Duration.ofHours(24));
        log.info("设备{}离线，消息已缓存: {}", sn, jsonString);
        resendLogger.info("设备{}离线，消息已缓存: {}", sn, jsonString);
    }

    /**
     * 当设备上线时调用：重发离线缓存消息
     */
    public void onDeviceOnline(String sn) {
        Channel channel = relayService.getChannel(sn);
//        Queue<Message<String>> queue = offlineMsgCache.get(sn);
//        if (channel == null || !channel.isActive()) return;
//
//        if (queue != null && !queue.isEmpty()) {
//            log.info("设备{}上线，重发离线消息{}条", sn, queue.size());
//            while (!queue.isEmpty()) {
//                Message<String> msg = queue.poll();
//                sendToChannel(channel, sn, msg);
//            }
//        }
        if(channel==null){
            log.info("设备[{}]channel不存在", sn);
            resendLogger.info("设备[{}]channel不存在", sn);
            return;
        }
        // Redis消息恢复
        String key = redisKey + sn;
        List<String> redisMsgs = redisTemplate.opsForList().range(key, 0, -1);
        if (redisMsgs != null && !redisMsgs.isEmpty()) {
            for (String json : redisMsgs) {
                ActionMsg msg = fromJson(json);
                sendToChannel(channel, sn, msg);
            }
            redisTemplate.delete(key);
        }
    }

    /**
     * 发送个推消息到设备通道
     */
    private void sendToChannel(Channel channel, String sn, ActionMsg message) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                false,
                MqttQoS.AT_LEAST_ONCE,
                false,
                0
        );
        String topicName = topicPrefix+sn;
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, 1);
        ByteBuf payload = Unpooled.copiedBuffer(toJson(message));

        MqttPublishMessage publishMessage = new MqttPublishMessage(
                mqttFixedHeader,
                variableHeader,
                payload
        );

        protocolProcess.publish().processPublish(channel, publishMessage);
        log.info("消息已投递给设备{}: {}", sn, message);
        resendLogger.info("消息已投递给设备{}: {}", sn, message);
    }
}
