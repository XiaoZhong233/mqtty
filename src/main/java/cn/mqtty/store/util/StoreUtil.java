package cn.mqtty.store.util;

import cn.mqtty.broker.msg.WillData;
import cn.mqtty.common.session.SessionStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.nutz.lang.util.NutMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

/**
 * Created by cs on 2018
 */
public class StoreUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreUtil.class);

    public static NutMap transPublishToMapBeta(SessionStore store) {
        try {
            NutMap sessionStore = new NutMap();
            sessionStore.put("clientId", store.getClientId());
            sessionStore.put("channelId", store.getChannelId());
            sessionStore.put("cleanSession", store.isCleanSession());
            sessionStore.put("brokerId", store.getBrokerId());
            sessionStore.put("expire", store.getExpire());
            WillData msg = store.getWillMessage();
            if (null != msg) {
                sessionStore.addv("payload", Base64.getEncoder().encodeToString(msg.getPayload()));
                sessionStore.addv("qosLevel", msg.getQos());
                sessionStore.addv("isRetain", msg.isRetain());
                sessionStore.addv("topicName", msg.getTopic());
                sessionStore.addv("hasWillMessage", true);
            }

            return sessionStore;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }


    public static SessionStore mapTransToPublishMsgBeta(NutMap store) {
        SessionStore sessionStore = new SessionStore();
        if (store.getBoolean("hasWillMessage", false)) {
            WillData willData = new WillData(
                    store.getString("topicName"),
                    Base64.getDecoder().decode(store.getString("payload")),
                    store.getInt("qosLevel"),
                    store.getBoolean("isRetain")
            );
            sessionStore.setWillMessage(willData);
        }
        sessionStore.setChannelId(store.getString("channelId"));
        sessionStore.setClientId(store.getString("clientId"));
        sessionStore.setCleanSession(store.getBoolean("cleanSession"));
        sessionStore.setBrokerId(store.getString("brokerId"));
        sessionStore.setExpire(store.getInt("expire"));
        return sessionStore;
    }
}
