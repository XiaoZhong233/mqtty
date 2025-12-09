/**
 * Copyright (c) 2018, Mr.Wang (recallcode@aliyun.com) All rights reserved.
 */

package cn.mqtty.common.session;


import cn.mqtty.broker.msg.WillData;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 会话存储
 */
public class SessionStore implements Serializable {
    private static final long serialVersionUID = -1L;

    private String brokerId;

    private String clientId;

    private String channelId;

    private int expire;

    private boolean cleanSession;

    private WillData willMessage;

    public SessionStore() {

    }

    public SessionStore(String brokerId, String clientId, String channelId, boolean cleanSession, WillData willMessage, int expire) {
        this.brokerId = brokerId;
        this.clientId = clientId;
        this.channelId = channelId;
        this.cleanSession = cleanSession;
        this.willMessage = willMessage;
        this.expire = expire;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }

    public String getClientId() {
        return clientId;
    }

    public SessionStore setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public SessionStore setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    public WillData getWillMessage() {
        return willMessage;
    }

    public SessionStore setWillMessage(WillData willMessage) {
        this.willMessage = willMessage;
        return this;
    }
}
