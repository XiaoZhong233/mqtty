package cn.mqtty.service;

import cn.mqtty.broker.msg.ActionMsg;
import io.netty.channel.Channel;

public interface DeviceService {

    void online(Channel channel, String sn, String clientId);

    void offline(Channel channel, String sn, String clientId);

    boolean isOnline(String sn);

    void messagePush(Channel channel, String sn, ActionMsg msg);
}
