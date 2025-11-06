package cn.mqtty.service.impl;

import io.netty.channel.Channel;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class BindSNService {

    // 保存设备SN与Channel的对应关系
    private final ConcurrentMap<String, Channel> snChannelMap = new ConcurrentHashMap<>();

    /**
     * 绑定SN和Channel（设备上线）
     */
    public void bind(String sn, Channel channel) {
        if (sn != null && channel != null) {
            snChannelMap.put(sn, channel);
        }
    }

    /**
     * 解绑（设备下线）
     */
    public void unbind(String sn) {
        if (sn != null) {
            snChannelMap.remove(sn);
        }
    }

    /**
     * 获取设备对应的Channel
     */
    public Channel getChannel(String sn) {
        return sn == null ? null : snChannelMap.get(sn);
    }

    /**
     * 判断设备是否在线
     */
    public boolean isOnline(String sn) {
        Channel channel = getChannel(sn);
        return channel != null && channel.isActive();
    }

    /**
     * 获取所有在线设备数量
     */
    public int onlineCount() {
        return (int) snChannelMap.values().stream()
                .filter(Channel::isActive)
                .count();
    }
}
