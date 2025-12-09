package cn.mqtty.broker.constants;

import io.netty.util.AttributeKey;

public class ChannelAttrs {
    public static final AttributeKey<Boolean> DISCONNECT_FLAG =
            AttributeKey.valueOf("DISCONNECT_FLAG");

}
