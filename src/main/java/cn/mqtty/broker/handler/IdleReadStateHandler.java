package cn.mqtty.broker.handler;

import cn.mqtty.broker.protocol.ProtocolProcess;
import cn.mqtty.common.session.SessionStore;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Slf4j
@ChannelHandler.Sharable
@Component
public class IdleReadStateHandler extends ChannelInboundHandlerAdapter {

    @Autowired
    private ProtocolProcess protocolProcess;

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if(evt instanceof IdleStateEvent event){
            if(event.state() == IdleState.ALL_IDLE){
                Channel channel = ctx.channel();
                String clientId = (String) channel.attr(AttributeKey.valueOf("clientId")).get();
                log.info("客户端{}触发空闲事件...即将关闭", clientId);
                channel.close();
            }
        }
    }
}
