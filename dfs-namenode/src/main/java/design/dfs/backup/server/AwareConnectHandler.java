package design.dfs.backup.server;

import design.dfs.common.network.AbstractChannelHandler;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.utils.ByteUtil;
import design.dfs.common.utils.NetUtil;
import design.dfs.ha.NodeRoleSwitcher;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Set;

/**
 * 自动感知客户端和DataNode连接的处理器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class AwareConnectHandler extends AbstractChannelHandler {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("BackupNode收到一个连接：[channel={}]", NetUtil.getChannelId(ctx.channel()));
        }
        NodeRoleSwitcher.getInstance().addConnect(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("BackupNode移除一个连接：[channel={}]", NetUtil.getChannelId(ctx.channel()));
        }
        NodeRoleSwitcher.getInstance().removeConnect(ctx.channel());
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) {
        // 0表示宕机 1-表示在线
        int status = ByteUtil.getInt(nettyPacket.getBody(), 0);
        NodeRoleSwitcher.getInstance().markNameNodeStatus(status);
        return true;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return Collections.emptySet();
    }
}
