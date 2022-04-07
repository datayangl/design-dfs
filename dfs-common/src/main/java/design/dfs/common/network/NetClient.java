package design.dfs.common.network;

import design.dfs.common.utils.DefaultScheduler;
import design.dfs.common.utils.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
@Slf4j
public class NetClient {
    private BaseChannelInitializer baseChannelInitializer;
    private String name;
    private DefaultScheduler defaultScheduler;
    private EventLoopGroup connectThreadGroup;
    private DefaultChannelHandler defaultChannelHandler;
    private List<NetClientFailListener> netClientFailListeners = new ArrayList<>();
    private int retryTime;

    private AtomicBoolean started = new AtomicBoolean(true);

    public NetClient(String name, DefaultScheduler defaultScheduler) {
        this(name, defaultScheduler, -1, 3000);
    }

    public NetClient(String name, DefaultScheduler defaultScheduler, int retryTime) {
        this(name, defaultScheduler, retryTime, 3000);
    }

    public NetClient(String name, DefaultScheduler defaultScheduler, int retryTime, long requestTimeout) {
        this.name = name;
        this.retryTime = retryTime;
        this.defaultScheduler = defaultScheduler;
        this.connectThreadGroup = new NioEventLoopGroup(1,
                new NamedThreadFactory("NetClient-Event-", false));
        this.defaultChannelHandler = new DefaultChannelHandler(name, defaultScheduler, requestTimeout);
//        this.defaultChannelHandler.addConnectListener(connected -> {
//            if (connected) {
//                synchronized (NetClient.this) {
//                    NetClient.this.notifyAll();
//                }
//            }
//        });
        this.baseChannelInitializer = new BaseChannelInitializer();
        this.baseChannelInitializer.addHandler(defaultChannelHandler);
    }

    /**
     * block until client is connected
     */
    public void ensureConnected() throws InterruptedException {
        ensureConnected(-1);
    }

    /**
     * block until client is connected
     * @param timeout
     * @throws InterruptedException
     */
    public void ensureConnected(int timeout) throws InterruptedException {
        int remainTimeout = timeout;

        synchronized (this) {
            while (!isConnected()) {
                if(!started.get()) {
                    throw new InterruptedException("can't connect to server：" + name);
                }
                if (timeout > 0) {
                    if (remainTimeout <= 0) {
                        throw new InterruptedException("can't connect to server：" + name);
                    }
                    wait(10);
                    remainTimeout -= 10;
                } else {
                    wait(10);
                }
            }
        }
    }


    /**
     * 是否连接上
     *
     * @return 是否已建立了链接
     */
    public boolean isConnected() {
        return defaultChannelHandler.isConnected();
    }

    /**
     * 启动连接
     *
     * @param hostname 主机名
     * @param port     端口
     */
    public void connect(String hostname, int port) {
        connect(hostname, port, 1, 0);
    }

    /**
     * 启动连接
     *
     * @param hostname 主机名
     * @param port     端口
     */
    private void connect(String hostname, int port, final int connectTimes, int delay) {
        defaultScheduler.scheduleOnce("连接服务端", () -> {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(connectThreadGroup)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .channel(NioSocketChannel.class)
                    .handler(baseChannelInitializer);
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
            try {
                ChannelFuture channelFuture = bootstrap.connect(hostname, port).sync();
                channelFuture.channel().closeFuture().addListener((ChannelFutureListener) f -> f.channel().close());
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.info("发起连接后同步等待连接被打断");
            } catch (Exception e) {
                log.error("发起连接过程中出现异常：[ex={}, started={}, name={}]", e.getMessage(), started.get(), name);
            } finally {
                int curConnectTimes = connectTimes + 1;
                maybeRetry(hostname, port, curConnectTimes);
            }
        }, delay);
    }

    /**
     * 尝试重新发起连接
     *
     * @param hostname     主机名
     * @param port         端口号
     * @param connectTimes 当前重试次数
     */
    private void maybeRetry(String hostname, int port, int connectTimes) {
        if (started.get()) {
            boolean retry = retryTime < 0 || connectTimes <= retryTime;
            if (retry) {
                log.error("重新发起连接：[started={}, name={}]", started.get(), name);
                connect(hostname, port, connectTimes, 3000);
            } else {
                shutdown();
                log.info("重试次数超出阈值，不再进行重试：[retryTime={}]", retryTime);
                // TODO 客户端失败处理
//                for (NetClientFailListener listener : new ArrayList<>(netClientFailListeners)) {
//                    try {
//                        listener.onConnectFail();
//                    } catch (Exception e) {
//                        log.error("Exception occur on invoke listener :", e);
//                    }
//                }
            }
        }
    }

    /**
     * 关闭服务，关闭连接、释放资源
     */
    public void shutdown() {
        if(log.isDebugEnabled()) {
            log.debug("Shutdown NetClient : [name={}]", name);
        }
        started.set(false);
        if (connectThreadGroup != null) {
            connectThreadGroup.shutdownGracefully();
        }
        defaultChannelHandler.clearConnectListener();
        defaultChannelHandler.clearNettyPackageListener();
    }

    /**
     * 添加连接状态监听器
     *
     * @param listener 连接监听器
     */
    public void addConnectListener(ConnectListener listener) {
        defaultChannelHandler.addConnectListener(listener);
    }

    /**
     * 添加网络包监听器
     *
     * @param listener 监听器
     */
    public void addNettyPackageListener(NettyPacketListener listener) {
        defaultChannelHandler.addNettyPackageListener(listener);
    }

    public void addNetClientFailListener(NetClientFailListener listener) {
        netClientFailListeners.add(listener);
    }


    /**
     * send request
     *
     * @param nettyPacket
     */
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        ensureConnected();
        defaultChannelHandler.send(nettyPacket);
    }
}
