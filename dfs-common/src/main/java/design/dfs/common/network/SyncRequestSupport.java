package design.dfs.common.network;

import design.dfs.common.Constants;
import design.dfs.common.enums.PacketType;
import design.dfs.common.exception.RequestTimeoutException;
import design.dfs.common.utils.DefaultScheduler;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 同步请求支持
 */
@Slf4j
public class SyncRequestSupport {
    // 同步请求的集合
    private Map<String, RequestPromise> promiseMap = new ConcurrentHashMap<>();
    private SocketChannel socketChannel;
    private String name;

    public SyncRequestSupport(String name, DefaultScheduler defaultScheduler, long requestTimeout) {
        this.name = name;
        defaultScheduler.schedule("定时超时检测", () -> checkRequestTimeout(requestTimeout), 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    /**
     * 发送同步请求
     * @param request
     * @return
     * @throws RequestTimeoutException
     */
    public NettyPacket sendRequest(NettyPacket request) throws RequestTimeoutException {
        setSequence(request);
        RequestPromise promise = new RequestPromise(request);
        promiseMap.put(request.getSequence(), promise);

        socketChannel.writeAndFlush(request);
        if (log.isDebugEnabled()) {
            log.debug("发送请求并同步等待结果：[request={}, sequence={}]",
                    PacketType.getEnum(request.getPacketType()).getDescription(), request.getSequence());
        }
        return promise.getResult();
    }

    /**
     * 收到响应
     *
     * @param response 响应
     * @return 是否处理消息
     */
    public boolean onResponse(NettyPacket response) {
        String sequence = response.getSequence();
        if (sequence != null) {
            RequestPromise promise = promiseMap.remove(sequence);
            if (promise != null) {
                promise.setResult(response);
                return true;
            }
        }
        return false;
    }

    /**
     * 设置请求的序列号
     *
     * @param request 请求
     */
    private void setSequence(NettyPacket request) {
        if (socketChannel == null || !socketChannel.isActive()) {
            throw new IllegalStateException("Socket channel is disconnect.");
        }
        request.setSequence(name + "-" + Constants.REQUEST_COUNTER.getAndIncrement());
    }

    /**
     * 定时检测请求是否超时，避免超时请求一直阻塞等待
     * @param requestTimeout
     */
    private void checkRequestTimeout(long requestTimeout) {
        synchronized (this) {
            for (Map.Entry<String, RequestPromise> entry : promiseMap.entrySet()) {
                RequestPromise promise = entry.getValue();
                if (promise.isTimeout(requestTimeout)) {
                    promise.markTimeout();
                }
            }
        }
    }
}