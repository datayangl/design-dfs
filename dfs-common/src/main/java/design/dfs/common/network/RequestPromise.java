package design.dfs.common.network;

import design.dfs.common.enums.PacketType;
import design.dfs.common.exception.RequestTimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * 同步请求
 *
 *  {@link #setResult(NettyPacket)} 和 {@link #markTimeout()} 操作结束需要调用 {@link #notifyAll()}} 避免线程一直阻塞
 *
 */
@Slf4j
public class RequestPromise {
    private NettyPacket request;
    private NettyPacket response;
    private final long startTime;
    private boolean timeout;
    private volatile boolean receiveResponseCompleted = false;

    public RequestPromise(NettyPacket request) {
        this.request = request;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 获取响应结果
     *
     */
    public NettyPacket getResult() throws RequestTimeoutException {
        waitForResult();
        return response;
    }

    /**
     * 结果返回
     *
     * @param nettyPacket nettyPacket
     */
    public void setResult(NettyPacket nettyPacket) {
        synchronized (this) {
            this.response = nettyPacket;
            this.receiveResponseCompleted = true;
            notifyAll();
        }
    }
    /**
     * 轮询等待相应结果
     * @throws RequestTimeoutException
     */
    public void waitForResult() throws RequestTimeoutException{
        synchronized (this) {
            try {
                while (!receiveResponseCompleted && !timeout) {
                    wait(10);
                }

                if (timeout) {
                    if (log.isDebugEnabled()) {
                        log.debug("同步请求超时: [cost={} s, request={}, sequence={}]",
                                (System.currentTimeMillis() - startTime) / 1000L,
                                (PacketType.getEnum(request.getPacketType()).getDescription()),
                                request.getSequence());
                    }
                    throw new RequestTimeoutException("请求超时: " +
                            PacketType.getEnum(request.getPacketType()).getDescription());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("同步请求成功了：[cost={} s, request={}, sequence={}]",
                                (System.currentTimeMillis() - startTime) / 1000.0D,
                                PacketType.getEnum(response.getPacketType()).getDescription(),
                                response.getSequence());
                    }
                }
            } catch (InterruptedException e) {
                log.info("NettyPackageWrapper#waitForResult is interrupt !");
            }
        }
    }

    public void markTimeout() {
        if (this.timeout) {
            return;
        }
        timeout = true;
        notifyAll();
    }

    public boolean isTimeout(long timeout) {
        if (this.timeout) {
            return true;
        }
        long now = System.currentTimeMillis();
        long timeoutInMs = request.getTimeoutInMs();
        if (timeoutInMs < 0) {
            return false;
        }

        if (timeoutInMs > 0) {
            return startTime + timeoutInMs < now;
        } else {
            return startTime + timeout < now;
        }
    }
}
