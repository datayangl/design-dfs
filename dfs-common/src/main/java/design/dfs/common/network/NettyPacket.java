package design.dfs.common.network;

import com.google.protobuf.InvalidProtocolBufferException;
import design.dfs.common.enums.PacketType;
import design.dfs.model.common.NettyPacketHeader;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * 网络数据封装
 *
 *          NettyPacket数据格式
 *  +--------------+-------------------------+---------------+-----------------------------+
 *  | HeaderLength | Actual Header (18byte)  | ContentLength | Actual Content (25byte)     |
 *  | 0x0012       | Header Serialization    | 0x0019        | Body  Serialization         |
 *  +--------------+-------------------------+---------------+-----------------------------+
 *
 */
@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NettyPacket {
    /**
     * 消息体
     */
    protected byte[] body;

    /**
     * 请求头
     *
     * error ->
     * packetType ->
     * nodeId ->
     * ack ->
     * supportChunked ->
     * sequence ->
     */
    private Map<String, String> header;

    public static NettyPacket copy(NettyPacket nettyPacket) {
        return new NettyPacket(nettyPacket.getBody(), new HashMap<>(nettyPacket.getHeader()));
    }

    /**
     * 设置请求序列号
     *
     * @param sequence 请求序列号
     */
    public void setSequence(String sequence) {
        if (sequence != null) {
            header.put("sequence", sequence);
        }
    }

    /**
     * 获取请求序列号
     */
    public String getSequence() {
        return header.get("sequence");
    }

    /**
     * 请求包类型
     *
     * @return 请求包类型
     */
    public int getPacketType() {
        return Integer.parseInt(header.getOrDefault("packetType", "0"));
    }

    /**
     * 设置请求包类型
     *
     * @param packetType 请求包类型
     */
    public void setPacketType(int packetType) {
        header.put("packetType", String.valueOf(packetType));
    }

    public void setUsername(String username) {
        header.put("username", username);
    }

    public String getUserName() {
        return header.getOrDefault("username", "");
    }

    public void setError(String error) {
        header.put("error", error);
    }

    public boolean isSuccess() {
        return getError() == null;
    }

    public boolean isError() {
        return !isSuccess();
    }

    public String getError() {
        return header.getOrDefault("error", null);
    }

    public void setNodeId(int nodeId) {
        header.put("nodeId", String.valueOf(nodeId));
    }

    public int getNodeId() {
        String nodeId = header.getOrDefault("nodeId", "-1");
        return Integer.parseInt(nodeId);
    }

    public void setAck(int ack) {
        header.put("ack", String.valueOf(ack));
    }

    public int getAck() {
        String ack = header.getOrDefault("ack", "0");
        return Integer.parseInt(ack);
    }

    public void setTimeoutInMs(long timeoutInMs) {
        header.put("timeoutInMs", String.valueOf(timeoutInMs));
    }

    public long getTimeoutInMs() {
        return Long.parseLong(header.getOrDefault("timeoutInMs", "0"));
    }

    /**
     * 创建网络请求通用请求
     *
     * @param body        body
     * @param packetType 请求类型
     * @return 请求
     */
    public static NettyPacket buildPacket(byte[] body, PacketType packetType) {
        NettyPacketBuilder builder = NettyPacket.builder();
        builder.body = body;
        builder.header = new HashMap<>();
        NettyPacket nettyPacket = builder.build();
        nettyPacket.setPacketType(packetType.value);
        return nettyPacket;
    }

    /**
     * 将数据写入ByteBuf
     *
     * @param out 输出
     */
    public void write(ByteBuf out) {
        NettyPacketHeader nettyPackageHeader = NettyPacketHeader.newBuilder()
                .putAllHeaders(header)
                .build();
        byte[] headerBytes = nettyPackageHeader.toByteArray();
        out.writeInt(headerBytes.length);
        out.writeBytes(headerBytes);
        out.writeInt(body.length);
        out.writeBytes(body);
    }


    /**
     * 解包
     *
     * @param byteBuf 内存缓冲区
     * @return netty网络包
     */
    public static NettyPacket parsePacket(ByteBuf byteBuf) throws InvalidProtocolBufferException {
        int headerLength = byteBuf.readInt();
        byte[] headerBytes = new byte[headerLength];
        byteBuf.readBytes(headerBytes);
        NettyPacketHeader nettyPackageHeader = NettyPacketHeader.parseFrom(headerBytes);
        int bodyLength = byteBuf.readInt();
        byte[] bodyBytes = new byte[bodyLength];
        byteBuf.readBytes(bodyBytes);
        return NettyPacket.builder()
                .header(new HashMap<>(nettyPackageHeader.getHeadersMap()))
                .body(bodyBytes)
                .build();
    }
}
