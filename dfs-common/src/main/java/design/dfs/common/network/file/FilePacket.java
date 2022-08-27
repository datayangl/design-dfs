package design.dfs.common.network.file;

import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * 文件传输包
 *
 * 数据结构: 包类型(4字节) + 元数据长度(4字节) + 元数据(m字节) + body长度(4字节) + body(n字节)
 *
 * 文件传输顺醋: FilePacket(HEAD) + FilePacket(BODY) * n + FilePacket(TAIL)
 */
@Slf4j
@Data
@Builder
public class FilePacket {
    /**
     * 包类型，文件传输前的请求头，文件传输内容包、文件传输完成后的结尾包
     */
    public static final Integer HEAD = 1;
    public static final Integer BODY = 2;
    public static final Integer TAIL = 3;

    /**
     * 包类型
     */
    private int type;

    /**
     * 文件元数据，key-value对
     * 例如：filename=/aaa/bbb/ccc.png,md5=xxxxxxx,crc32=xxxx,size=xxx
     */
    private Map<String, String> fileMetaData;

    /**
     * 文件内容
     */
    private byte[] body;

    /**
     * 转换为ByteBuf
     */
    public byte[] toBytes() {
        String metaDataString = null;
        int lengthOfMetaData = 0;
        if (fileMetaData != null && !fileMetaData.isEmpty()) {
            // fileMetaData(Map类型) 转字符串
            metaDataString = Joiner.on("&").withKeyValueSeparator("=").join(fileMetaData);
            lengthOfMetaData = metaDataString.getBytes().length;
        }

        int lengthOfBody = body == null ? 0 : body.length;
        ByteBuf buffer = Unpooled.buffer(4 + 4 + lengthOfMetaData + 4 + lengthOfBody);
        buffer.writeInt(type);
        buffer.writeInt(lengthOfMetaData);
        if (lengthOfMetaData > 0) {
            buffer.writeBytes(metaDataString.getBytes());
        }
        buffer.writeInt(lengthOfBody);
        if (lengthOfBody > 0) {
            buffer.writeBytes(body);
        }
        return buffer.array();
    }

    /**
     * 将字节数组解包成网络包
     */
    public static FilePacket parseFrom(byte[] bytes) {
        ByteBuf byteBuf = Unpooled.copiedBuffer(bytes);
        FilePacketBuilder builder = FilePacket.builder();
        int type = byteBuf.readInt();
        builder.type(type);
        int lengthOfFileMetaData = byteBuf.readInt();
        if (lengthOfFileMetaData > 0) {
            byte[] metaDataBytes = new byte[lengthOfFileMetaData];
            byteBuf.readBytes(metaDataBytes, 0, lengthOfFileMetaData);
            String fileMetaData = new String(metaDataBytes, 0, lengthOfFileMetaData);
            StringTokenizer st = new StringTokenizer(fileMetaData, "&");
            Map<String, String> metaData = new HashMap<>(2);
            int i;
            while (st.hasMoreTokens()) {
                String s = st.nextToken();
                i = s.indexOf("=");
                String name = s.substring(0, i);
                String value = s.substring(i + 1);
                metaData.put(name, value);
            }
            builder.fileMetaData(metaData);
        }
        int lengthOfBody = byteBuf.readInt();
        if (lengthOfBody > 0) {
            byte[] body = new byte[lengthOfBody];
            byteBuf.readBytes(body, 0, lengthOfBody);
            builder.body = body;
        }
        return builder.build();
    }
}
