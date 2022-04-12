package design.dfs.backup.fs;

import com.google.protobuf.InvalidProtocolBufferException;
import design.dfs.common.utils.ByteUtil;
import design.dfs.common.utils.FileUtil;
import design.dfs.model.backup.INode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 内存目录树快照
 *
 * 文件结构：文件长度(4byte) + 最大txid(8byte) + 文件内容
 *
 *
 */
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
@Data
public class FsImage {
    private static final int LENGTH_OF_FILE_LENGTH_FIELD = 4;
    private static final int LENGTH_OF_MAX_TX_ID_FIELD = 8;

    /**
     * 当前最大的txId
     */
    private long maxTxId;

    /**
     * 目录树节点
     */
    private INode iNode;

    public byte[] toByteArray() {
        byte[] body = iNode.toByteArray();
        int fileLength = LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD + body.length;
        byte[] data = new byte[fileLength];
        ByteUtil.setInt(data, 0, fileLength);
        ByteUtil.setLong(data, LENGTH_OF_FILE_LENGTH_FIELD, maxTxId);
        System.arraycopy(body, 0, data, LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD, body.length);
        return data;
    }

    /**
     * FsImage 解析
     * @param fileChannel 文件channel
     * @param path    文件绝对路径
     * @param length  文件长度
     * @return 如果合法返回 FsImage，不合法返回null
     * @throws IOException IO异常，文件不存在
     */
    public static FsImage parse(FileChannel fileChannel, String path, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD);
        fileChannel.read(buffer);
        buffer.flip();
        if (buffer.remaining() < LENGTH_OF_FILE_LENGTH_FIELD) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return null;
        }
        int fileLength = buffer.getInt();
        if (fileLength != length) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return null;
        } else {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            long maxTxId = buffer.getLong();
            int bodyLength = fileLength - LENGTH_OF_FILE_LENGTH_FIELD - LENGTH_OF_MAX_TX_ID_FIELD;
            buffer = ByteBuffer.allocate(bodyLength);
            fileChannel.read(buffer);
            buffer.flip();
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            INode iNode;
            try {
                iNode = INode.parseFrom(body);
            } catch (InvalidProtocolBufferException e) {
                log.error("Parse FsImage failed.", e);
                return null;
            }

            FsImage fsImage = new FsImage(maxTxId, iNode);
            stopWatch.stop();
            log.info("加载FSImage: [file={}, size={}, maxTxId={}, cost={} s]",
                    path,
                    FileUtil.formatSize(length),
                    fsImage.getMaxTxId(),
                    stopWatch.getTime() / 1000L);
            stopWatch.reset();
            return fsImage;
        }
    }

    /**
     * FsImage 校验
     *
     * @param channel File Channel
     * @param path    文件路径
     * @param length  文件长度
     * @return 如果合法返回MaxTxId, 如果不合法返回-1
     * @throws IOException 文件不存在
     */
    public static long validate(FileChannel channel, String path, int length) throws IOException {
        ByteBuffer buffer =  ByteBuffer.allocate(LENGTH_OF_FILE_LENGTH_FIELD + LENGTH_OF_MAX_TX_ID_FIELD);
        channel.read(buffer);
        buffer.flip();
        if (buffer.remaining() < LENGTH_OF_FILE_LENGTH_FIELD) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return -1L;
        }

        int fileLength = buffer.getInt();
        if (fileLength != length) {
            log.warn("FsImage文件不完整: [file={}]", path);
            return -1L;
        } else {
            return buffer.getLong();
        }
    }
}
