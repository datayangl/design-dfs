package design.dfs.common.network.file;

import design.dfs.common.utils.FileUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * 文件存储
 */
@Slf4j
@Data
public class FileAppender {
    private File file;
    private FileAttribute fileAttribute;
    private int readLength;
    private FileChannel fileChannel;
    private FileOutputStream fos;
    private FileTransportCallback fileTransportCallback;
    private long startTransportTime = -1;

    public FileAppender(FileAttribute fileAttribute, FileTransportCallback fileTransportCallback) throws IOException {
        this.fileAttribute = fileAttribute;
        this.readLength = 0;
        this.fileTransportCallback = fileTransportCallback;
        this.file = new File(fileTransportCallback.getPath(fileAttribute.getFilename()));
        FileUtil.mkdirParent(file.getAbsolutePath());
        this.fos = new FileOutputStream(file, false);
        this.fileChannel = fos.getChannel();
        this.fileChannel.position(0);
        this.startTransportTime = System.currentTimeMillis();
        this.fileAttribute.setAbsolutePath(file.getAbsolutePath());
        log.info("文件传输开始：[filename={}]", this.fileAttribute.getFilename());
    }

    /**
     * 数据包写入文件
     * @param data
     * @throws IOException
     */
    public void append(byte[] data) throws IOException {
        this.fileChannel.write(ByteBuffer.wrap(data));
        this.readLength += data.length;
        // 计算 已传输数据在总数据量中的占比(百分比)，方便显示进度
        float p = new BigDecimal(String.valueOf(readLength))
                .multiply(new BigDecimal(100))
                .divide(new BigDecimal(String.valueOf(fileAttribute.getSize())), 2, RoundingMode.HALF_UP)
                .floatValue();
        if (log.isDebugEnabled()) {
            log.debug("文件传输进度：[filename={}，progress={}]", fileAttribute.getFilename(), p);
        }
        fileTransportCallback.onProgress(fileAttribute.getFilename(), fileAttribute.getSize(), readLength, p, data.length);
    }

    /**
     * 文件传输完成
     */
    public void completed() throws IOException, InterruptedException {
        String md5 = FileUtil.fileMd5(file.getAbsolutePath());
        boolean success = this.fileAttribute.getMd5().equals(md5);
        if (!success) {
            throw new IllegalStateException("文件损坏.");
        }
        fileTransportCallback.onCompleted(fileAttribute);
        log.info("文件传输完成：[filename={}]", fileAttribute.getFilename());
    }

    /**
     * 释放资源
     */
    public void release() {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                log.error("关闭channel失败：", e);
            }
        }
        if (fos != null) {
            try {
                fos.close();
            } catch (IOException e) {
                log.error("关闭channel失败：", e);
            }
        }
    }

    /**
     * 判断文件传输是否超时
     * @return
     */
    public boolean isTimeout() {
        return System.currentTimeMillis() > (startTransportTime + 30 * 60 * 1000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileAppender that = (FileAppender) o;
        return Objects.equals(fileAttribute.getId(), that.fileAttribute.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fileAttribute.getId());
    }
}