package design.dfs.backup.fs;

import design.dfs.common.annotation.VisibleForTesting;
import design.dfs.common.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  FsImage 检查点
 *
 *  检查点 checkpoint 负责将内存目录树进行持久化存储到磁盘。
 *
 */
@Slf4j
public class FsImageCheckpointTask implements Runnable{

    @Override
    public void run() {
        log.info("BackupNode start checkpoint thread");
        try {

        } catch (Exception e) {
            log.error("FsImageCheckpointTask error:", e);
        }
    }

    /**
     * 写入 FsImage 文件
     */
    @VisibleForTesting
    public void doCheckpoint(FsImage fsImage, String path) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.toByteArray());
        FileUtil.saveFile(path, true, buffer);
        log.info("保存FsImage文件：[file={}]", path);
    }
}
