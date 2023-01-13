package design.dfs.namenode.fs;

import design.dfs.backup.fs.FsImage;
import design.dfs.common.utils.FileUtil;
import design.dfs.namenode.editslog.FsEditLog;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * FsImage 清理任务
 */
@Slf4j
public class FsImageClearTask implements Runnable{
    private String baseDir;
    private AbstractFileSystem fileSystem;
    private FsEditLog fsEditLog;

    public FsImageClearTask(AbstractFileSystem fileSystem, String baseDir) {
        this(fileSystem, baseDir, null);
    }

    public FsImageClearTask( AbstractFileSystem fileSystem, String baseDir, FsEditLog fsEditLog) {
        this.fileSystem = fileSystem;
        this.baseDir = baseDir;
        this.fsEditLog = fsEditLog;
    }

    @Override
    public void run() {
        Map<Long, String> timeFsImageMap = fileSystem.scanFsImageMap(baseDir);
        List<Long> sortedList = new ArrayList<>(timeFsImageMap.keySet());
        sortedList.sort((o1, o2) -> o1.equals(o2) ? 0 : (int) (o2 - o1));
        boolean findValidFsImage = false;
        long maxTxId = -1;
        for (Long time : sortedList) {
            String path = timeFsImageMap.get(time);
            if (findValidFsImage) {
                FileUtil.delete(path);
                log.info("删除FSImage: [file={}]", path);
                continue;
            }

            try (RandomAccessFile raf = new RandomAccessFile(path, "r"); FileInputStream fis =
                    new FileInputStream(raf.getFD()); FileChannel channel = fis.getChannel())  {
                maxTxId = FsImage.validate(channel, path, (int) raf.length());
                if (maxTxId > 0) {
                    findValidFsImage = true;
                    log.info("清除FSImage任务，找到最新的合法的FsImage: [file={}]", path);
                } else {
                    FileUtil.delete(path);
                    log.info("删除FSImage: [file={}]", path);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            // 如果是NameNode，则需要清除EditLog文件
            if (findValidFsImage && fsEditLog != null) {
                //fsEditLog.cleanEditLogByTxId(maxTxId);
            }
        }
    }
}
