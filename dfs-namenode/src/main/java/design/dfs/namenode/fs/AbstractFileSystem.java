package design.dfs.namenode.fs;

import design.dfs.backup.fs.FsImage;
import design.dfs.model.namenode.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

@Slf4j
public abstract class AbstractFileSystem implements FileSystem{
    /**
     * 负责管理内存文件目录树的组件
     */
    protected FsDirectory directory;

    public AbstractFileSystem() {
        this.directory = new FsDirectory();
    }

    /**
     * 基于本地文件恢复元数据空间
     *
     * @throws Exception IO异常
     */
    protected abstract void recoveryNamespace() throws Exception;

    @Override
    public void mkdir(String path, Map<String, String> attr) {
        this.directory.mkdir(path, attr);
    }

    @Override
    public boolean createFile(String filename, Map<String, String> attr) {
        return this.directory.createFile(filename, attr);
    }

    @Override
    public boolean deleteFile(String filename) {
        Node node = this.directory.delete(filename);
        return node != null;
    }

    @Override
    public Set<Metadata> getFilesBySlot(int slot) {
        return null;
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node listFiles(String filename, int level) {
        return this.directory.listFiles(filename, level);
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node listFiles(String filename) {
        return this.directory.listFiles(filename);
    }

    /**
     * <pre>
     *     假设存在文件：
     *
     *     /aaa/bbb/c1.png
     *     /aaa/bbb/c2.png
     *     /bbb/ccc/c3.png
     *
     * 传入：/aaa，则返回：[/bbb/c1.png, /bbb/c2.png]
     *
     * </pre>
     * <p>
     * 返回文件名
     */
    public List<String> findAllFiles(String path) {
        return this.directory.findAllFiles(path);
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node unsafeListFiles(String filename) {
        return this.directory.unsafeListFiles(filename);
    }

    /**
     * 获取文件属性
     *
     * @param filename 文件名称
     * @return 文件属性
     */
    public Map<String, String> getAttr(String filename) {
        Node node = this.directory.listFiles(filename);
        if (node == null) {
            return null;
        }
        return Collections.unmodifiableMap(node.getAttr());
    }

    /**
     * 加载 FsImage 恢复文件系统
     *
     * @param fsImage
     */
    protected void applyFsImage(FsImage fsImage) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        log.info("Starting to apply FsImage file ...");
        directory.applyFsImage(fsImage);
        stopWatch.stop();
        log.info("Apply FsImage File cost {} ms", stopWatch.getTime());
    }

    /**
     * 扫描最新的 FsImage 文件
     *
     * @param baseDir
     * @return
     * @throws IOException
     */
    protected FsImage scanLatestValidFsImage(String baseDir) throws IOException {
        Map<Long, String> timeFsImageMap = scanFsImageMap(baseDir);
        List<Long> sortedList = new ArrayList<>(timeFsImageMap.keySet());
        sortedList.sort((o1, o2) ->
            o1.equals(o2) ? 0 : (int) (o2 - o1)
         );

        for (long time : sortedList) {
            String path = timeFsImageMap.get(time);
            RandomAccessFile raf = new RandomAccessFile(path, "r");
            FileChannel channel = new FileInputStream(raf.getFD()).getChannel();
            FsImage fsImage = FsImage.parse(channel, path, (int) raf.length());
            if (fsImage != null) {
                return fsImage;
            }
        }
        return null;
    }

    /**
     * 扫描 FsImage 目录
     *
     * @param path
     * @return
     */
    public Map<Long, String> scanFsImageMap(String path) {
        Map<Long, String> timeFsImageMap = new HashMap<>(8);
        File dir = new File(path);
        if (!dir.exists()) {
            return timeFsImageMap;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return timeFsImageMap;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            if (!file.getName().contains("fsimage")) {
                continue;
            }
            String str = file.getName().split("-")[1];
            long time = Long.parseLong(str);
            timeFsImageMap.put(time, file.getAbsolutePath());
        }
        return timeFsImageMap;
    }
}