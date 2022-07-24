package design.dfs.datanode.server;

import design.dfs.common.FileInfo;
import design.dfs.common.utils.StringUtil;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.datanode.server.locate.FileLocator;
import design.dfs.datanode.server.locate.FileLocatorFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 存储管理器
 */
@Slf4j
public class StorageManager {
    private static final String STORAGE_INFO = "storage.info";
    private static final String STORAGE_TEMP = "storage.temp";
    private static final int HASH_SIZE = 256;
    private String storageDir;
    private FileLocator fileLocator;

    public StorageManager(DataNodeConfig dataNodeConfig) {
        this.storageDir = dataNodeConfig.getBaseDir() + "/storage";
        this.fileLocator = FileLocatorFactory.getFileLocator(dataNodeConfig.getFileLocatorType(),
                storageDir, HASH_SIZE);
        this.initStorage(storageDir);
    }

    /**
     * 初始化存储目录
     *
     * @param baseDir 基础目录
     */
    private void initStorage(String baseDir) {
        File file = new File(baseDir);
        if (file.exists()) {
            return;
        }
        log.info("开始初始化存储文件目录: [baseDir={}]", baseDir);
        for (int i = 0; i < HASH_SIZE; i++) {
            for (int j = 0; j < HASH_SIZE; j++) {
                String parent = StringUtil.format(i);
                String child = StringUtil.format(j);
                File tar = new File(file, parent + "/" + child);
                if (!tar.mkdirs()) {
                    throw new IllegalStateException("初始化存储目录失败: " + tar.getAbsolutePath());
                }
            }
        }
        log.info("初始化存储文件目录成功...");
    }

    /**
     * 报告存储信息
     */
    public StorageInfo getStorageInfo() {
        StorageInfo storageInfo = new StorageInfo();
        File fileDir = new File(storageDir);
        storageInfo.setFreeSpace(fileDir.getFreeSpace());
        log.info("DataNode文件存储路径：[file={}]", fileDir.getAbsolutePath());
        List<FileInfo> fileInfos = scanFile(fileDir);
        long storageSize = 0L;
        for (FileInfo fileInfo : fileInfos) {
            storageSize += fileInfo.getFileSize();
        }
        storageInfo.setStorageSize(storageSize);
        storageInfo.setFiles(fileInfos);
        return storageInfo;
    }

    /**
     * 扫描本地文件
     *
     * @param dir 目录
     */
    private List<FileInfo> scanFile(File dir) {
        List<FileInfo> fileInfos = new LinkedList<>();

        try {
            for (int i = 0; i < HASH_SIZE; i++)
                for (int j = 0; j < HASH_SIZE; j++) {
                    String parent = String.format("%03d", i);
                    String child = String.format("%03d", j);
                    File storageInfoFile =  new File(dir + File.separator + parent + child + File.separator + STORAGE_INFO);
                    if (!storageInfoFile.exists()) {
                        continue;
                    }
                    List<FileInfo> currentFolderFile = new ArrayList<>();
                    try (FileInputStream fis = new FileInputStream(storageInfoFile); FileChannel fileChannel = fis.getChannel()){
                        ByteBuffer byteBuffer = ByteBuffer.allocate((int) storageInfoFile.length());
                        fileChannel.read(byteBuffer);
                        byteBuffer.flip();
                        while (byteBuffer.hasRemaining()) {
                            try {
                                // fileName length
                                int filenameBytesLength = byteBuffer.getInt();
                                // file size
                                long fileSize = byteBuffer.getLong();
                                byte[] fileNameBytes = new byte[filenameBytesLength];
                                byteBuffer.get(fileNameBytes);
                                String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);
                                if (fileExists(fileName)) {
                                    FileInfo fileInfo = new FileInfo();
                                    fileInfo.setFileName(fileName);
                                    fileInfo.setFileSize(fileSize);
                                    fileInfos.add(fileInfo);
                                    currentFolderFile.add(fileInfo);
                                }
                            } catch (Exception e) {
                                log.error("Parse storageInfo failed.", e);
                                System.exit(0);
                            }
                        }
                    }
                }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return fileInfos;
    }

    private boolean fileExists(String fileName) {
        String absolutePathByFileName = getAbsolutePathByFileName(fileName);
        File file = new File(absolutePathByFileName);
        return file.exists();
    }

    /**
     * 根据文件名获取本地绝对路径
     *
     * @param fileName 文件名
     * @return 本地绝对路径
     */
    public String getAbsolutePathByFileName(String fileName) {
        return fileLocator.locate(fileName);
    }
}
