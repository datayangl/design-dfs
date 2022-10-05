package design.dfs.datanode.server;

import design.dfs.common.network.file.FileAttribute;
import design.dfs.common.network.file.FileTransportCallback;
import design.dfs.datanode.namenode.NameNodeClient;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * 默认文件传输回调
 */
@Slf4j
public class DefaultFileTransportCallback implements FileTransportCallback {
    private NameNodeClient nameNodeClient;
    private StorageManager storageManager;

    public DefaultFileTransportCallback(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.nameNodeClient = nameNodeClient;
    }

    @Override
    public String getPath(String filename) {
        String localFileName = storageManager.getAbsolutePathByFileName(filename);
        log.info("获取文件路径文件：[filename={}, location={}]", filename, localFileName);
        return localFileName;
    }

    @Override
    public void onCompleted(FileAttribute fileAttribute) throws InterruptedException, IOException {
        nameNodeClient.informReplicaReceived(fileAttribute.getFilename(), fileAttribute.getSize());

        //.info("文件下载完成");
    }

    @Override
    public void onProgress(String filename, long total, long current, float progress, int currentWriteBytes) {
    }
}
