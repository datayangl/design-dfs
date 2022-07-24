package design.dfs.namenode.fs;

import design.dfs.namenode.editslog.EditLogWrapper;

public interface PlaybackEditLogCallback {
    /**
     * 回放日志的回调
     *
     * @param editLogWrapper editLogWrapper
     */
    void playback(EditLogWrapper editLogWrapper);
}
