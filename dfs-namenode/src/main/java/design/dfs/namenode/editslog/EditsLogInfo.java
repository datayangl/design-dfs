package design.dfs.namenode.editslog;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * editslog文件信息
 */
@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EditsLogInfo implements Comparable<EditsLogInfo> {
    private long start;
    private long end;
    private String name;

    @Override
    public int compareTo(EditsLogInfo o) {
        return (int) (this.start - o.start);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EditsLogInfo that = (EditsLogInfo) o;
        return start == that.start &&
                end == that.end &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, name);
    }
}
