package design.dfs.common.utils;

/**
 * 字节工具类
 */
public class ByteUtil {

    public static void setInt(byte[] bytes, int index, int value) {
        bytes[index] = (byte) (value >>> 24);
        bytes[index + 1] = (byte) (value >>> 16);
        bytes[index + 2] = (byte) (value >>> 8);
        bytes[index + 3] = (byte) value;
    }

    public static void setLong(byte[] bytes, int index, long value) {
        bytes[index] = (byte) (value >>> 56);
        bytes[index + 1] = (byte) (value >>> 48);
        bytes[index + 2] = (byte) (value >>> 40);
        bytes[index + 3] = (byte) (value >>> 32);
        bytes[index + 4] = (byte) (value >>> 24);
        bytes[index + 5] = (byte) (value >>> 16);
        bytes[index + 6] = (byte) (value >>> 8);
        bytes[index + 7] = (byte) value;
    }

    public static int getInt(byte[] bytes, int index) {
        return  (bytes[index]     & 0xff) << 24 |
                (bytes[index + 1] & 0xff) << 16 |
                (bytes[index + 2] & 0xff) <<  8 |
                bytes[index + 3] & 0xff;
    }

    static long getLong(byte[] bytes, int index) {
        return  ((long) bytes[index]     & 0xff) << 56 |
                ((long) bytes[index + 1] & 0xff) << 48 |
                ((long) bytes[index + 2] & 0xff) << 40 |
                ((long) bytes[index + 3] & 0xff) << 32 |
                ((long) bytes[index + 4] & 0xff) << 24 |
                ((long) bytes[index + 5] & 0xff) << 16 |
                ((long) bytes[index + 6] & 0xff) <<  8 |
                (long) bytes[index + 7] & 0xff;
    }


}
