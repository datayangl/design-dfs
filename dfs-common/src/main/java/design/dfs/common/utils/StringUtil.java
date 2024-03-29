package design.dfs.common.utils;

import java.io.File;
import java.util.Random;

public class StringUtil {
    public static final Random RANDOM = new Random();
    public static final String BASE_KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";


    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length) {
        return getRandomString(length, BASE_KEY);
    }

    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length, boolean upperCase) {
        return getRandomString(length, BASE_KEY, upperCase);
    }

    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length, String key) {
        return getRandomString(length, key, false);
    }

    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length, String key, boolean upperCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = RANDOM.nextInt(key.length());
            sb.append(key.charAt(number));
        }
        String s = sb.toString();
        return upperCase ? s.toUpperCase() : s;
    }

    public static String[] split(String str, char c) {
        return str.split(String.valueOf(c));
//        return org.apache.commons.lang3.StringUtils.split(str, c);
    }


    public static int hash(String source, int maxSize) {
        int hash = toPositive(murmur2(source.getBytes()));
        return hash % maxSize;
    }

    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     *
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    public static boolean validateFileName(String filename) {
        String osName = System.getProperty("os.name").toLowerCase();
        boolean win = osName.startsWith("win");
        if (!win && !filename.startsWith(File.separator)) {
            return false;
        }
        String name = new File(filename).getName();
        if (filename.contains("//")) {
            return false;
        }
        return !name.startsWith(".");
    }

    public static String format(int i) {
        if (i >= 100) {
            return String.valueOf(i);
        }
        if (i >= 10) {
            return "0" + i;
        }
        if (i >= 0) {
            return "00" + i;
        }
        return "";
    }
}
