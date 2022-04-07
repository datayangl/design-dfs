package design.dfs.common.utils;

public class StringUtil {
    public static String[] split(String str, char c) {
        return str.split(String.valueOf(c));
//        return org.apache.commons.lang3.StringUtils.split(str, c);
    }
}
