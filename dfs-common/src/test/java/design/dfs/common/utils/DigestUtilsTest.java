package design.dfs.common.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertNotNull;

public class DigestUtilsTest {
    @Test
    public void testMd5Hex() {
        String str = DigestUtils.md5Hex("" + System.nanoTime() + new Random().nextInt());
        assertNotNull(str);
    }
}
