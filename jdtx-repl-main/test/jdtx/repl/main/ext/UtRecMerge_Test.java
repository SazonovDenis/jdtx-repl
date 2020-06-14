package jdtx.repl.main.ext;

import jandcode.app.test.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtRecMerge_Test extends AppTestCase {

    @Test
    public void test_1() throws Exception {
        InputStream inputStream = new FileInputStream("test/jdtx/repl/main/ext/UtRecMergeTest.xml");
        UtRecMergeReaderXml reader = new UtRecMergeReaderXml(inputStream);
        Map map;
        map = reader.nextRec();
        while (map != null) {
            System.out.println(map);
            map = reader.nextRec();
        }
    }

}
