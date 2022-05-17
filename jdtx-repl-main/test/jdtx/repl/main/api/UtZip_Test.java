package jdtx.repl.main.api;

import jandcode.utils.test.*;
import org.junit.*;

import java.io.*;
import java.util.*;

public class UtZip_Test extends UtilsTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        logOn();
    }

    @Test
    public void test_putToZip() throws Exception {
        File[] files = {
                new File("../_test-data/_test-data_srv/db1.gdb"),
                new File("../_test-data/_test-data_ws2/db2.gdb"),
                new File("../_test-data/_test-data_ws3/db3.gdb")
        };

        File destZipFile = new File("../_test-data/test.zip");

        //
        UtZip.doZipFiles(Arrays.asList(files), new File("../_test-data"), destZipFile);

        //
        System.out.println("destZipFile: " + destZipFile.getCanonicalPath());
    }

    @Test
    public void test_doUnzipDir() throws Exception {
        UtZip.doUnzipDir("../_test-data/test.zip", "../_test-data/test.zip.dir/");
    }

    @Test
    public void test_putToZip_FailPath() {
        File[] files = {
                new File("../_test-data/_test-data_srv/db1.gdb"),
                new File("../_test-data/_test-data_ws2/db2.gdb"),
                new File("../_test-data/_test-data_ws3/db3.gdb"),
                new File("d:/install/jdk-1.8.0_202.zip")
        };

        File destZipFile = new File("../_test-data/test0.zip");

        //
        try {
            UtZip.doZipFiles(Arrays.asList(files), new File("../_test-data"), destZipFile);
            throw new Exception("should fail");
        } catch (Exception e) {
            System.out.println("Fail: " + e.getMessage());
        }
    }

    @Test
    public void test_putToZip_FailNotExists() {
        File[] files = {
                new File("../_test-data/_test-data_srv/db1.gdb"),
                new File("../_test-data/_test-data_ws2/db2.gdb"),
                new File("../_test-data/_test-data_ws3/db3.gdb"),
                new File("../_test-data/_test-data_ws3/db999.gdb")
        };

        File destZipFile = new File("../_test-data/test0.zip");

        //
        try {
            UtZip.doZipFiles(Arrays.asList(files), new File("../_test-data"), destZipFile);
            throw new Exception("should fail");
        } catch (Exception e) {
            System.out.println("Fail: " + e.getMessage());
        }
    }

}
