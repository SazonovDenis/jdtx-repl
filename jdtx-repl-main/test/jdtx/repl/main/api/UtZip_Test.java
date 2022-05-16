package jdtx.repl.main.api;

import jandcode.utils.test.*;
import jdtx.repl.main.api.*;
import org.junit.*;

import java.io.*;
import java.util.*;

public class UtZip_Test extends UtilsTestCase {

    @Test
    public void test_putToZip() throws Exception {
        logOn();

        File[] fIn = {
                new File("../_test-data/_test-data_srv/db1.gdb"),
                new File("../_test-data/_test-data_ws2/db2.gdb"),
                new File("../_test-data/_test-data_ws3/db3.gdb"),
                new File("d:/install/jdk-1.8.0_202.zip")
        };

        File destZipFile = new File("../_test-data/test.zip");

        //
        UtZip.doZipFiles(Arrays.asList(fIn), destZipFile);

        //
        System.out.println("destZipFile: " + destZipFile.getCanonicalPath());
    }

    @Test
    public void test_doUnzipDir() throws Exception {
        UtZip.doUnzipDir("../_test-data/test.zip", "../_test-data/test.zip.dir/");
    }

}
