package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.test.*;
import org.junit.*;

import java.io.*;

/**
 */
public class JdxUtils_Test {

    @Test
    public void test_file() throws Exception {
        StopWatch sw = new StopWatch();

        //File f = new File("../_test-data/~tmp_csv.xml ");
        File f = new File("../_test-data/db1.gdb");
        //File f = new File("../_test-data/test.zip");
        System.out.println("file: " + f);

        sw.start();
        System.out.println("md5, JdxUtils.getMd5File: " + JdxUtils.getMd5File(f));
        sw.stop();

        sw.start();
        String etalon = UtFile.loadString(f);
        System.out.println("md5, UtString.md5Str:     " + UtString.md5Str(etalon));
        sw.stop();

        //
        UtFile.saveString(etalon, new File(f.getPath() + ".dat"));
    }

    @Test
    public void test_etalon() throws Exception {
        String etalon = "12345qwert ~~ ЙЦУКЕН = mnvcxzasdfg\thjkiuyt\rrewq !@#$%^&*()_+\u0000wer\u5102\uf102u5----\u00d7\u00d0==";

        File f = new File("../_test-data/etalon.txt");
        System.out.println("file: " + f);

        //
        UtFile.saveString(etalon, f);

        //
        System.out.println("md5, UtString.md5Str:     " + UtString.md5Str(etalon));
        System.out.println("md5, UtFile.loadString:   " + UtString.md5Str(UtFile.loadString(f)));
        System.out.println("md5, JdxUtils.getMd5File: " + JdxUtils.getMd5File(f));
    }

}
