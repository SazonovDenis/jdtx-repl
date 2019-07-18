package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.test.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;
import java.util.*;

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

    /**
     * Проверяет проблему, ныне устраненную - некорректная сортировка при разрыве в разреженном списке таблиц
     */
    @Test
    public void test_sort_infinite() throws Exception {
        //
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();
        //
        JdxDbStruct_XmlRW dbStruct_XmlRW = new JdxDbStruct_XmlRW();
        IJdxDbStruct structActual = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structActual.xml");
        IJdxDbStruct structFixed = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFixed.xml");

        //
        JdxUtils.sortTablesByReference(structActual.getTables());
        JdxUtils.sortTablesByReference(structFixed.getTables());

        //
        UtDbComparer.dbStructDiff(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println("=== structDiffNew");
        for (IJdxTable table : structDiffNew.getTables()) {
            System.out.println(table.getName());
        }

        //
        List<IJdxTable> structDiffNewSorted = JdxUtils.sortTablesByReference(structDiffNew.getTables());
        System.out.println("=== structDiffNewSorted");
        for (IJdxTable table : structDiffNewSorted) {
            System.out.println(table.getName());
        }
    }


}
