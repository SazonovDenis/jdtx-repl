package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.test.*;
import jdtx.repl.main.api.struct.*;
import junit.framework.*;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtJdx_Test extends TestCase {

    @Test
    public void test_file() throws Exception {
        StopWatch sw = new StopWatch();

        //File f = new File("../_test-data/~tmp_csv.xml ");
        File f = new File("../_test-data/db1.gdb");
        //File f = new File("../_test-data/test.zip");
        System.out.println("file: " + f);

        sw.start();
        System.out.println("md5, JdxUtils.getMd5File: " + UtJdx.getMd5File(f));
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
        System.out.println("md5, JdxUtils.getMd5File: " + UtJdx.getMd5File(f));
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
        //
        IJdxDbStruct structActual = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structActual.xml");
        IJdxDbStruct structFixed = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFixed.xml");

        //
        UtJdx.sortTablesByReference(structActual.getTables());
        UtJdx.sortTablesByReference(structFixed.getTables());

        //
        UtDbComparer.getStructDiff(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println("=== structDiffNew");
        for (IJdxTable table : structDiffNew.getTables()) {
            System.out.println(table.getName());
        }

        //
        List<IJdxTable> structDiffNewSorted = UtJdx.sortTablesByReference(structDiffNew.getTables());
        System.out.println("=== structDiffNewSorted");
        for (IJdxTable table : structDiffNewSorted) {
            System.out.println(table.getName());
        }
    }


    /**
     * Проверяет проблему - сортировка должна быть не только по зависимостям, но также и по алфафиту
     */
    @Test
    public void test_sort_alphabet() throws Exception {
        JdxDbStruct_XmlRW dbStruct_XmlRW = new JdxDbStruct_XmlRW();

        //
        IJdxDbStruct struct1 = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.struct1.xml");
        IJdxDbStruct struct2 = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.struct2.xml");

        // Печатаем список до сортировки
        List<IJdxTable> list1 = struct1.getTables();
        List<IJdxTable> list2 = struct2.getTables();
        for (int i = 0; i < list1.size(); i++) {
            IJdxTable table1 = list1.get(i);
            IJdxTable table2 = list2.get(i);
            System.out.println(table1.getName() + " - " + table2.getName());
        }

        //
        System.out.println("---");

        // Сортируем
        list1 = UtJdx.sortTablesByReference(struct1.getTables());
        list2 = UtJdx.sortTablesByReference(struct2.getTables());

        // Печатаем список после сортировки
        for (int i = 0; i < list1.size(); i++) {
            IJdxTable table1 = list1.get(i);
            IJdxTable table2 = list2.get(i);
            System.out.println(table1.getName() + " - " + table2.getName());
        }

        // Проверяем
        for (int i = 0; i < list1.size(); i++) {
            IJdxTable table1 = list1.get(i);
            IJdxTable table2 = list2.get(i);
            assertEquals(table1.getName(), table2.getName());
        }

        // ===
        System.out.println("======");


        // ===
        // Проверяем сортировку полного списка
        // ===

        struct1 = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFull1.xml");
        struct2 = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFull2.xml");

        // Сортируем
        list1 = UtJdx.sortTablesByReference(struct1.getTables());
        list2 = UtJdx.sortTablesByReference(struct2.getTables());

        // Проверяем
        for (int i = 0; i < list1.size(); i++) {
            IJdxTable table1 = list1.get(i);
            IJdxTable table2 = list2.get(i);
            assertEquals(table1.getName(), table2.getName());
        }
    }


    @Test
    public void test_longValueOf() throws Exception {
        assertEquals(null, UtJdx.longValueOf(null));
        assertEquals(null, UtJdx.longValueOf(""));
        assertEquals(null, UtJdx.longValueOf("null"));
        assertEquals(Long.valueOf(-1), UtJdx.longValueOf("-1"));
        assertEquals(Long.valueOf(-1), UtJdx.longValueOf(-1));

        assertEquals(Long.valueOf(200), UtJdx.longValueOf(null, 200L));
        assertEquals(Long.valueOf(200), UtJdx.longValueOf("", 200L));
        assertEquals(Long.valueOf(200), UtJdx.longValueOf("null", 200L));
        assertEquals(Long.valueOf(-1), UtJdx.longValueOf("-1", 200L));
        assertEquals(Long.valueOf(-1), UtJdx.longValueOf(-1, 200L));
    }


}
