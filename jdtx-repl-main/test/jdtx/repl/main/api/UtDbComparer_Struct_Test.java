package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import junit.framework.*;
import org.junit.Test;

import java.util.*;

/**
 *
 */
public class UtDbComparer_Struct_Test extends TestCase {

    @Test
    public void test_compare() throws Exception {
        //
        List<IJdxTable> tablesAdded;
        List<IJdxTable> tablesRemoved;
        List<IJdxTable> tablesChanged;

        //
        JdxDbStruct_XmlRW dbStruct_XmlRW = new JdxDbStruct_XmlRW();
        IJdxDbStruct structActual = dbStruct_XmlRW.read("test/jdtx/repl/main/api/UtDbComparer_Struct_Test.structActual.xml");
        IJdxDbStruct structFixed = dbStruct_XmlRW.read("test/jdtx/repl/main/api/UtDbComparer_Struct_Test.structFixed.xml");

        //
        tablesAdded = new ArrayList<>();
        tablesRemoved = new ArrayList<>();
        tablesChanged = new ArrayList<>();
        UtDbComparer.getStructDiff(structActual, structFixed, tablesAdded, tablesRemoved, tablesChanged);

        //
        System.out.println();
        System.out.println("=== getStructDiff ===");
        //
        System.out.println("tablesAdded:");
        for (IJdxTable table : tablesAdded) {
            System.out.println("  " + table.getName());
        }
        System.out.println("tablesRemoved:");
        for (IJdxTable table : tablesRemoved) {
            System.out.println("  " + table.getName());
        }
        System.out.println("tablesChanged:");
        for (IJdxTable table : tablesChanged) {
            System.out.println("  " + table.getName());
        }

        //
        assertEquals(10, tablesAdded.size());
        assertEquals(4, tablesRemoved.size());
        assertEquals(5, tablesChanged.size());

        //
        tablesAdded = new ArrayList<>();
        tablesRemoved = new ArrayList<>();
        UtDbComparer.getStructDiffTables(structActual, structFixed, tablesAdded, tablesRemoved);

        //
        System.out.println();
        System.out.println("=== getStructDiffTables ===");
        //
        System.out.println("tablesAdded:");
        for (IJdxTable table : tablesAdded) {
            System.out.println("  " + table.getName());
        }
        System.out.println("tablesRemoved:");
        for (IJdxTable table : tablesRemoved) {
            System.out.println("  " + table.getName());
        }

        //
        assertEquals(10, tablesAdded.size());
        assertEquals(4, tablesRemoved.size());
    }

    @Test
    public void test_compare_1() throws Exception {
        JdxDbStruct_XmlRW dbStruct_XmlRW = new JdxDbStruct_XmlRW();

        String fileName = "Z:/jdtx-repl/_test-data/dat.xml";
        IJdxDbStruct struct = dbStruct_XmlRW.read(fileName);
        String crc = UtDbComparer.getDbStructCrcTables(struct);
        System.out.println(fileName);
        System.out.println(crc);
        System.out.println();

        fileName = "Z:/jdtx-repl/_test-data/_test-data_ws2/temp/6.dbStruct.actual.xml";
        struct = dbStruct_XmlRW.read(fileName);
        crc = UtDbComparer.getDbStructCrcTables(struct);
        System.out.println(fileName);
        System.out.println(crc);
        System.out.println();

        fileName = "<empty>";
        struct = new JdxDbStruct();
        crc = UtDbComparer.getDbStructCrcTables(struct);
        System.out.println(fileName);
        System.out.println(crc);
        System.out.println();

    }


}
