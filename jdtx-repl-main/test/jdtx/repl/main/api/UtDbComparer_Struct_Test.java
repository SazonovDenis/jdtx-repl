package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 */
public class UtDbComparer_Struct_Test {

    @Test
    public void test_compare() throws Exception {
        //
        IJdxDbStruct structDiffCommon;
        IJdxDbStruct structDiffNew;
        IJdxDbStruct structDiffRemoved;
        //
        JdxDbStruct_XmlRW dbStruct_XmlRW = new JdxDbStruct_XmlRW();
        IJdxDbStruct structActual = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structActual.xml");
        IJdxDbStruct structFixed = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFixed.xml");

        //
        structDiffCommon = new JdxDbStruct();
        structDiffNew = new JdxDbStruct();
        structDiffRemoved = new JdxDbStruct();
        UtDbComparer.getStructDiff(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println("=== getStructDiff ===");
        System.out.println("structNew:");
        //
        for (IJdxTable table : structDiffNew.getTables()) {
            System.out.println("  " + table.getName());
        }
        //
        System.out.println("structRemoved:");
        for (IJdxTable table : structDiffRemoved.getTables()) {
            System.out.println("  " + table.getName());
        }


        //
        structDiffCommon = new JdxDbStruct();
        structDiffNew = new JdxDbStruct();
        structDiffRemoved = new JdxDbStruct();
        UtDbComparer.getStructDiffTables(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println();
        System.out.println("=== getStructDiffTables ===");
        //
        System.out.println("structNew:");
        for (IJdxTable table : structDiffNew.getTables()) {
            System.out.println("  " + table.getName());
        }
        //
        System.out.println("structRemoved:");
        for (IJdxTable table : structDiffRemoved.getTables()) {
            System.out.println("  " + table.getName());
        }
    }


}
