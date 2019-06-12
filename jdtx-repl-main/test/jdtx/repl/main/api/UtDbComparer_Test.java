package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 */
public class UtDbComparer_Test {

    @Test
    public void test_compare() throws Exception {
        //
        IJdxDbStruct structDiffCommon;
        IJdxDbStruct structDiffNew;
        IJdxDbStruct structDiffRemoved;
        //
        UtDbStruct_XmlRW dbStruct_XmlRW = new UtDbStruct_XmlRW();
        IJdxDbStruct structActual = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structActual.xml");
        IJdxDbStruct structFixed = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFixed.xml");

        //
        structDiffCommon = new JdxDbStruct();
        structDiffNew = new JdxDbStruct();
        structDiffRemoved = new JdxDbStruct();
        UtDbComparer.dbStructDiff(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println("=== dbStructDiff ===");
        System.out.println("structNew:");
        //
        for (IJdxTableStruct table : structDiffNew.getTables()) {
            System.out.println("  " + table.getName());
        }
        //
        System.out.println("structRemoved:");
        for (IJdxTableStruct table : structDiffRemoved.getTables()) {
            System.out.println("  " + table.getName());
        }


        //
        structDiffCommon = new JdxDbStruct();
        structDiffNew = new JdxDbStruct();
        structDiffRemoved = new JdxDbStruct();
        UtDbComparer.dbStructDiffTables(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println();
        System.out.println("=== dbStructDiffTables ===");
        //
        System.out.println("structNew:");
        for (IJdxTableStruct table : structDiffNew.getTables()) {
            System.out.println("  " + table.getName());
        }
        //
        System.out.println("structRemoved:");
        for (IJdxTableStruct table : structDiffRemoved.getTables()) {
            System.out.println("  " + table.getName());
        }
    }


}
