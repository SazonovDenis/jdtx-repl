package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 */
public class UtDbComparer_Test {

    @Test
    public void test_compare() throws Exception {
        //
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();
        //
        UtDbStruct_XmlRW dbStruct_XmlRW = new UtDbStruct_XmlRW();
        IJdxDbStruct structActual = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structActual.xml");
        IJdxDbStruct structFixed = dbStruct_XmlRW.read("test/jdtx/repl/main/api/JdxUtils_Test.structFixed.xml");

        //
        UtDbComparer.dbStructDiff(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println("=== structDiffNew");
        for (IJdxTableStruct table : structDiffNew.getTables()) {
            System.out.println(table.getName());
        }
        //
        System.out.println("=== structDiffRemoved");
        for (IJdxTableStruct table : structDiffRemoved.getTables()) {
            System.out.println(table.getName());
        }


        //
        //^c сравнение dbStructIsEqual дает ложную разницу в составе таблиц
        System.out.println();
        //UtDbComparer.dbStructIsEqualTables(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        //
        System.out.println("=== structDiffNew");
        for (IJdxTableStruct table : structDiffNew.getTables()) {
            System.out.println(table.getName());
        }
        //
        System.out.println("=== structDiffRemoved");
        for (IJdxTableStruct table : structDiffRemoved.getTables()) {
            System.out.println(table.getName());
        }
    }


}
