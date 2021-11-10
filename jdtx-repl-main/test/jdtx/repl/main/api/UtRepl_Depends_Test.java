package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class UtRepl_Depends_Test extends ReplDatabaseStruct_Test {

    String delim = "\n  ";

    @Test
    public void test_getTablesDependsOn() throws Exception {
        IJdxTable tableMain = struct.getTable("pawnChit");

        //
        List<IJdxTable> res0 = UtJdx.getTablesDependsOn(tableMain, true);
        //
        System.out.println();
        System.out.print(tableMain.getName());
        for (IJdxTable table : res0) {
            System.out.print(delim + table.getName());
        }
        System.out.println();

        //
        List<IJdxTable> res1 = UtJdx.getDependTables(struct, tableMain, true);
        //
        System.out.println();
        System.out.print(tableMain.getName());
        for (IJdxTable table : res1) {
            System.out.print(delim + table.getName());
        }
        System.out.println();
    }

    @Test
    public void test_getTablesDependsOn_() throws Exception {
        delim = ",";
        test_getTablesDependsOn();
        System.out.println();
    }

}
