package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class UtRepl_Depends_Test extends ReplDatabaseStruct_Test {

    String delim = "\n  ";
    String tableMainName = "pawnChit";

    @Test
    public void test_getTablesDependsOn() throws Exception {
        IJdxTable table = struct.getTable(tableMainName);

        //
        List<IJdxTable> res0 = UtJdx.getTablesDependsOn(table, true);
        //
        System.out.println();
        System.out.println("Влияющие таблицы");
        System.out.print(table.getName());
        for (IJdxTable tableRes : res0) {
            System.out.print(delim + tableRes.getName());
        }
        System.out.println();

        //
        List<IJdxTable> res1 = UtJdx.getDependTables(struct.getTables(), table, true);
        //
        System.out.println();
        System.out.println("Зависимые таблицы");
        System.out.print(table.getName());
        for (IJdxTable tableRes : res1) {
            System.out.print(delim + tableRes.getName());
        }
        System.out.println();
    }

    @Test
    public void test_getTablesDependsOn_oneLine() throws Exception {
        delim = ",";
        tableMainName = "Lic";
        test_getTablesDependsOn();
        System.out.println();
    }

    @Test
    public void test_getRefsToTable() throws Exception {
        System.out.println("db: " + db.getDbSource().getDatabase());

        tableMainName = "Lic";
        IJdxTable table = struct.getTable(tableMainName);

        // Собираем зависимости
        List<IJdxTable> structTables = struct.getTables();
        Map<String, Collection<IJdxForeignKey>> refsToTable = UtJdx.getRefsToTable(structTables, table, false);
        List<String> refsToTableKeys = UtJdx.getSortedKeys(structTables, refsToTable.keySet());

        // Печатаем зависимости
        System.out.println();
        System.out.println(table.getName());
        //
        for (String tableName : refsToTableKeys) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(tableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();
                System.out.println("  " + tableName + "." + refFieldName + " -> " + fk.getTable().getName());
            }
        }
    }

}
