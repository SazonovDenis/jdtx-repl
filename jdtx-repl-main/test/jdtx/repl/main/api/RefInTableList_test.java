package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.util.*;

public class RefInTableList_test extends ReplDatabaseStruct_Test {

    /**
     * Проверяем, что все таблицы в списке не имеют висячих ссылок.
     * Используется при добавленние в репликацию новых таблиц,
     * чтобы проверить с каких таблиц запросить снэпшоты,
     * да так, чтобы не было битых ссылок.
     */
    @Test
    public void test_TablesRefs() throws Exception {
        // Проверяемый список (типа вновь добавленные в репликацию)
        String tableNamesStrNew = "CommentText,CommentTip,Usr,UsrGrp,UsrOtdel";

        // Существующие справочники (типа те справочники, которые уже ранее запрашивали)
        String tableNamesStrExists = "LicDocTip,LicDocVid,Lic,PawnChitSubject,PawnChit";

        // Разложим в списки
        String[] tableNamesNew = tableNamesStrNew.split(",");
        String[] tableNamesExists = tableNamesStrExists.split(",");
        //
        Collection<IJdxTable> tableListNew = new ArrayList<>();
        for (String tableName : tableNamesNew) {
            IJdxTable table = struct.getTable(tableName);
            tableListNew.add(table);
        }
        //
        Collection<IJdxTable> tableListExists = new ArrayList<>();
        for (String tableName : tableNamesExists) {
            IJdxTable table = struct.getTable(tableName);
            tableListExists.add(table);
        }

        // Проверим
        checkRefInTableList(tableListNew, tableListExists, struct);
    }

    void checkRefInTableList(Collection<IJdxTable> tableListNew, Collection<IJdxTable> tableListExists, IJdxDbStruct struct) {
        for (IJdxTable table : tableListNew) {
            for (IJdxForeignKey fieldFk : table.getForeignKeys()) {
                IJdxTable refTable = fieldFk.getTable();
                String refTableName = refTable.getName();
                String refFieldName = fieldFk.getField().getName();

                //
                boolean refTableNotFound = false;
                if (!tableListNew.contains(refTable) && !tableListExists.contains(refTable)) {
                    refTableNotFound = true;
                }

                //
                if (refTableNotFound) {
                    System.out.println("Not found reference: " + table.getName() + "." + refFieldName + " -> " + refTableName);
                }
            }
        }
    }


}
