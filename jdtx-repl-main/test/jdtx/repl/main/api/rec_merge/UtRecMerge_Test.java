package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtRecMerge_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //
        db = dbm.getDb();
        //
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
    }

    @Test
    public void test_UtRecMergeReader() throws Exception {
        InputStream inputStream = new FileInputStream("test/jdtx/repl/main/api/rec_merge/UtRecMergeTest.xml");
        UtRecMergeReaderXml reader = new UtRecMergeReaderXml(inputStream);
        Map map;
        map = reader.nextRec();
        while (map != null) {
            System.out.println(map);
            map = reader.nextRec();
        }
    }

    @Test
    public void test_loadTableDuplicates() throws Exception {
        //String tableName = "Ulz";
        //String namesStr = "Name,UlzTip";
        //String tableName = "Lic";
        String tableName = "LicDocTip";
        //
        //String namesStr = "NameF,NameI,DocNo";
        //String namesStr = "RNN";
        //String namesStr = "LICDOCVID";
        //String namesStr = "DocNo";
        String namesStr = "Name";
        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> resList = utRecMerge.loadTableDuplicates(tableName, fieldNames);

        // Печатаем дубликаты
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
        }

        //
        System.out.println("Duplicates count: " + resList.size());
    }

    @Test
    public void test_makeMergeTask() throws Exception {
        String tableName = "LicDocTip";
        String namesStr = "Name";

        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.loadTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergeTask> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);

        // Сериализация
        UtRecMergeReader reader = new UtRecMergeReader();
        reader.writeToFile(mergeTasks, "../temp/task.json");

        // Десериализация
        Collection<RecMergeTask> mergeTasksFile = reader.readFromFile("../temp/task.json");

        // Печатаем задачи, что прочистали
        System.out.println("MergeTasks count: " + mergeTasksFile.size());
        System.out.println();
        for (RecMergeTask mergeTask : mergeTasksFile) {
            System.out.println("Table: " + mergeTask.tableName);
            System.out.println("Etalon: " + mergeTask.recordEtalon);
            System.out.println("Delete: " + mergeTask.recordsDelete);
            System.out.println();
        }
    }

    @Test
    public void test_execMergeTask() throws Exception {
        String tableName = "LicDocTip";
        String namesStr = "Name";

        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.loadTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergeTask> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);

        // Печатаем задачу на слияние
        System.out.println("MergeTasks count: " + mergeTasks.size());
        System.out.println();
        for (RecMergeTask mergeTask : mergeTasks) {
            System.out.println("Table: " + mergeTask.tableName);
            System.out.println("Etalon: " + mergeTask.recordEtalon);
            System.out.println("Delete: " + mergeTask.recordsDelete);
            System.out.println();
        }

        // Исполняем задачу на слияние
        Map<String, Map<String, RecMergeResultRefTable>> mergeResults = utRecMerge.execMergeTask(mergeTasks, true);

        // Печатаем результат выполнения задачи
        System.out.println("MergeResults:");
        System.out.println();
        for (String taskTableName : mergeResults.keySet()) {
            Map<String, RecMergeResultRefTable> mergeResult = mergeResults.get(taskTableName);

            for (String refTableName : mergeResult.keySet()) {
                RecMergeResultRefTable mergeResultRec = mergeResult.get(refTableName);
                System.out.println("Ref table: " + mergeResultRec.refTtableName);
                System.out.println("Ref field: " + mergeResultRec.refTtableRefFieldName);
                if (mergeResultRec.recordsUpdated.size() == 0) {
                    System.out.println("RecordsUpdated: empty");
                } else {
                    UtData.outTable(mergeResultRec.recordsUpdated);
                }
                System.out.println();
            }
        }
    }

    @Test
    public void test_execMergeTaskFromFile() throws Exception {
        // Читаем задачу на слияние
        UtRecMergeReader reader = new UtRecMergeReader();
        Collection<RecMergeTask> mergeTasks = reader.readFromFile("../temp/task.json");

        // Печатаем задачу на слияние
        System.out.println("MergeTasks count: " + mergeTasks.size());
        System.out.println();
        for (RecMergeTask mergeTask : mergeTasks) {
            System.out.println("Table: " + mergeTask.tableName);
            System.out.println("Etalon: " + mergeTask.recordEtalon);
            System.out.println("Delete: " + mergeTask.recordsDelete);
            System.out.println();
        }

        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Map<String, Map<String, RecMergeResultRefTable>> mergeResults = utRecMerge.execMergeTask(mergeTasks, true);

        // Печатаем результат выполнения задачи
        System.out.println("MergeResults:");
        System.out.println();
        for (String taskTableName : mergeResults.keySet()) {
            Map<String, RecMergeResultRefTable> mergeResult = mergeResults.get(taskTableName);

            for (String refTableName : mergeResult.keySet()) {
                RecMergeResultRefTable mergeResultRec = mergeResult.get(refTableName);
                System.out.println("Ref table: " + mergeResultRec.refTtableName);
                System.out.println("Ref field: " + mergeResultRec.refTtableRefFieldName);
                if (mergeResultRec.recordsUpdated.size() == 0) {
                    System.out.println("RecordsUpdated: empty");
                } else {
                    UtData.outTable(mergeResultRec.recordsUpdated);
                }
                System.out.println();
            }
        }
    }

/*

Сериализация/десериализация в/из файла
  задачи на слияние
  результат слияния

интерфейс командной строки
  слияние с удалением
  просто слияние без удаления
*/

}
