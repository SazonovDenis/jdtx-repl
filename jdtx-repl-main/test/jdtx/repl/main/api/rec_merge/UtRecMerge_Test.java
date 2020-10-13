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
        Collection<RecDuplicate> resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

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
        String tableName = "Ulz";
        //String tableName = "LicDocTip";
        //String tableName = "Lic";
        String namesStr = "Name";
        //String namesStr = "RNN";

        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergeTask> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);

        // Сериализация задач
        UtRecMergeReader reader = new UtRecMergeReader();
        reader.writeTasks(mergeTasks, "../temp/task.json");

        // Десериализация задач
        Collection<RecMergeTask> mergeTasksFile = reader.readTasks("../temp/task.json");

        // Печатаем задачи, что прочитали
        UtRecMerge.printTasks(mergeTasksFile);
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
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergeTask> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        Map<String, MergeResultTable> mergeResults = utRecMerge.execMergeTask(mergeTasks, true);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        UtRecMergeReader reader = new UtRecMergeReader();
        reader.writeResilts(mergeResults, "../temp/result.json");
    }

    @Test
    public void test_execMergeTaskFromFile() throws Exception {
        // Читаем задачу на слияние
        UtRecMergeReader reader = new UtRecMergeReader();
        Collection<RecMergeTask> mergeTasks = reader.readTasks("../temp/task.json");

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Map<String, MergeResultTable> mergeResults = utRecMerge.execMergeTask(mergeTasks, true);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        reader = new UtRecMergeReader();
        reader.writeResilts(mergeResults, "../temp/result.json");
    }

/*
todo:
Сериализация/десериализация в/из файла
  результат слияния
Откат результат слияния
*/

}
