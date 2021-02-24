package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 * todo: 1. Сериализация/десериализация в/из файла
 * todo: 2. Откат результат слияния
 */
public class UtRecMerge_Merge_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;

    //String tableName = "Ulz";
    //String tableName = "LicDocTip";
    String tableName = "LicDocVid";
    //String tableName = "Lic";
    String namesStr = "Name";
    //String namesStr = "RNN";

    //
    String[] fieldNames = namesStr.split(",");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //
        db = dbm.getDb();
        System.out.println("db: " + db.getDbSource().getDatabase());
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
    public void test_MakeDuplicatesLoadDuplicates() throws Exception {
        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);


        // =================

        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));

        // =================

        // Ищем дубликаты
        Collection<RecDuplicate> resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Печатаем дубликаты
        int x = 0;
        //
        System.out.println("Duplicates count: " + resList.size());
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
            //
            x = res.records.size();
        }

        // =================

        // Копируем запись
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        long idMax = db.loadSql("select max(id) id from " + tableName).get(0).getValueLong("id");
        DataRecord rec = dbu.loadSqlRec("select * from " + tableName + " where id = :id", UtCnv.toMap("id", idMax));
        rec.setValue("id", null);
        dbu.insertRec(tableName, rec.getValues());

        // =================

        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));

        // =================

        // Снова ищем дубликаты
        resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Снова печатаем дубликаты
        System.out.println("Duplicates count: " + resList.size());
        int x1 = 0;
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
            //
            x1 = res.records.size();
        }
        //
        assertEquals(1, resList.size());
        assertEquals(x1, x + 2);
    }

    @Test
    public void test_execMergeTask() throws Exception {
        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        //
        String tableName = "LicDocTip";
        String namesStr = "Name";

        //
        String[] fieldNames = namesStr.split(",");

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        //
        assertEquals("Найдены дубликаты", true, duplicates.size() > 0);

        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        MergeResultTableMap mergeResults = utRecMerge.execMergePlan(mergeTasks, UtRecMerge.DO_DELETE);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        UtRecMergeReader reader = new UtRecMergeReader();
        reader.writeMergeResilts(mergeResults, "temp/result.json");


        // =================

        // Снова ищем дубликаты
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));
        //
        duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        assertEquals("Найдены дубликаты", false, duplicates.size() > 0);
    }

    @Test
    public void test_makeMergeTask_ToFile() throws Exception {
        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);

        // Сериализация задач
        UtRecMergeReader reader = new UtRecMergeReader();
        reader.writeTasks(mergeTasks, "temp/task.json");

        // Десериализация задач
        Collection<RecMergePlan> mergeTasksFile = reader.readTasks("temp/task.json");
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачи, что прочитали
        UtRecMerge.printTasks(mergeTasksFile);
    }

    @Test
    public void test_execMergeTask_FromFile() throws Exception {
        // Формируем задачу на слияние
        test_makeMergeTask_ToFile();

        // Читаем задачу на слияние
        UtRecMergeReader reader = new UtRecMergeReader();
        Collection<RecMergePlan> mergeTasks = reader.readTasks("temp/task.json");
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        MergeResultTableMap mergeResults = utRecMerge.execMergePlan(mergeTasks, UtRecMerge.DO_DELETE);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        reader = new UtRecMergeReader();
        reader.writeMergeResilts(mergeResults, "temp/result.json");

        // =================

        // Снова ищем дубликаты
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));
        //
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        assertEquals("Найдены дубликаты", false, duplicates.size() > 0);
    }

    @Test
    public void test_execRevertExecTask() throws Exception {
        // Формируем задачу на слияние
        test_makeMergeTask_ToFile();

        // Читаем задачу на слияние
        UtRecMergeReader reader = new UtRecMergeReader();
        Collection<RecMergePlan> mergeTasks = reader.readTasks("temp/task.json");

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        MergeResultTableMap mergeResults = utRecMerge.execMergePlan(mergeTasks, UtRecMerge.DO_DELETE);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        reader = new UtRecMergeReader();
        reader.writeMergeResilts(mergeResults, "temp/result.json");

        // Читаем результат выполнения задачи
        MergeResultTableMap mergeResults1 = reader.readResults("temp/result.json");

        //
        utRecMerge.revertExecMergePlan(mergeResults1);

        // Снова ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        //
        assertEquals("Найдены дубликаты", true, duplicates.size() > 0);
    }


}
