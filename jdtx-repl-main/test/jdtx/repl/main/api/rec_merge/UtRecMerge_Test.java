package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.JdxDbUtils;
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


        // =================
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));

        // =================

        // Ищем дубликаты
        Collection<RecDuplicate> resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Печатаем дубликаты
        System.out.println("Duplicates count: " + resList.size());
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
        }


        // =================

        // Копируем запись
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        long idMax = db.loadSql("select max(id) id from " + tableName).get(0).getValueLong("id");
        DataRecord rec = dbu.loadSqlRec("select * from " + tableName + " where id = :id", UtCnv.toMap("id", idMax));
        rec.setValue("id", idMax + 1);
        dbu.insertRec(tableName, rec.getValues());

        // =================
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));

        // =================

        // Снова ищем дубликаты
        resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Снова печатаем дубликаты
        System.out.println("Duplicates count: " + resList.size());
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
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
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergeTask> mergeTasks = utRecMerge.prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        MergeResultTableMap mergeResults = utRecMerge.execMergeTask(mergeTasks, UtRecMerge.DO_DELETE);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        UtRecMergeReader reader = new UtRecMergeReader();
        reader.writeMergeResilts(mergeResults, "../temp/result.json");
    }

    @Test
    public void test_makeMergeTaskToFile() throws Exception {
        //String tableName = "Ulz";
        //String tableName = "LicDocTip";
        String tableName = "LicDocVid";
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
    public void test_execMergeTaskFromFile() throws Exception {
        // Читаем задачу на слияние
        UtRecMergeReader reader = new UtRecMergeReader();
        Collection<RecMergeTask> mergeTasks = reader.readTasks("../temp/task.json");

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        MergeResultTableMap mergeResults = utRecMerge.execMergeTask(mergeTasks, UtRecMerge.DO_DELETE);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        reader = new UtRecMergeReader();
        reader.writeMergeResilts(mergeResults, "../temp/result.json");
    }

    @Test
    public void test_execRevertExecTask() throws Exception {
        // Читаем задачу на слияние
        UtRecMergeReader reader = new UtRecMergeReader();
        Collection<RecMergeTask> mergeTasks = reader.readTasks("../temp/task.json");

        // Печатаем задачу на слияние
        UtRecMerge.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        MergeResultTableMap mergeResults = utRecMerge.execMergeTask(mergeTasks, UtRecMerge.DO_DELETE);

        // Печатаем результат выполнения задачи
        UtRecMerge.printMergeResults(mergeResults);

        // Сохраняем результат выполнения задачи
        reader = new UtRecMergeReader();
        reader.writeMergeResilts(mergeResults, "../temp/result.json");

        // Читаем результат выполнения задачи
        MergeResultTableMap mergeResults1 = reader.readResults("../temp/result.json");

        //
        utRecMerge.revertExecTask(mergeResults1);
    }


/*
todo:
Сериализация/десериализация в/из файла
  результат слияния
Откат результат слияния
*/

}
