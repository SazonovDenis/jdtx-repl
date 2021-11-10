package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class UtRecMerge_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;


    //String tableName = "Ulz";
    String tableName = "LicDocTip";
    //String tableName = "LicDocVid";
    //String tableName = "Lic";
    //
    //String namesStr = "Name";
    String namesStr = "Name,ShortName";
    //String namesStr = "RNN";

    //
    Collection<String> fieldNames = Arrays.asList(namesStr.split(","));


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
        //
        logOn();
    }

    @Test
    public void test_MakeDuplicatesLoadDuplicates() throws Exception {
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);


        // =================

        System.out.println("table: " + tableName + ", fields: " + namesStr);

        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));

        // =================

        // Ищем дубликаты
        Collection<RecDuplicate> resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Печатаем дубликаты
        System.out.println("Заданий на слияние: " + resList.size());
        int recordCount0 = 1;
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
            //
            recordCount0 = res.records.size();
        }

        // =================

        System.out.println("Копируем запись");

        // Копируем запись 2 раза
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        long idMax = db.loadSql("select max(id) id from " + tableName).get(0).getValueLong("id");
        DataRecord rec = dbu.loadSqlRec("select * from " + tableName + " where id = :id", UtCnv.toMap("id", idMax));
        rec.setValue("id", null);
        dbu.insertRec(tableName, rec.getValues());
        dbu.insertRec(tableName, rec.getValues());

        // =================

        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));

        // =================

        // Снова ищем дубликаты
        resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Снова печатаем дубликаты
        System.out.println("Заданий на слияние: " + resList.size());
        int recordCount1 = 0;
        for (RecDuplicate res : resList) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
            //
            recordCount1 = res.records.size();
        }
        //
        assertEquals("Есть задание на слияние", true, resList.size() != 0);
        assertEquals("Количество дубликатов", recordCount1, recordCount0 + 2);
    }

    @Test
    public void test_FundDuplicatesUseNull() throws Exception {
        logOff();

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        //
        tableName = "Lic";
        namesStr = "NameF,Rnn,DocSer";
        String namesStrInfo = "NameF,NameI,NameO";
        String namesStrUpdate = "NameF = 'NameF_test', DocSer = ''";
        fieldNames = Arrays.asList(namesStr.split(","));

        // =================

        // Провоцируем наличие дубликатов
        db.execSql("update " + tableName + " set " + namesStrUpdate + " where id <> 0");
        db.execSql("update " + tableName + " set " + namesStrUpdate + " where id <> 0");

        // =================

        System.out.println("table: " + tableName + ", fields: " + namesStr);

        UtData.outTable(db.loadSql("select id, " + namesStrInfo + "," + namesStr + " from " + tableName + " order by id"));

        // =================

        // Ищем дубликаты (useNullValues = false)
        Collection<RecDuplicate> resList0 = utRecMerge.findTableDuplicates(tableName, fieldNames, false);

        // Печатаем дубликаты
        System.out.println("Заданий на слияние: " + resList0.size());
        int recordCount0 = 1;
        for (RecDuplicate res : resList0) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
            //
            recordCount0 = res.records.size();
        }

        // Ищем дубликаты (useNullValues = true)
        Collection<RecDuplicate> resList1 = utRecMerge.findTableDuplicates(tableName, fieldNames, true);

        // Печатаем дубликаты
        System.out.println("Заданий на слияние: " + resList1.size());
        int recordCount1 = 1;
        for (RecDuplicate res : resList1) {
            System.out.println(res.params);
            UtData.outTable(res.records, 10);
            System.out.println();
            //
            recordCount1 = res.records.size();
        }
    }

    @Test
    public void test_execMergeTask() throws Exception {
        tableName = "LicDocTip";
        namesStr = "Name";
        fieldNames = Arrays.asList(namesStr.split(","));

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);


        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        //
        assertEquals("Найдены дубликаты", true, duplicates.size() > 0);


        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);


        // Печатаем задачу на слияние
        UtRecMergePrint.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        File fileResults = new File("temp/result.zip");
        utRecMerge.execMergePlan(mergeTasks, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);


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
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/task.json");

        // Десериализация задач
        Collection<RecMergePlan> mergeTasksFile = reader.readTasks("temp/task.json");
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачи, что прочитали
        UtRecMergePrint.printTasks(mergeTasksFile);
    }

    @Test
    public void test_execMergeTask_FromFile() throws Exception {
        // Формируем задачу на слияние
        test_makeMergeTask_ToFile();


        // Читаем задачу на слияние
        UtRecMergeRW reader = new UtRecMergeRW();
        Collection<RecMergePlan> mergeTasks = reader.readTasks("temp/task.json");
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачу на слияние
        UtRecMergePrint.printTasks(mergeTasks);


        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        File fileResults = new File("temp/result.zip");
        utRecMerge.execMergePlan(mergeTasks, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);


        // Снова ищем дубликаты
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by id"));
        //
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        assertEquals("Найдены дубликаты", false, duplicates.size() > 0);
    }

    @Test
    public void test_execMergeTask_FromFile_tmp() throws Exception {
        tableName = "SubjectTip";
        namesStr = "Name,Parent";
        fieldNames = Arrays.asList(namesStr.split(","));

        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        // Печатаем текущие дубликаты
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by " + namesStr + ", id"));

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/_" + tableName + ".task");


        // Читаем задачу на слияние
        mergeTasks = reader.readTasks("temp/_" + tableName + ".task");
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачу на слияние
        UtRecMergePrint.printTasks(mergeTasks);

        // Исполняем задачу на слияние
        System.out.println("Исполняем задачу на слияние");


        // Исполняем задачу на слияние
        File fileResults = new File("temp/_" + tableName + ".result.zip");
        utRecMerge.execMergePlan(mergeTasks, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);

        // Печатаем теперь дубликаты
        UtData.outTable(db.loadSql("select id, " + namesStr + " from " + tableName + " order by " + namesStr + ", id"));
    }

    @Test
    public void test_findTableDuplicates_XXX() throws Exception {
        // todo: плохо работает поиск дубликатов в иерархичных таблицах:
        // в эталонных записях на ПОДЧИНЕННОМ уровне ссылки на РОДИТЕЛЯ остаются на удаляемые записи,
        // Например
        //  Казахстан.id = 1
        //  Казахстан.id = 2
        //  Астана.parent = 2
        //  Астана.parent = 2
        // План удаления
        // Оставляем:
        //  Казахстан.id = 1
        // Удаляем
        //  Казахстан.id = 2
        // Оставляем:
        //  Астана.parent = 2
        // Удаляем:
        //  Астана.parent = 2
        tableName = "Region";
        namesStr = "RegionTip,Parent,Name";
        fieldNames = Arrays.asList(namesStr.split(","));

        // Ищем дубликаты
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/_Region_1.task.json");
    }

    @Test
    public void test_execMergeTask_XXX() throws Exception {
        test_findTableDuplicates_XXX();

        // Читаем задачу на слияние
        UtRecMergeRW reader = new UtRecMergeRW();
        Collection<RecMergePlan> mergeTasks = reader.readTasks("temp/_Region_1.task.json");

        // Исполняем задачу на слияние
        System.out.println("Исполняем задачу на слияние");
        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        File fileResults = new File("temp/result.zip");
        utRecMerge.execMergePlan(mergeTasks, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);
    }

    @Test
    public void test_execRevertExecTask() throws Exception {
        // Формируем задачу на слияние
        test_makeMergeTask_ToFile();

        // Читаем задачу на слияние
        UtRecMergeRW reader = new UtRecMergeRW();
        Collection<RecMergePlan> mergeTasks = reader.readTasks("temp/task.json");

        // Печатаем задачу на слияние
        UtRecMergePrint.printTasks(mergeTasks);


        // Исполняем задачу на слияние
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        File fileResults = new File("temp/result.zip");
        utRecMerge.execMergePlan(mergeTasks, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);


        // Читаем результат выполнения задачи
        RecMergeResultReader resultReader = new RecMergeResultReader(new FileInputStream(fileResults));
        // Отменяем слияние
        utRecMerge.revertExecMergePlan(resultReader);


        // Снова ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        //
        assertEquals("Найдены дубликаты", true, duplicates.size() > 0);
    }

    @Test
    public void test_NoDuplicates() throws Exception {
        logOff();
        assertEquals("Найдены дубликаты LicDocTip", true, getDuplicatesCount("LicDocTip", "Name") == 0);
        System.out.println();
        assertEquals("Найдены дубликаты LicDocVid", true, getDuplicatesCount("LicDocVid", "Name") == 0);
        System.out.println();
    }

    public long getDuplicatesCount(String tableName, String fieldNamesStr) throws Exception {
        DataStore st = db.loadSql("select count(*) cnt, " + fieldNamesStr + " from " + tableName + " where id <> 0 group by " + fieldNamesStr);
        //
        System.out.println(tableName + "." + fieldNamesStr);
        UtData.outTable(st);
        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Collection<String> fieldNames = Arrays.asList(fieldNamesStr.split(","));
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        return duplicates.size();
    }

}
