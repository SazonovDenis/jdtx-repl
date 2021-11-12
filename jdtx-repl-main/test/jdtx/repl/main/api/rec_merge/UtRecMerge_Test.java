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
    //String fieldNamesStr = "Name";
    String fieldNamesStr = "Name,ShortName";
    //String fieldNamesStr = "RNN";

    //
    Collection<String> fieldNames = Arrays.asList(fieldNamesStr.split(","));


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
        //logOn();
    }

    @Test
    public void test_MakeDuplicatesLoadDuplicates() throws Exception {
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Печатаем дубликаты
        UtRecMergePrint.printDuplicates(resList);

        //
        int recordCount0 = 1;
        for (RecDuplicate res : resList) {
            recordCount0 = res.records.size();
        }

        // =================

        System.out.println();
        System.out.println("Копируем запись: " + tableName);
        System.out.println();

        // Копируем запись 2 раза
        JdxDbUtils dbu = new JdxDbUtils(db, struct);
        long idMax = db.loadSql("select max(id) id from " + tableName).get(0).getValueLong("id");
        DataRecord rec = dbu.loadSqlRec("select * from " + tableName + " where id = :id", UtCnv.toMap("id", idMax));
        rec.setValue("id", null);
        dbu.insertRec(tableName, rec.getValues());
        dbu.insertRec(tableName, rec.getValues());

        // =================

        //UtData.outTable(db.loadSql("select id, " + fieldNamesStr + " from " + tableName + " order by id"));

        // =================

        // Снова ищем дубликаты
        resList = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Снова печатаем дубликаты
        UtRecMergePrint.printDuplicates(resList);

        //
        int recordCount1 = 0;
        for (RecDuplicate res : resList) {
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
        fieldNamesStr = "NameF,Rnn,DocSer";
        String namesStrInfo = "NameF,NameI,NameO";
        String namesStrUpdate = "NameF = 'NameF_test', DocSer = ''";
        fieldNames = Arrays.asList(fieldNamesStr.split(","));

        // =================

        // Провоцируем наличие дубликатов
        db.execSql("update " + tableName + " set " + namesStrUpdate + " where id <> 0");
        db.execSql("update " + tableName + " set " + namesStrUpdate + " where id <> 0");

        // =================

        System.out.println("table: " + tableName + ", fields: " + fieldNamesStr);

        UtData.outTable(db.loadSql("select id, " + namesStrInfo + "," + fieldNamesStr + " from " + tableName + " order by id"));

        // =================

        // Ищем дубликаты (useNullValues = false)
        Collection<RecDuplicate> resList0 = utRecMerge.findTableDuplicates(tableName, fieldNames, false);

        // Печатаем дубликаты
        UtRecMergePrint.printDuplicates(resList0);

        //
        int recordCount0 = 1;
        for (RecDuplicate res : resList0) {
            recordCount0 = res.records.size();
        }

        // Ищем дубликаты (useNullValues = true)
        Collection<RecDuplicate> resList1 = utRecMerge.findTableDuplicates(tableName, fieldNames, true);

        // Печатаем дубликаты
        UtRecMergePrint.printDuplicates(resList1);

        //
        int recordCount1 = 1;
        for (RecDuplicate res : resList1) {
            recordCount1 = res.records.size();
        }


        //
        //assertEquals("Есть задание на слияние", true, resList1.size() != 0);
        //assertEquals("Количество дубликатов", recordCount1, recordCount0 + 2);
    }

    private void test_MakeNoDuplicates() throws Exception {
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/_" + tableName + ".task.json");
        reader.writeDuplicates(duplicates, "temp/_" + tableName + ".duplicates.json");

        // Исполняем задачу на слияние
        File fileResults = new File("temp/result.zip");
        utRecMerge.execMergePlan(mergeTasks, fileResults);
    }

    @Test
    public void test_execMergeTask() throws Exception {
        tableName = "LicDocTip";
        fieldNamesStr = "Name";
        fieldNames = Arrays.asList(fieldNamesStr.split(","));

        //
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        // Теперь дубликаты есть
        check_IsDuplicates();

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

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


        // Теперь дубликатов нет
        check_NoDuplicates();
    }

    @Test
    public void test_makeMergeTask_ToFile() throws Exception {
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        // Теперь дубликаты есть
        check_IsDuplicates();

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/task.json");
        reader.writeDuplicates(duplicates, "temp/duplicates.json");

        // Десериализация задач
        Collection<RecMergePlan> mergeTasksFile = reader.readTasks("temp/task.json");
        //
        assertEquals("Есть задание на слияние", true, mergeTasks.size() > 0);

        // Печатаем задачи, что прочитали
        UtRecMergePrint.printTasks(mergeTasksFile);
    }

    @Test
    public void test_execMergeTask_FromFile() throws Exception {
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов и формируем задачу на слияние
        test_makeMergeTask_ToFile();

        // Теперь дубликаты есть
        check_IsDuplicates();

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


        // Теперь дубликатов нет
        check_NoDuplicates();
    }

    @Test
    public void test_execMergeTask_FromFile_tmp() throws Exception {
        tableName = "SubjectTip";
        fieldNamesStr = "Name,Parent";
        fieldNames = Arrays.asList(fieldNamesStr.split(","));

        //
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов
        test_MakeDuplicatesLoadDuplicates();

        // Теперь дубликаты есть
        check_IsDuplicates();

        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/_" + tableName + ".task.json");
        reader.writeDuplicates(duplicates, "temp/_" + tableName + ".duplicates.json");


        // Читаем задачу на слияние
        mergeTasks = reader.readTasks("temp/_" + tableName + ".task.json");
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

        // Теперь дубликатов нет
        check_NoDuplicates();
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
        fieldNamesStr = "RegionTip,Parent,Name";
        fieldNames = Arrays.asList(fieldNamesStr.split(","));

        // Ищем дубликаты
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergeRW reader = new UtRecMergeRW();
        reader.writeTasks(mergeTasks, "temp/_Region_1.task.json");
        reader.writeDuplicates(duplicates, "temp/_Region_1.duplicates.json");
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
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов и формируем задачу на слияние
        test_makeMergeTask_ToFile();

        // Теперь дубликаты есть
        check_IsDuplicates();


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
        System.out.println();
        System.out.println("Результат выполнения слияния:");
        UtRecMergePrint.printMergeResults(fileResults);


        // Теперь дубликатов нет
        check_NoDuplicates();


        // Отменяем слияние
        RecMergeResultReader resultReader = new RecMergeResultReader(new FileInputStream(fileResults));
        utRecMerge.revertExecMergePlan(resultReader);


        // Опять дубликаты есть
        check_IsDuplicates();
    }

    public void check_NoDuplicates() throws Exception {
        assertEquals("Найдены дубликаты " + tableName, true, getDuplicatesCount(tableName, fieldNamesStr) == 0);
    }

    public void check_IsDuplicates() throws Exception {
        assertEquals("Не найдены дубликаты " + tableName, true, getDuplicatesCount(tableName, fieldNamesStr) != 0);
    }

    public long getDuplicatesCount(String tableName, String fieldNamesStr) throws Exception {
        DataStore st = db.loadSql("select count(*) cnt, " + fieldNamesStr + " from " + tableName + " where id <> 0 group by " + fieldNamesStr + " having count(*) > 1");
        //
        if (st.size() == 0) {
            System.out.println("Дубликатов в: " + tableName + " по полям: " + fieldNamesStr + " нет");
        } else {
            System.out.println("Дубликаты в: " + tableName + " по полям: " + fieldNamesStr);
            UtData.outTable(st);
        }
        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);
        Collection<String> fieldNames = Arrays.asList(fieldNamesStr.split(","));
        Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(tableName, fieldNames);
        return duplicates.size();
    }

}
