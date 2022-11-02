package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class JdxRecMerge_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;


    //String tableName = "Ulz";
    String tableName = "LicDocTip";
    //String tableName = "LicDocVid";
    //String tableName = "Lic";
    //
    String tableNameCascade = "Lic";
    //
    //String fieldNamesStr = "Name";
    String fieldNamesStr = "Name,ShortName";
    //String fieldNamesStr = "RNN";

    //
    Collection<String> fieldNames = Arrays.asList(fieldNamesStr.split(","));


    private JdxRecMerger getJdxRecMerger() throws Exception {
        //IJdxDataSerializer dataSerializer = new JdxDataSerializerDecode(db, 1);
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer);
        return recMerger;
    }

    private JdxRecMerger getJdxRecMerger(Db db, IJdxDbStruct struct) throws Exception {
        //IJdxDataSerializer dataSerializer = new JdxDataSerializerDecode(db, 1);
        IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain();
        JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer);
        return recMerger;
    }

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
        JSONObject cfgGroups = UtRepl.loadAndValidateJsonFile("test/etalon/field_groups.json");
        GroupsStrategyStorage.initInstance(cfgGroups, struct);
    }

    @Test
    public void test_makeDuplicates() throws Exception {
        JdxRecMerger recMerger = getJdxRecMerger();

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = recMerger.findTableDuplicates(tableName, fieldNamesStr, true);

        // Печатаем дубликаты
        UtRecMergePrint.printDuplicates(duplicates);

        //
        int recordCount0 = 1;
        for (RecDuplicate res : duplicates) {
            recordCount0 = res.records.size();
        }


        // Создаем дубликаты
        makeDuplicates(db, struct, tableName, tableNameCascade);


        // Снова ищем дубликаты
        duplicates = recMerger.findTableDuplicates(tableName, fieldNamesStr, true);

        // Снова печатаем дубликаты
        UtRecMergePrint.printDuplicates(duplicates);

        //
        int recordCount1 = 0;
        for (RecDuplicate res : duplicates) {
            recordCount1 = res.records.size();
        }

        //
        assertEquals("Есть задание на слияние", false, duplicates.size() == 0);
        assertEquals("Количество дубликатов", recordCount1, recordCount0 + 2);
    }

    @Test
    public void test_FundDuplicatesUseNull() throws Exception {
        logOff();

        //
        JdxRecMerger recMerger = getJdxRecMerger();

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
        Collection<RecDuplicate> resList0 = recMerger.findTableDuplicates(tableName, fieldNamesStr, false);

        // Печатаем дубликаты
        UtRecMergePrint.printDuplicates(resList0);

        //
        int recordCount0 = 1;
        for (RecDuplicate res : resList0) {
            recordCount0 = res.records.size();
        }

        // Ищем дубликаты (useNullValues = true)
        Collection<RecDuplicate> resList1 = recMerger.findTableDuplicates(tableName, fieldNamesStr, true);

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
        JdxRecMerger recMerger = getJdxRecMerger();

        // Ищем дубликаты, создаем задачу на слияние, пишем ее в файл
        findDuplicates_makeMergePlans_ToFile(tableName, fieldNamesStr);

        // Читаем задачу на слияние
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        Collection<RecMergePlan> mergePlans = reader.readPlans("temp/_" + tableName + ".plan.json");

        // Исполняем задачу на слияние
        File fileResults = new File("temp/result.zip");
        fileResults.delete();
        recMerger.execMergePlan(mergePlans, fileResults);
    }

    @Test
    public void test_execMergePlan() throws Exception {
        tableName = "LicDocTip";
        fieldNamesStr = "Name";
        fieldNames = Arrays.asList(fieldNamesStr.split(","));

        //
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов
        test_makeDuplicates();

        // Теперь дубликаты есть
        check_IsDuplicates();

        //
        JdxRecMerger recMerger = getJdxRecMerger();

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = recMerger.findTableDuplicates(tableName, fieldNamesStr, true);
        //
        assertEquals("Найдены дубликаты", true, duplicates.size() > 0);


        // Тупо превращаем дубликаты в задачу на слияние
        Collection<RecMergePlan> mergePlans = recMerger.prepareMergePlan(tableName, duplicates);
        //
        assertEquals("Есть задание на слияние", true, mergePlans.size() > 0);


        // Печатаем задачу на слияние
        UtRecMergePrint.printPlans(mergePlans);

        // Исполняем задачу на слияние
        File fileResults = new File("temp/result.zip");
        fileResults.delete();
        recMerger.execMergePlan(mergePlans, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);


        // Теперь дубликатов нет
        check_NoDuplicates();
    }

    @Test
    public void test_makeDuplicates_makeMergePlan_ToFile() throws Exception {
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов
        test_makeDuplicates();

        // Теперь дубликаты есть
        check_IsDuplicates();

        // Ищем дубликаты, создаем задачу на слияние, пишем ее в файл
        findDuplicates_makeMergePlans_ToFile(tableName, fieldNamesStr);
    }

    public void findDuplicates_makeMergePlans_ToFile(String tableName, String fieldNamesStr) throws Exception {
        JdxRecMerger recMerger = getJdxRecMerger();

        // Ищем дубликаты
        Collection<RecDuplicate> duplicates = recMerger.findTableDuplicates(tableName, fieldNamesStr, true);

        // Тупо превращаем дубликаты в задачи на слияние
        Collection<RecMergePlan> mergePlans = recMerger.prepareMergePlan(tableName, duplicates);

        // Сериализация задач
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        reader.writeDuplicates(duplicates, "temp/_" + tableName + ".duplicates.json");
        reader.writePlans(mergePlans, "temp/_" + tableName + ".plan.json");

        // Читаем и печатаем задачи
        Collection<RecMergePlan> mergePlansFile = reader.readPlans("temp/_" + tableName + ".plan.json");
        UtRecMergePrint.printPlans(mergePlansFile);
    }

    @Test
    public void test_execMergePlans_FromFile() throws Exception {
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов и формируем задачу на слияние
        test_makeDuplicates_makeMergePlan_ToFile();

        // Теперь дубликаты есть
        check_IsDuplicates();

        // Читаем задачу на слияние
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        Collection<RecMergePlan> mergePlans = reader.readPlans("temp/_" + tableName + ".plan.json");

        //
        assertEquals("Есть задание на слияние", true, mergePlans.size() > 0);

        // Печатаем задачу на слияние
        UtRecMergePrint.printPlans(mergePlans);


        // Исполняем задачу на слияние
        JdxRecMerger recMerger = getJdxRecMerger();
        File fileResults = new File("temp/_" + tableName + ".result.zip");
        fileResults.delete();
        recMerger.execMergePlan(mergePlans, fileResults);

        // Печатаем результат выполнения задачи
        UtRecMergePrint.printMergeResults(fileResults);


        // Теперь дубликатов нет
        check_NoDuplicates();
    }

    @Test
    public void test_execMergePlans_FromFile_SubjectTip() throws Exception {
        tableName = "SubjectTip";
        fieldNamesStr = "Name,Parent";
        fieldNames = Arrays.asList(fieldNamesStr.split(","));

        //
        test_execMergePlans_FromFile();
    }


    @Test
    public void test_execMergePlans_Parent() throws Exception {
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

        //
        test_execMergePlans_FromFile();
    }

    @Test
    public void test_revert_ws() throws Exception {
        //logOn();

        //
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        IJdxRecMerger recMerger = ws.getRecMerger();

        //
        File fileResults = new File("temp/229990_result.zip");
        recMerger.revertExec(fileResults, UtCnv.toList("LicDocTip", ","));
    }

    @Test
    public void test_revertExecMergePlan() throws Exception {
        test_MakeNoDuplicates();

        // Вначале дубликатов нет
        check_NoDuplicates();

        // Провоцируем появление дубликатов и формируем задачу на слияние
        test_makeDuplicates_makeMergePlan_ToFile();

        // Теперь дубликаты есть
        check_IsDuplicates();


        // Читаем задачу на слияние
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        Collection<RecMergePlan> mergePlans = reader.readPlans("temp/_" + tableName + ".plan.json");

        //
        assertEquals("Есть задание на слияние", false, mergePlans.size() == 0);

        // Печатаем задачу на слияние
        System.out.println("Задача на слияние:");
        UtRecMergePrint.printPlans(mergePlans);
        System.out.println();


        // Исполняем задачу на слияние
        System.out.println("Исполняем задачу на слияние");
        JdxRecMerger recMerger = getJdxRecMerger();
        File fileResults = new File("temp/result.zip");
        fileResults.delete();
        recMerger.execMergePlan(mergePlans, fileResults);
        System.out.println();

        // Печатаем результат выполнения задачи
        System.out.println("Результат выполнения слияния:");
        UtRecMergePrint.printMergeResults(fileResults);
        System.out.println();


        // Теперь дубликатов нет
        System.out.println("Теперь дубликатов нет");
        check_NoDuplicates();
        System.out.println();


        // Отменяем слияние
        System.out.println("Отменяем слияние, без таблицы " + tableName);
        recMerger.revertExec(fileResults, UtCnv.toList("Tab1,Tab2", ","));
        System.out.println();


        // Пока дубликаты не появились
        System.out.println("Пока дубликаты не появились");
        check_NoDuplicates();
        System.out.println();


        // Отменяем слияние
        System.out.println("Отменяем слияние, для таблицы " + tableName);
        recMerger.revertExec(fileResults, UtCnv.toList("Tab3," + tableName, ","));
        System.out.println();


        // Опять дубликаты есть
        System.out.println("Опять дубликаты есть");
        check_IsDuplicates();
    }

    void check_NoDuplicates() throws Exception {
        assertDuplicatesExists(false, db, struct, tableName, fieldNamesStr);
    }

    void check_IsDuplicates() throws Exception {
        assertDuplicatesExists(true, db, struct, tableName, fieldNamesStr);
    }

    public void assertDuplicatesExists(boolean shouldExists, Db db, IJdxDbStruct struct, String tableName, String fieldsNames) throws Exception {
        if (shouldExists) {
            assertEquals("Не найдены дубликаты " + UtJdx.getDbName(db) + "." + tableName, true, getDuplicatesCount(db, struct, tableName, fieldsNames) != 0);
        } else {
            assertEquals("Найдены дубликаты " + UtJdx.getDbName(db) + "." + tableName, true, getDuplicatesCount(db, struct, tableName, fieldsNames) == 0);
        }
    }

    public void makeDuplicates(Db db, IJdxDbStruct struct, String tableName, String tableNameCascade) throws Exception {
        System.out.println("Копируем запись: " + UtJdx.getDbName(db) + "." + tableName);

        //
        JdxDbUtils dbu = new JdxDbUtils(db, struct);

        // Делаем еще 2 дубликата

        // Выбираем запись tableName
        long id = db.loadSql("select max(id) id from " + tableName).get(0).getValueLong("id");
        DataRecord rec = dbu.loadSqlRec("select * from " + tableName + " where id = :id", UtCnv.toMap("id", id));

        // Копируем запись 2 раза
        rec.setValue("id", null);
        long id1 = dbu.insertRec(tableName, rec.getValues());
        long id2 = dbu.insertRec(tableName, rec.getValues());


        // Добавляем зависимости (использование записи)

        // Выбираем зависимые записи из tableNameCascade
        long idCascade0 = db.loadSql("select max(id) id from " + tableNameCascade).get(0).getValueLong("id");
        long idCascade1 = db.loadSql("select max(id) id from " + tableNameCascade + " where id < " + idCascade0).get(0).getValueLong("id");
        long idCascade2 = db.loadSql("select max(id) id from " + tableNameCascade + " where id < " + idCascade1).get(0).getValueLong("id");

        // Ставим ссылки на вновь вставленные записи
        dbu.updateRec(tableNameCascade, UtCnv.toMap("id", idCascade0, tableName, id), tableName);
        dbu.updateRec(tableNameCascade, UtCnv.toMap("id", idCascade1, tableName, id1), tableName);
        dbu.updateRec(tableNameCascade, UtCnv.toMap("id", idCascade2, tableName, id2), tableName);
    }

    public long getDuplicatesCount(Db db, IJdxDbStruct struct, String tableName, String fieldNamesStr) throws Exception {
        DataStore st = db.loadSql("select count(*) cnt, " + fieldNamesStr + " from " + tableName + " where id <> 0 group by " + fieldNamesStr + " having count(*) > 1");
        //
        if (st.size() == 0) {
            System.out.println("Дубликатов в: " + UtJdx.getDbName(db) + "." + tableName + " по полям: " + fieldNamesStr + " нет");
        } else {
            System.out.println("Дубликаты в: " + UtJdx.getDbName(db) + "." + tableName + " по полям: " + fieldNamesStr);
            UtData.outTable(st);
        }
        //
        JdxRecMerger recMerger = getJdxRecMerger(db, struct);
        Collection<RecDuplicate> duplicates = recMerger.findTableDuplicates(tableName, fieldNamesStr, true);
        return duplicates.size();
    }

}
