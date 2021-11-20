package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.rec_merge.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

import java.io.*;

public class JdxReplWsSrv_Merge_Test extends JdxReplWsSrv_Test {


    /**
     * Создание репликации и удаление дубликтов, которые появились на сервере после превичного слияния.
     */
    @Test
    public void test_SetUp_Merge() throws Exception {
        // ---

        // Создание репликации
        allSetUp();

        // Проверяем отсутствие дубликатов
        UtRecMerge_Test testMerge = new UtRecMerge_Test();
        testMerge.setUp();

        // Проверяем отсутствие дубликатов на сервере
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws2
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws3
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocVid", "Name") == 0);


        // ---

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Прогон тестов
        test_AllHttp();

        // Проверяем наличие дубликатов на сервере
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") != 0);

        // Проверяем наличие дубликатов на ws2
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocVid", "Name") != 0);

        // Проверяем наличие дубликатов на ws3
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocVid", "Name") != 0);


        // ---

        // Поиск дубликатов и формирование плана, формируем команду MERGE для филиалов
        doMergeByCommand();


        // ---

        // Проверяем отсутствие дубликатов на сервере
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws2
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws3
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocVid", "Name") == 0);


        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Прогон базового сценария репликации, с односторонним фильтром по LIC
     */
    @Test
    public void test_allSetUp_TestAll_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json";
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        test_allSetUp_TestAll();
    }

    /**
     * Создиние дубликатов на работающей системе и их удаление
     * через команду "MERGE" репликации
     */
    @Test
    public void test_LicDoc_MergeCommand() throws Exception {
        UtRecMerge_Test testMerge = new UtRecMerge_Test();
        testMerge.setUp();


        // Проверяем отсутствие дубликатов на сервере
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws2
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws3
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocVid", "Name") == 0);

        //
        System.out.println();


        // ---

        // Создание дубликатов LicDocTip на ws2
        testMerge.makeDuplicates(db2, struct2, "LicDocTip");
        // Создание дубликатов LicDocVid на ws3
        testMerge.makeDuplicates(db3, struct3, "LicDocVid");

        //
        System.out.println();

        // Проверяем отсутствие дубликатов на сервере
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") == 0);

        // Проверяем наличие дубликатов на ws2
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocTip", "Name") != 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocVid", "Name") == 0);

        // Проверяем наличие дубликатов на ws3
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocTip", "Name") == 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocVid", "Name") != 0);

        //
        System.out.println();

        // Обмен
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        System.out.println();

        // Проверяем наличие дубликатов на сервере
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") != 0);


        // ---

        // Поиск дубликатов и формирование плана, формируем команду MERGE для филиалов
        doMergeByCommand();

        //
        System.out.println();

        // Проверяем отсутствие дубликатов на сервере
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws2
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "LicDocVid", "Name") == 0);

        // Проверяем отсутствие дубликатов на ws3
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct3, "LicDocVid", "Name") == 0);


        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Создиние дубликатов на работающей системе и их удаление
     * на сервере, через jc-команду
     */
    @Test
    public void test_CommentTip_jc() throws Exception {
        UtRecMerge_Test testMerge = new UtRecMerge_Test();

        // Проверяем отсутствие дубликатов
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct2, "CommentTip", "Name") == 0);
        //assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db5, struct2, "CommentTip", "Name") == 0);

        // Просто добавляем значения
        UtTest utTest1 = new UtTest(db);
        utTest1.makeChange_CommentTip(struct, 1);
        utTest1.makeChange_CommentTip(struct, 1);
        utTest1.makeChange_CommentTip(struct, 1);
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.makeChange_CommentTip(struct2, 2);
        utTest2.makeChange_CommentTip(struct2, 2);
        utTest2.makeChange_CommentTip(struct2, 2);
        //
        UtTest utTest3 = new UtTest(db3);
        utTest3.makeChange_CommentTip(struct3, 3);
        utTest3.makeChange_CommentTip(struct3, 3);
        utTest3.makeChange_CommentTip(struct3, 3);
        //
        //UtTest utTest5 = new UtTest(db5);
        //utTest5.makeChange_CommentTip(struct5, 5);

        // Проверяем отсутствие дубликатов
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct2, "CommentTip", "Name") == 0);
        //assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db5, struct2, "CommentTip", "Name") == 0);

        // Делаем дубликаты
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));
        db.execSql("update CommentTip set Name = 'Tip-ins-all' where Name like 'Tip-ins-ws%'");
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));

        // Проверяем наличие дубликатов на сервере
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db, struct, "CommentTip", "Name") != 0);
        // Проверяем отсутствие дубликатов на филиалах
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct2, "CommentTip", "Name") == 0);
        //assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db5, struct2, "CommentTip", "Name") == 0);

        // Синхронизируем
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем наличие дубликатов
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db, struct, "CommentTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "CommentTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db3, struct2, "CommentTip", "Name") != 0);
        //assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db5, struct2, "CommentTip", "Name") != 0);

        //
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript project = jc.loadProject("../ext/srv/project.jc");
        Merge_Ext ext = (Merge_Ext) project.createExt("jdtx.repl.main.ext.Merge_Ext");

        //
        IVariantMap args = new VariantMap();
        args.put("table", "CommentTip");
        args.put("file", "temp/_CommentTip.plan.json");
        args.put("fields", "Name");
        args.put("cfg_group", "test/etalon/field_groups.json");

        // Готовим план слияния записей
        deletePlanFiles("temp/_CommentTip");
        ext.rec_merge_find(args);

        // Сливаем записи
        ext.rec_merge_exec(args);

        //
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));


        // Проверяем отсутствие дубликатов на сервере
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "CommentTip", "Name") == 0);
        // Проверяем наличие дубликатов на филиалах
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "CommentTip", "Name") != 0);
        assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db3, struct2, "CommentTip", "Name") != 0);
        //assertEquals("Не появились дубликаты", true, testMerge.getDuplicatesCount(db5, struct2, "CommentTip", "Name") != 0);


        // Синхронизируем
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Проверяем отсутствие дубликатов
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db, struct, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db2, struct2, "CommentTip", "Name") == 0);
        assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db3, struct2, "CommentTip", "Name") == 0);
        //assertEquals("Найдены дубликаты", true, testMerge.getDuplicatesCount(db5, struct2, "CommentTip", "Name") == 0);


        //
        test_DumpTables_1_2_3();
    }

    /**
     * Поиск дубликатов и формирование плана, формируем команду MERGE для филиалов
     */
    private void doMergeByCommand() throws Exception {
        UtRecMerge_Test testMerge = new UtRecMerge_Test();
        testMerge.setUp();


        // Поиск дубликатов, формирование плана
        //testMerge.findDuplicates_makeMergePlans_ToFile("LicDocTip", "Name");
        //testMerge.findDuplicates_makeMergePlans_ToFile("LicDocVid", "Name");

        //
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript project = jc.loadProject("../ext/srv/project.jc");
        Merge_Ext extMerge = (Merge_Ext) project.createExt("jdtx.repl.main.ext.Merge_Ext");
        Jdx_Ext extJdx = (Jdx_Ext) project.createExt("jdtx.repl.main.ext.Jdx_Ext");


        //
        IVariantMap args = new VariantMap();

        // Готовим план слияния записей
        deletePlanFiles("temp/LicDocTip");
        //
        args.clear();
        args.put("table", "LicDocTip");
        args.put("fields", "Name");
        args.put("file", "temp/LicDocTip.plan.json");
        args.put("cfg_group", "test/etalon/field_groups.json");
        //
        extMerge.rec_merge_find(args);

        //
        deletePlanFiles("temp/LicDocVid");
        //
        args.clear();
        args.put("table", "LicDocVid");
        args.put("fields", "Name");
        args.put("file", "temp/LicDocVid.plan.json");
        args.put("cfg_group", "test/etalon/field_groups.json");
        //
        extMerge.rec_merge_find(args);


        // Отправляем команду на слияние
        args.clear();
        args.put("file", "temp/LicDocTip.plan.json");
        //
        extJdx.repl_merge_request(args);

        //
        args.clear();
        args.put("file", "temp/LicDocVid.plan.json");
        //
        extJdx.repl_merge_request(args);


/*
        // Формируем из плана команду MERGE для филиалов
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        srv.srvMergeRequest("temp/_LicDocTip.plan.json");
        srv.srvMergeRequest("temp/_LicDocVid.plan.json");
*/


        // Обмен
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
    }

    private void deletePlanFiles(String filePrefix) {
        new File(filePrefix+".plan.json").delete();
        new File(filePrefix+".plan.json.duplicates").delete();
        new File(filePrefix+".plan.result.zip").delete();
    }


}
