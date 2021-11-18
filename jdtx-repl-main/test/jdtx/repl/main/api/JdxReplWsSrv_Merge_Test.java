package jdtx.repl.main.api;

import jdtx.repl.main.api.rec_merge.*;
import org.junit.*;

public class JdxReplWsSrv_Merge_Test extends JdxReplWsSrv_Test {


    /**
     * Создание репликации.
     * Удаление дубликтов, которые появились на сервере после превичного слияния.
     */
    @Test
    public void test_allSetUp_TestAll() throws Exception {
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
     * Умышленное создиние дубликатов на работающей системе и их удаление
     */
    @Test
    public void test_MergeByCommand() throws Exception {
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
     * Поиск дубликатов и формирование плана, формируем команду MERGE для филиалов
     */
    private void doMergeByCommand() throws Exception {
        UtRecMerge_Test testMerge = new UtRecMerge_Test();
        testMerge.setUp();


        // Поиск дубликатов, формирование плана
        testMerge.findDuplicates_makeMergePlans_ToFile("LicDocTip", "Name");
        testMerge.findDuplicates_makeMergePlans_ToFile("LicDocVid", "Name");


        // Формируем из плана команду MERGE для филиалов
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        srv.srvMergeRequest("temp/_LicDocTip.plan.json");
        srv.srvMergeRequest("temp/_LicDocVid.plan.json");


        // Обмен
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
    }



}
