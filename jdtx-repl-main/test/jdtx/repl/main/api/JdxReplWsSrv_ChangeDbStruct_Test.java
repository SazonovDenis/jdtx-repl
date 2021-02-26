package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.junit.*;

/**
 */
public class JdxReplWsSrv_ChangeDbStruct_Test extends JdxReplWsSrv_Test {

    String cfg_publications = "test/etalon/publication_struct_152.json";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //json_ws = "test/etalon/mail_http_ws_unidirectional.json";
    }


    /**
     * Проверка возможности холостого цикла MUTE-UNMUTE (без реального изменения структуры).
     */
    @Test
    public void test_Mute_Unmute() throws Exception {
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        JdxReplWs ws;
        JdxReplWs ws2;
        JdxReplWs ws3;


        // ===
        // Проверяем, что разрешенная, фиксированная и реальная структуры совпадают на ws1, ws2 и ws3
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db2);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db3);
        //
        ws = new JdxReplWs(db);
        ws.init();
        ws2 = new JdxReplWs(db2);
        ws2.init();
        ws3 = new JdxReplWs(db2);
        ws3.init();
        //
        System.out.println("ws1 struct size: " + ws.struct.getTables().size());
        System.out.println("ws2 struct size: " + ws2.struct.getTables().size());
        System.out.println("ws3 struct size: " + ws3.struct.getTables().size());
        //
        assertEquals("Перед тестом структура ws и ws2 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws2.struct));
        assertEquals("Перед тестом структура ws и ws3 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws3.struct));


        // ===
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        // Вносим изменения в данные на станциях
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // ===
        // Начинаем (на сервере) смену версии БД - формируем сигнал "всем молчать"
        test_srvDbStructStart();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        assert_handleSelfAudit_false(db);
        assert_handleSelfAudit_false(db2);
        assert_handleSelfAudit_false(db3);


        // ===
        // Завершаем (на сервере) смену версии БД - рассылаем сигнал "всем говорить"
        test_srvDbStructFinish();

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Убеждаемся что все рабочие станции говорят
        assert_handleSelfAudit_true(db);
        assert_handleSelfAudit_true(db2);
        assert_handleSelfAudit_true(db3);


        // ===
        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    /**
     * Проверка пограничных состояний:
     * Станция не применяет реплики:
     * - если реплики другой структуры.
     */
    @Test
    public void test_No_ApplyReplicas() throws Exception {
        JdxReplWs ws;
        JdxReplWs ws2;
        IJdxDbStruct structActual_ws1;
        IJdxDbStruct structFixed_ws1;
        IJdxDbStruct structAllowed_ws1;
        //
        UtDbStructMarker utDbStructMarker = new UtDbStructMarker(db);
        UtDbStructMarker utDbStructMarker_ws2 = new UtDbStructMarker(db2);
        //
        long queInNoDone1;
        long queInNoDone2;
        JdxStateManagerWs stateManager_ws2 = new JdxStateManagerWs(db2);


        // ===
        // Проверяем, что разрешенная, фиксированная и реальная структуры совпадают на ws1 и ws2
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db2);
        //
        ws = new JdxReplWs(db);
        ws.init();
        ws2 = new JdxReplWs(db2);
        ws2.init();
        //
        System.out.println("ws1 struct size: " + ws.struct.getTables().size());
        System.out.println("ws2 struct size: " + ws2.struct.getTables().size());
        //
        assertEquals("Перед тестом структура ws и ws2 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws2.struct));


        // ===
        // Начинаем и завершаем изменения структуре БД ws1

        // Делаем изменения структуре БД
        test_ws1_changeDbStruct();

        // Устанавливаем "разрешенную" структуру
        ws = new JdxReplWs(db);
        ws.init();
        structActual_ws1 = ws.struct;
        utDbStructMarker.setDbStructAllowed(structActual_ws1);

        // Делаем фиксацию структуры
        ws.dbStructApplyFixed();

        // Проверяем, что фиксация прошла нормально
        ws = new JdxReplWs(db);
        ws.init();
        //
        structActual_ws1 = ws.struct;
        structFixed_ws1 = utDbStructMarker.getDbStructFixed();
        structAllowed_ws1 = utDbStructMarker.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structAllowed_ws1));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structFixed_ws1));


        // ===
        // Формируем изменения данных на ws1, отправка в сеть
        test_ws1_makeChange();
        //
        test_ws1_doReplSession();
        //
        test_srv_doReplSession();


        // ===
        // Попытка принять и использовать реплики на ws2
        queInNoDone1 = stateManager_ws2.getQueNoDone("in");
        //
        ws2 = new JdxReplWs(db2);
        ws2.init();

        // Получаем входящие реплики
        ws2.receive();
        // Применяем входящие реплики
        ws2.handleQueIn();
        //
        queInNoDone2 = stateManager_ws2.getQueNoDone("in");

        // Применение реплик приостановлено
        assertEquals(queInNoDone1, queInNoDone2);


        // ===
        // Изменения структуре БД ws2
        // Делаем изменения структуре БД
        test_ws2_changeDbStruct();

        // Устанавливаем "разрешенную" структуру
        ws2 = new JdxReplWs(db2);
        ws2.init();
        structActual_ws1 = ws2.struct;
        utDbStructMarker_ws2.setDbStructAllowed(structActual_ws1);

        // Делаем фиксацию структуры
        ws2.dbStructApplyFixed();

        // Проверяем, что фиксация прошла нормально
        ws2 = new JdxReplWs(db2);
        ws2.init();
        //
        structActual_ws1 = ws2.struct;
        structFixed_ws1 = utDbStructMarker_ws2.getDbStructFixed();
        structAllowed_ws1 = utDbStructMarker_ws2.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structAllowed_ws1));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structFixed_ws1));


        // Попытка принять и использовать реплики
        queInNoDone1 = stateManager_ws2.getQueNoDone("in");
        //
        ws2 = new JdxReplWs(db2);
        ws2.init();

        // Получаем входящие реплики
        ws2.receive();
        // Применяем входящие реплики
        ws2.handleQueIn();
        //
        queInNoDone2 = stateManager_ws2.getQueNoDone("in");

        // Применение реплик проходит нормально
        assertNotSame(queInNoDone1, queInNoDone2);


        // ===
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }


    /**
     * Проверка пограничных состояний:
     * Станция не формирует реплики при несовпадении структур:
     * - если "реальная" структура не совпадет с "зафиксированной";
     * - если "реальная" структура не совпадет с "разрешенной".
     */
    @Test
    public void test_No_HandleSelfAudit() throws Exception {
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        JdxReplWs ws;
        IJdxDbStruct structActual;
        IJdxDbStruct structFixed;
        IJdxDbStruct structAllowed;
        //
        UtDbStructMarker utDbStructMarker = new UtDbStructMarker(db);


        // ===
        // Проверяем, что разрешенная, фиксированная и реальная структуры совпадают на ws1
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);


        // ===
        // Проверяем, что можно формировать реплики
        assert_handleSelfAudit_true(db);


        // ===
        // Изменения структуре БД ws1
        test_ws1_changeDbStruct();


        // ===
        // Проверяем, что реплики формировать не удается
        assert_handleSelfAudit_false(db);


        // ===
        // Пытаемся сделать фиксацию структуры после изменения структуры
        ws = new JdxReplWs(db);
        ws.init();
        //
        ws.dbStructApplyFixed();

        // Проверяем, что фиксация не удается
        ws = new JdxReplWs(db);
        ws.init();
        //
        structActual = ws.struct;
        structFixed = utDbStructMarker.getDbStructFixed();
        structAllowed = utDbStructMarker.getDbStructAllowed();
        //
        assertEquals(false, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(false, UtDbComparer.dbStructIsEqual(structActual, structFixed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structAllowed, structFixed));


        // ===
        // Устанавливаем "разрешенную" структуру
        utDbStructMarker.setDbStructAllowed(structActual);


        // ===
        // Проверяем, что реплики формировать не удается
        assert_handleSelfAudit_false(db);


        // ===
        // Пытаемся сделать фиксацию структуры
        ws.dbStructApplyFixed();

        // Проверяем, что фиксация прошла нормально
        ws = new JdxReplWs(db);
        ws.init();
        //
        structActual = ws.struct;
        structFixed = utDbStructMarker.getDbStructFixed();
        structAllowed = utDbStructMarker.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structFixed));


        // ===
        // Проверяем, что можно формировать реплики
        assert_handleSelfAudit_true(db);


        // ===
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }


    /**
     * Инициализация и прогон полного цикла смены структуры БД
     */
    @Test
    public void test_allSetUp_ModifyDbStruct() throws Exception {
        allSetUp();
        //
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        //
        test_modifyDbStruct();
    }

    /**
     * Инициализация и прогон полного цикла смены структуры БД,
     * ТРИ РАЗА
     */
    @Test
    public void test_allSetUp_modifyDbStruct_triple() throws Exception {
        allSetUp();
        //
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        //
        test_modifyDbStruct();
        test_modifyDbStruct();
        test_modifyDbStruct();
    }

    /**
     * Прогон полного цикла смены структуры БД
     */
    @Test
    public void test_modifyDbStruct() throws Exception {
        JdxReplWs ws;
        JdxReplWs ws2;
        JdxReplWs ws3;


        // ===
        // Проверяем, что разрешенная, фиксированная и реальная структуры совпадают на ws1, ws2 и ws3
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db2);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db3);
        //
        ws = new JdxReplWs(db);
        ws.init();
        ws2 = new JdxReplWs(db2);
        ws2.init();
        ws3 = new JdxReplWs(db2);
        ws3.init();
        //
        System.out.println("ws1 struct size: " + ws.struct.getTables().size());
        System.out.println("ws2 struct size: " + ws2.struct.getTables().size());
        System.out.println("ws3 struct size: " + ws3.struct.getTables().size());
        //
        assertEquals("Перед тестом структура ws и ws2 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws2.struct));
        assertEquals("Перед тестом структура ws и ws3 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws3.struct));


        // ===
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        // Вносим изменения в данные на станциях
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // ===
        // Начинаем (на сервере) смену версии БД - формируем сигнал "всем молчать"
        test_srvDbStructStart();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        assert_handleSelfAudit_false(db);
        assert_handleSelfAudit_false(db2);
        assert_handleSelfAudit_false(db3);


        // ===
        // Физически меняем свою структуру на сервере
        modifyDbStruct_internal(db);

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        assert_handleSelfAudit_false(db);
        assert_handleSelfAudit_false(db2);
        assert_handleSelfAudit_false(db3);


        // ===
        // Завершаем (на сервере) смену версии БД

        // На сервере напрямую задаем структуру публикаций (команда repl_set_cfg)
        JSONObject cfg = UtRepl.loadAndValidateCfgFile(cfg_publications);
        UtCfgMarker utCfgMarker = new UtCfgMarker(db);
        utCfgMarker.setSelfCfg(cfg, UtCfgType.PUBLICATIONS);

        // От сервера рассылаем...
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // ... рассылаем на рабочие станции новые правила публикаций (команда repl_send_cfg) ...
        srv.srvSendCfg(cfg_publications, UtCfgType.PUBLICATIONS, 0);

        // ... рассылаем сигнал "всем говорить" (команда repl_dbstruct_finish)
        test_srvDbStructFinish();

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        // Только сервер изменил структуру и перестал молчать, а станции еще не смогли сделать SET_DB_STRUCT,
        // потому что у них реальная структура не совпадает с разрешенной (не хватает новых таблиц TEST_TABLE_*)
        assertEquals(1, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        // Убеждаемся что рабочие станции молчат (из-из запрета), а сервер нет
        assert_handleSelfAudit_true(db);
        assert_handleSelfAudit_false(db2);
        assert_handleSelfAudit_false(db3);


        // ===
        // Физически меняем структуру на рабочих станциях ws2 и ws3
        modifyDbStruct_internal(db2);
        modifyDbStruct_internal(db3);
        //reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой


        // ===
        // Убеждаемся что рабочие станции молчат (из-за незафиксированности структуры), а сервер нет
        assert_handleSelfAudit_true(db);
        assert_handleSelfAudit_false(db2);
        assert_handleSelfAudit_false(db3);


        // ===
        // Заставляем станции зафиксировать структуру
        test_ws2_doReplSession();
        test_ws3_doReplSession();


        // ===
        // Убеждаемся что все рабочие станции говорят
        assert_handleSelfAudit_true(db);
        assert_handleSelfAudit_true(db2);
        assert_handleSelfAudit_true(db3);

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));


        // ===
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    private void assertEqualsStruct_Actual_Allowed_Fixed_ws(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        //
        IJdxDbStruct structActual = ws.struct;
        UtDbStructMarker utDbStructMarker = new UtDbStructMarker(db);
        IJdxDbStruct structFixed = utDbStructMarker.getDbStructFixed();
        IJdxDbStruct structAllowed = utDbStructMarker.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structFixed));
    }


/*
    @Test
    public void test_sync_changeDbStruct() throws Exception {
        sync_http();
        test_DumpTables();
        modifyDbStruct_internal();
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой
    }
*/


    @Test
    public void test_ws1_changeDbStruct() throws Exception {
        modifyDbStruct_internal(db);
    }

    @Test
    public void test_ws2_changeDbStruct() throws Exception {
        modifyDbStruct_internal(db2);
    }

    @Test
    public void test_ws3_changeDbStruct() throws Exception {
        modifyDbStruct_internal(db3);
    }

    /**
     * Проверяет корректность формирования аудита при цикле вставки и удаления влияющей записи:
     */
    @Test
    public void test_auditAfterInsDel() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.make_InsDel(struct2, 2);

        // Формирование аудита
        JdxReplWs ws = new JdxReplWs(db2);
        ws.handleSelfAudit();
    }


    @Test
    public void test_srvDbStructStart() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        srv.srvDbStructStart();
    }

    @Test
    public void test_srvDbStructFinish() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        srv.srvDbStructFinish();
    }


    /**
     * Проверяем, что реплики формировать не удается
     */
    void assert_handleSelfAudit_false(Db db) throws Exception {
        JdxReplWs ws;
        long auditAge0;
        long auditAge1;
        long auditAge2;

        //
        ws = new JdxReplWs(db);
        ws.init();
        //
        UtAuditAgeManager uta = new UtAuditAgeManager(db, ws.struct);

        //
        auditAge0 = uta.getAuditAge();
        //
        UtTest utTest = new UtTest(db);
        utTest.makeChange(ws.struct, ws.wsId);
        //
        auditAge1 = uta.getAuditAge();
        //
        ws.handleSelfAudit();
        //
        auditAge2 = uta.getAuditAge();

        //
        assertEquals(auditAge0, auditAge1);
        assertEquals(auditAge1, auditAge2);
    }

    /**
     * Проверяем, что можно формировать реплики
     */
    void assert_handleSelfAudit_true(Db db) throws Exception {
        JdxReplWs ws;
        long auditAge0;
        long auditAge1;
        long auditAge2;

        //
        ws = new JdxReplWs(db);
        ws.init();
        //
        UtAuditAgeManager uta = new UtAuditAgeManager(db, ws.struct);

        //
        auditAge0 = uta.getAuditAge();
        //
        UtTest utTest = new UtTest(db);
        utTest.makeChange(ws.struct, ws.wsId);
        //
        auditAge1 = uta.getAuditAge();
        //
        ws.handleSelfAudit();
        //
        auditAge2 = uta.getAuditAge();

        //
        assertEquals(auditAge0, auditAge1);
        assertNotSame(auditAge1, auditAge2);
    }

    public void do_DumpTables(Db db, Db db2, Db db3, IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct struct3) throws Exception {
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой
        super.do_DumpTables(db, db2, db3, struct1, struct2, struct3);
    }

    //todo проверить, что лишний аудит не создается

    /**
     * Меняет структуру БД:
     * Удаляет таблицу AppUpdate,
     * добавляет одну таблицу,
     * в таблицу Region добавляет поле
     */
    void modifyDbStruct_internal(Db db) throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct;

        //
        struct = reader.readDbStruct();
        struct_rw.toFile(struct, "../_test-data/dbStruct_0.xml");

        //
        UtTest utTest = new UtTest(db);
        //
        utTest.changeDbStruct_DropTable("appupdate");
        //
        String randomTableName = utTest.getFirstRandomTable();
        if (randomTableName != null) {
            utTest.changeDbStruct_DropFirstRandomField(randomTableName);
            utTest.changeDbStruct_AddRandomField(randomTableName);
            utTest.changeDbStruct_AddRandomField(randomTableName);
        }
        //
        utTest.changeDbStruct_AddRandomTable();
        //
        utTest.changeDbStruct_DropLastField("categoryHist");
        utTest.changeDbStruct_AddRandomField("categoryHist");
        //
        utTest.changeDbStruct_AddRandomField("region");

        //
        struct = reader.readDbStruct();
        struct_rw.toFile(struct, "../_test-data/dbStruct_1.xml");
    }


}
