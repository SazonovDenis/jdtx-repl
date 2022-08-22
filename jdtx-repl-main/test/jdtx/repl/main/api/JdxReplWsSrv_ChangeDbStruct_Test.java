package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.junit.*;

/**
 *
 */
public class JdxReplWsSrv_ChangeDbStruct_Test extends JdxReplWsSrv_Test {

    String cfg_publications = "test/etalon/publication_struct_152.json";

    private String sqlWsList = "select\n" +
            "  WORKSTATION_LIST.ID as WS_ID,\n" +
            "  WORKSTATION_LIST.NAME,\n" +
            "  WORKSTATION_LIST.GUID,\n" +
            "  STATE__ENABLED.param_value as ENABLED,\n" +
            "  STATE__MUTE_AGE.param_value as MUTE_AGE\n" +
            "from\n" +
            "  Z_Z_SRV_WORKSTATION_LIST WORKSTATION_LIST\n" +
            "  left join Z_Z_SRV_WORKSTATION_STATE STATE__ENABLED on (WORKSTATION_LIST.id = STATE__ENABLED.ws_id and STATE__ENABLED.param_name = 'enabled')\n" +
            "  left join Z_Z_SRV_WORKSTATION_STATE STATE__MUTE_AGE on (WORKSTATION_LIST.id = STATE__MUTE_AGE.ws_id and STATE__MUTE_AGE.param_name = 'mute_age')\n" +
            "where\n" +
            "  1=1\n";

    private String sqlEnabledIs1 = sqlWsList +
            "  and STATE__ENABLED.param_value = 1";

    private String sqlEnabledIs1MuteIs0 = sqlWsList +
            "  and STATE__ENABLED.param_value = 1\n" +
            "  and STATE__MUTE_AGE.param_value = 0";

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
        JdxReplWs ws1;
        JdxReplWs ws2;
        JdxReplWs ws3;


        // ---
        // Проверяем, что разрешенная, фиксированная и реальная структуры совпадают на ws1, ws2 и ws3
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db2);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db3);
        //
        ws1 = new JdxReplWs(db);
        ws1.init();
        ws2 = new JdxReplWs(db2);
        ws2.init();
        ws3 = new JdxReplWs(db2);
        ws3.init();
        //
        System.out.println("ws1 struct tables count: " + ws1.struct.getTables().size());
        System.out.println("ws2 struct tables count: " + ws2.struct.getTables().size());
        System.out.println("ws3 struct tables count: " + ws3.struct.getTables().size());
        //
        assertEquals("Перед тестом структура ws1 и ws2 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws1.struct, ws2.struct));
        assertEquals("Перед тестом структура ws1 и ws3 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws1.struct, ws3.struct));


        // ---
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(3, db.loadSql(sqlEnabledIs1MuteIs0).size());


        // ---
        // Вносим изменения в данные на станциях
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // ---
        // Начинаем (на сервере) смену версии БД - формируем сигнал "всем молчать"
        doSrvMuteAll();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(0, db.loadSql(sqlEnabledIs1MuteIs0).size());


        // ---
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        assert_handleSelfAudit(db, false);
        assert_handleSelfAudit(db2, false);
        assert_handleSelfAudit(db3, false);


        // ---
        // Завершаем (на сервере) смену версии БД - рассылаем сигнал "всем говорить"
        doSrv_Unmute();

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        // Убеждаемся что все рабочие станции говорят
        assert_handleSelfAudit(db, true);
        assert_handleSelfAudit(db2, true);
        assert_handleSelfAudit(db3, true);


        // ---
        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(3, db.loadSql(sqlEnabledIs1MuteIs0).size());
    }

    /**
     * Проверка пограничных состояний:
     * Станция не применяет реплики:
     * - если реплики другой структуры.
     */
    @Test
    public void test_No_ApplyReplicas() throws Exception {
        JdxReplWs ws1;
        JdxReplWs ws2;
        //
        DatabaseStructManager databaseStructManager_ws1 = new DatabaseStructManager(db);
        DatabaseStructManager databaseStructManager_ws2 = new DatabaseStructManager(db2);
        //
        long queInNoDone1;
        long queInNoDone2;
        JdxStateManagerWs stateManager_ws2 = new JdxStateManagerWs(db2);


        // ---
        // Начальные проверки

        // Проверяем, что на ws1 и ws2 разрешенная, фиксированная и реальная структуры совпадают
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db2);
        //
        ws1 = new JdxReplWs(db);
        ws1.init();
        ws2 = new JdxReplWs(db2);
        ws2.init();

        // Проверяем, что структуры одинаковы у ws1 и ws2
        System.out.println("ws1 tables count: " + ws1.struct.getTables().size());
        System.out.println("ws2 tables count: " + ws2.struct.getTables().size());
        //
        assertEquals("Перед тестом структура ws1 и ws2 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws1.struct, ws2.struct));


        // ---
        // Начинаем и завершаем изменения в структуре БД ws1

        // Делаем изменения в структуре БД
        modifyDbStruct_internal(db);

        // Читаем новую структуру БД
        ws1 = new JdxReplWs(db);
        ws1.init();
        IJdxDbStruct structActual_ws1 = ws1.struct;

        // Устанавливаем "разрешенную" структуру
        databaseStructManager_ws1.setDbStructAllowed(structActual_ws1);

        // Устанавливаем "фиксированную" структуру
        databaseStructManager_ws1.setDbStructFixed(structActual_ws1);


        // ---
        // Формируем изменения данных на ws1, отправка в сеть
        test_ws1_makeChange();
        //
        test_ws1_doReplSession();
        //
        test_srv_doReplSession();


        // ---
        // Попытка принять и использовать реплики на ws2
        queInNoDone1 = stateManager_ws2.getQueNoDone("in");

        //
        ws2 = new JdxReplWs(db2);
        ws2.init();
        // Получаем входящие реплики
        ws2.replicasReceive();
        // Применяем входящие реплики
        try {
            ws2.handleAllQueIn();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //
        queInNoDone2 = stateManager_ws2.getQueNoDone("in");

        // Применение реплик приостановлено
        assertEquals(queInNoDone1, queInNoDone2);


        // ---
        // Начинаем и завершаем изменения в структуре БД ws2

        // Делаем изменения в структуре БД
        modifyDbStruct_internal(db2);
        ws2 = new JdxReplWs(db2);
        ws2.init();
        IJdxDbStruct structActual_ws2 = ws2.struct;

        // Устанавливаем "разрешенную" структуру
        databaseStructManager_ws2.setDbStructAllowed(structActual_ws2);

        // Устанавливаем "фиксированную" структуру
        databaseStructManager_ws2.setDbStructFixed(structActual_ws2);


        // ---
        // Попытка принять и использовать реплики
        queInNoDone1 = stateManager_ws2.getQueNoDone("in");

        //
        ws2 = new JdxReplWs(db2);
        ws2.init();
        // Получаем входящие реплики
        ws2.replicasReceive();
        // Применяем входящие реплики
        ws2.handleAllQueIn();

        //
        queInNoDone2 = stateManager_ws2.getQueNoDone("in");

        // Применение реплик проходит нормально
        assertNotSame(queInNoDone1, queInNoDone2);
    }


    /**
     * Проверка пограничных состояний:
     * Станция не формирует реплики при несовпадении структур:
     * - если "реальная" структура не совпадет с "зафиксированной";
     * - если "реальная" структура не совпадет с "разрешенной".
     */
    @Test
    public void test_No_HandleSelfAudit() throws Exception {
        JdxReplWs ws1;
        //
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);


        // ---
        // Проверяем, что разрешенная, фиксированная и реальная структуры совпадают на ws1
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);


        // ---
        // Проверяем, что можно формировать реплики
        assert_handleSelfAudit(db, true);


        // ---
        // Делаем изменения в структуре БД ws1
        modifyDbStruct_internal(db);

        // Читаем получившуюся структуру
        ws1 = new JdxReplWs(db);
        ws1.init();
        IJdxDbStruct structActual = ws1.struct;


        // ---
        // Последовательно делаем шаги по фиксации изменения структуры,
        // проверяем, что реплики пойдут только после завершения смены версии

        // Проверяем, что реплики формировать не удается
        assert_handleSelfAudit(db, false);

        // Устанавливаем "разрешенную" структуру
        databaseStructManager.setDbStructAllowed(structActual);

        // Проверяем, что реплики формировать не удается
        assert_handleSelfAudit(db, false);

        // Проверяем, что реплики формировать не удается
        assert_handleSelfAudit(db, false);

        // Устанавливаем "фиксированную" структуру
        databaseStructManager.setDbStructFixed(structActual);

        // Проверяем, что можно формировать реплики
        assert_handleSelfAudit(db, true);
    }


    /**
     * Инициализация и прогон полного цикла смены структуры БД
     */
    @Test
    public void test_ModifyDbStruct() throws Exception {
        doModifyDbStruct();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
    }

    /**
     * Инициализация и прогон полного цикла смены структуры БД,
     * три раза
     */
    @Test
    public void test_modifyDbStruct_triple() throws Exception {
        doModifyDbStruct();
        doModifyDbStruct();
        doModifyDbStruct();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
    }

    /**
     * Прогон полного цикла смены структуры БД
     */
    void doModifyDbStruct() throws Exception {
        JdxReplWs ws;
        JdxReplWs ws2;
        JdxReplWs ws3;


        // ---
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
        System.out.println("ws1 tables count: " + ws.struct.getTables().size());
        System.out.println("ws2 tables count: " + ws2.struct.getTables().size());
        System.out.println("ws3 tables count: " + ws3.struct.getTables().size());
        //
        assertEquals("Перед тестом структура ws и ws2 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws2.struct));
        assertEquals("Перед тестом структура ws и ws3 должны совпадать", true, UtDbComparer.dbStructIsEqual(ws.struct, ws3.struct));


        // ---
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(3, db.loadSql(sqlEnabledIs1MuteIs0).size());


        // ---
        // Вносим изменения в данные на станциях
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // ---
        // Начинаем (на сервере) смену версии БД - формируем сигнал "всем молчать"
        doSrvMuteAll();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        System.out.println("Проверяем на сервере ответ на сигнал - все должны быть MUTE");
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(0, db.loadSql(sqlEnabledIs1MuteIs0).size());


        // ---
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        assert_handleSelfAudit(db, false);
        assert_handleSelfAudit(db2, false);
        assert_handleSelfAudit(db3, false);


        // ---
        // Физически меняем свою структуру на сервере
        modifyDbStruct_internal(db);

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        assert_handleSelfAudit(db, false);
        assert_handleSelfAudit(db2, false);
        assert_handleSelfAudit(db3, false);


        // ---
        // Завершаем (на сервере) смену версии БД

        // На сервере напрямую задаем структуру публикаций (команда repl_set_cfg)
        // Обновляем конфиг cfg_publications своей "серверной" рабочей станции
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfg_publications);
        CfgManager cfgManager = new CfgManager(db);
        cfgManager.setSelfCfg(cfg, CfgType.PUBLICATIONS);

        // От сервера рассылаем...
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // ... рассылаем на рабочие станции новые правила публикаций через сигнал "SET_DB_STRUCT"
        srv.srvSetAndSendDbStruct(cfg_publications, 1, UtQue.SRV_QUE_OUT001);
        srv.srvSetAndSendDbStruct(cfg_publications, 2, UtQue.SRV_QUE_OUT001);
        srv.srvSetAndSendDbStruct(cfg_publications, 3, UtQue.SRV_QUE_OUT001);

        // ... Завершаем (на сервере) смену версии БД - рассылаем всем сигнал "UNMUTE"
        // Эта команда не будет обработана станциями из-за разницы dbStructCrc, пока они не примут структуру.
        System.out.println("Рассылаем всем сигнал UNMUTE");
        //srv.srvUnmuteAll();
        srv.srvSendWsUnmute(1, UtQue.SRV_QUE_OUT001);
        srv.srvSendWsUnmute(2, UtQue.SRV_QUE_OUT001);
        srv.srvSendWsUnmute(3, UtQue.SRV_QUE_OUT001);

        // ---
        System.out.println("Проверяем, что все станции пока молчат");
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(0, db.loadSql(sqlEnabledIs1MuteIs0).size());

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        System.out.println("Проверяем на сервере ответ на сигнал - все должны быть MUTE, кроме сервера");
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        // Только сервер изменил структуру и перестал молчать, а станции еще не смогли сделать SET_DB_STRUCT,
        // потому что у них реальная структура не совпадает с разрешенной (не хватает новых таблиц TEST_TABLE_*)
        assertEquals(1, db.loadSql(sqlEnabledIs1MuteIs0).size());

        // ---
        System.out.println("Убеждаемся что рабочие станции молчат (из-из запрета), а сервер нет");
        assert_handleSelfAudit(db, true);
        assert_handleSelfAudit(db2, false);
        assert_handleSelfAudit(db3, false);


        // ---
        System.out.println("Физически меняем структуру на рабочих станциях ws2 и ws3");
        modifyDbStruct_internal(db2);
        modifyDbStruct_internal(db3);


        // ---
        System.out.println("Убеждаемся что рабочие станции молчат (из-за незафиксированности структуры), а сервер нет");
        assert_handleSelfAudit(db, true);
        assert_handleSelfAudit(db2, false);
        assert_handleSelfAudit(db3, false);


        // ---
        System.out.println("Даем станции обработать команды с сервера и тем самым заставляем зафиксировать смену структуру БД");
        System.out.println("Сработает реакция на SET_DB_STRUCT");
        test_ws2_doReplSession();
        test_ws3_doReplSession();
        System.out.println("Сработает реакция на UNMUTE");
        test_ws2_doReplSession();
        test_ws3_doReplSession();

        // ---
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        System.out.println("Убеждаемся что все рабочие станции говорят");
        assert_handleSelfAudit(db, true);
        assert_handleSelfAudit(db2, true);
        assert_handleSelfAudit(db3, true);

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        // Проверяем на сервере ответы на сигнал UNMUTE - проверяем состояние MUTE у станций
        System.out.println("Проверяем на сервере ответ на сигнал - все должны быть UNMUTE");
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(3, db.loadSql(sqlEnabledIs1MuteIs0).size());
    }

    /**
     * Проверяет корректность формирования аудита при цикле вставки и удаления влияющей записи:
     */
    @Test
    public void test_auditAfterInsDel() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.setUp();
        utTest.make_Region_InsDel_0(struct2, 2);

        // Формирование аудита
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();
        ws.handleSelfAudit();
    }

    /**
     * ВНИМАНИЕ! Перед запуском теста нужно:
     * <p>
     * 1) прогнать полный цикл репликации
     * jdtx.repl.main.api.JdxReplWsSrv_Test#test_allSetUp_TestAll()
     * <p>
     * 2) запустить три клиентских и серверный процесы
     * jdtx.repl.main.api.JdxReplWsSrv_Test#loop_srv()
     * jdtx.repl.main.api.JdxReplWsSrv_Test#loop_1_repl()
     * jdtx.repl.main.api.JdxReplWsSrv_Test#loop_2_repl()
     * jdtx.repl.main.api.JdxReplWsSrv_Test#loop_3_repl()
     * jdtx.repl.main.api.JdxReplWsSrv_Test#loop_5_repl()
     */
    @Test
    public void test_srvStateWait() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Печатаем состояние MUTE
        srv.srvMuteState(false, false, 0);

        // Всех в состояние MUTE
        srv.srvMuteAll();
        System.out.println("===");
        System.out.println("wait for MUTE");
        System.out.println("===");
        //
        long waitMuteAge = srv.srvMuteState(true, false, 0);

        // Всех в состояние MUTE с контролем возраста по предыдущему MUTE
        waitMuteAge = waitMuteAge + 1;
        System.out.println("===");
        System.out.println("wait for MUTE age: " + waitMuteAge);
        System.out.println("===");
        //
        srv.srvMuteAll();
        //
        srv.srvMuteState(true, false, waitMuteAge);
        System.out.println("===");

        // Всех в состояние UNMUTE
        srv.srvUnmuteAll();

        //
        srv.srvMuteState(false, true, 0);
        System.out.println("===");
    }

    private void assertEqualsStruct_Actual_Allowed_Fixed_ws(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        //
        IJdxDbStruct structActual = ws.struct;
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);
        IJdxDbStruct structFixed = databaseStructManager.getDbStructFixed();
        IJdxDbStruct structAllowed = databaseStructManager.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structFixed));
    }

    public void doSrvMuteAll() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        srv.srvMuteAll();
    }

    public void doSrv_Unmute() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        srv.srvUnmuteAll();
    }

    /**
     * Проверяем, можно или нет формировать реплики (обрабатывается ли собственный аудит)
     */
    void assert_handleSelfAudit(Db db, boolean auditIsAllowed) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        //
        UtAuditAgeManager uta = new UtAuditAgeManager(db, ws.struct);

        // Исходный возраст аудита
        long auditAge0 = uta.getAuditAge();

        //
        UtTest utTest = new UtTest(db);
        utTest.makeChange(ws.struct, ws.wsId);
        //
        long auditAge1 = uta.getAuditAge();

        // Возраст после обработки аудита
        ws.handleSelfAudit();
        //
        long auditAge2 = uta.getAuditAge();

        //
        assertEquals(true, auditAge0 == auditAge1);
        assertEquals(!auditIsAllowed, auditAge1 == auditAge2);
    }

    public void do_DumpTables(Db db, Db db2, Db db3, IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct struct3) throws Exception {
        reloadDbStructAll(); // Чтобы тестовые фунции работали с новой структурой
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
