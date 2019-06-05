package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ut.*;
import org.junit.*;

import java.util.concurrent.*;

/**
 */
public class JdxReplWsSrv_ChangeDbStruct_Test extends JdxReplWsSrv_Test {


    @Override
    public void setUp() throws Exception {
        super.setUp();
        json_ws = "test/etalon/mail_http_ws_unidirectional.json";
    }

    /**
     * Проверка пограничных состояний:
     * Станция не применяет реплики другой структуры.
     */
    @Test
    public void test_No_ApplyReplicas() throws Exception {
        test_all_setUp();
        sync_http();

        //
        JdxReplWs ws;
        JdxReplWs ws2;
        IJdxDbStruct structActual_ws1;
        IJdxDbStruct structFixed_ws1;
        IJdxDbStruct structAllowed_ws1;
        //
        UtDbStruct_DbRW dbStructRW = new UtDbStruct_DbRW(db);
        UtDbStruct_DbRW dbStructRW_ws2 = new UtDbStruct_DbRW(db2);
        //
        long queInNoDone1;
        long queInNoDone2;
        JdxStateManagerWs stateManager_ws2 = new JdxStateManagerWs(db2);


        // ===
        // Проверяем, что утвержденная, фиксированная и реальная структуры совпадают на ws1 и ws2
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db2);
        //
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        ws2 = new JdxReplWs(db2);
        ws2.init(json_ws);
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
        ws.init(json_ws);
        structActual_ws1 = ws.struct;
        dbStructRW.dbStructSaveAllowed(structActual_ws1);

        // Делаем фиксацию структуры
        ws.dbStructUpdate();

        // Проверяем, что фиксация прошла нормально
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        structActual_ws1 = ws.struct;
        structFixed_ws1 = dbStructRW.getDbStructFixed();
        structAllowed_ws1 = dbStructRW.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structAllowed_ws1));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structFixed_ws1));


        // ===
        // Формируем изменения данных на ws1, отправка в сеть
        test_ws1_makeChange();
        //
        test_ws1_handleSelfAudit();
        //
        test_ws1_send_receive();
        //
        test_sync_srv();


        // ===
        // Попытка принять и использовать реплики на ws2
        queInNoDone1 = stateManager_ws2.getQueInNoDone();
        //
        ws2 = new JdxReplWs(db2);
        ws2.init(json_ws);

        // Получаем входящие реплики
        ws2.receive();
        // Применяем входящие реплики
        ws2.handleQueIn();
        //
        queInNoDone2 = stateManager_ws2.getQueInNoDone();

        // Применение реплик приостановлено
        assertEquals(queInNoDone1, queInNoDone2);


        // ===
        // Изменения структуре БД ws2
        // Делаем изменения структуре БД
        test_ws2_changeDbStruct();

        // Устанавливаем "разрешенную" структуру
        ws2 = new JdxReplWs(db2);
        ws2.init(json_ws);
        structActual_ws1 = ws2.struct;
        dbStructRW_ws2.dbStructSaveAllowed(structActual_ws1);

        // Делаем фиксацию структуры
        ws2.dbStructUpdate();

        // Проверяем, что фиксация прошла нормально
        ws2 = new JdxReplWs(db2);
        ws2.init(json_ws);
        //
        structActual_ws1 = ws2.struct;
        structFixed_ws1 = dbStructRW_ws2.getDbStructFixed();
        structAllowed_ws1 = dbStructRW_ws2.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structAllowed_ws1));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual_ws1, structFixed_ws1));


        // Попытка принять и использовать реплики
        queInNoDone1 = stateManager_ws2.getQueInNoDone();
        //
        ws2 = new JdxReplWs(db2);
        ws2.init(json_ws);

        // Получаем входящие реплики
        ws2.receive();
        // Применяем входящие реплики
        ws2.handleQueIn();
        //
        queInNoDone2 = stateManager_ws2.getQueInNoDone();

        // Применение реплик проходит нормально
        assertNotSame(queInNoDone1, queInNoDone2);
    }

    /**
     * Проверка пограничных состояний:
     * Станция не формирует реплики при несовпадении структур:
     * - если "реальная" структура не совпадет с "зафиксированной".
     * - если "реальная" структура не совпадет с "утвержденной".
     */
    @Test
    public void test_No_HandleSelfAudit() throws Exception {
        test_all_setUp();
        sync_http();

        //
        JdxReplWs ws;
        IJdxDbStruct structActual;
        IJdxDbStruct structFixed;
        IJdxDbStruct structAllowed;
        //
        UtDbStruct_DbRW dbStructRW = new UtDbStruct_DbRW(db);
        UtAuditAgeManager uta = new UtAuditAgeManager(db, struct);


        // ===
        // Проверяем, что утвержденная, фиксированная и реальная структуры совпадают на ws1
        assertEqualsStruct_Actual_Allowed_Fixed_ws(db);


        // ===
        // Проверяем, что можно формировать реплики
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        long auditAge0 = uta.getAuditAge();
        //
        test_ws1_makeChange();
        //
        long auditAge1 = uta.getAuditAge();
        //
        ws.handleSelfAudit();
        //
        long auditAge2 = uta.getAuditAge();
        //
        assertEquals(auditAge0, auditAge1);
        assertNotSame(auditAge1, auditAge2);


        // ===
        // Изменения структуре БД ws1
        test_ws1_changeDbStruct();


        // ===
        // Проверяем, что реплики формировать не удается
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        auditAge0 = uta.getAuditAge();
        //
        test_ws1_makeChange();
        //
        auditAge1 = uta.getAuditAge();
        //
        ws.handleSelfAudit();
        //
        auditAge2 = uta.getAuditAge();
        //
        assertEquals(auditAge0, auditAge1);
        assertEquals(auditAge1, auditAge2);


        // ===
        // Пытаемся сделать фиксацию структуры после изменения структуры
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        ws.dbStructUpdate();

        // Проверяем, что фиксация не удается
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        structActual = ws.struct;
        structFixed = dbStructRW.getDbStructFixed();
        structAllowed = dbStructRW.getDbStructAllowed();
        //
        assertEquals(false, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(false, UtDbComparer.dbStructIsEqual(structActual, structFixed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structAllowed, structFixed));


        // ===
        // Устанавливаем "разрешенную" структуру
        dbStructRW.dbStructSaveAllowed(structActual);


        // ===
        // Проверяем, что реплики формировать не удается
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        auditAge0 = uta.getAuditAge();
        //
        test_ws1_makeChange();
        //
        auditAge1 = uta.getAuditAge();
        //
        ws.handleSelfAudit();
        //
        auditAge2 = uta.getAuditAge();
        //
        assertEquals(auditAge0, auditAge1);
        assertEquals(auditAge1, auditAge2);


        // ===
        // Пытаемся сделать фиксацию структуры
        ws.dbStructUpdate();

        // Проверяем, что фиксация прошла нормально
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        structActual = ws.struct;
        structFixed = dbStructRW.getDbStructFixed();
        structAllowed = dbStructRW.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structFixed));


        // ===
        // Проверяем, что можно формировать реплики
        ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        auditAge0 = uta.getAuditAge();
        //
        test_ws1_makeChange();
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

    private void assertEqualsStruct_Actual_Allowed_Fixed_ws(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init(json_ws);
        //
        IJdxDbStruct structActual = ws.struct;
        UtDbStruct_DbRW dbStructRW = new UtDbStruct_DbRW(db);
        IJdxDbStruct structFixed = dbStructRW.getDbStructFixed();
        IJdxDbStruct structAllowed = dbStructRW.getDbStructAllowed();
        //
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structAllowed));
        assertEquals(true, UtDbComparer.dbStructIsEqual(structActual, structFixed));
    }

    @Test
    public void test_sync_changeDbStruct() throws Exception {
        sync_http();
        test_dumpTables();
        test_changeDbStruct();
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой
    }

    @Test
    public void test_changeDbStruct() throws Exception {
        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // ===
        // Начинаем смену версии БД - формируем сигнал "всем молчать"
        test_srvDbStructStart();

        //
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));


        // ===
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        //
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();


        // ===
        // Меняем свою структуру
        changeDbStruct(db);
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http();
        sync_http();


        // ===
        // Меняем структуру на рабочих станциях
        changeDbStruct(db2);
        changeDbStruct(db3);
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Убеждаемся что рабочие станции молчат (из-за несовпадения струтуры)
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();


        // ===
        // Завершаем смену версии БД - рассылаем сигнал "всем говорить"
        test_srvDbStructFinish();

        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем ответа на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));


        // ===
        // Убеждаемся что рабочие станции говорят
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        //
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        // Цикл синхронизации
        sync_http();
        sync_http();


        // ===
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой
        test_dumpTables();
    }

    @Test
    public void test_ws1_changeDbStruct() throws Exception {
        changeDbStruct(db);
    }

    @Test
    public void test_ws2_changeDbStruct() throws Exception {
        changeDbStruct(db2);
    }

    @Test
    public void test_ws3_changeDbStruct() throws Exception {
        changeDbStruct(db3);
    }

    /**
     * Меняет структуру БД:
     * Удаляет таблицу AppUpdate,
     * добавляет одну таблицу,
     * в таблицу Region добавляет поле
     */
    void changeDbStruct(Db db) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct;

        //
        struct = reader.readDbStruct();
        struct_rw.saveToFile(struct, "../_test-data/dbStruct_0.xml");

        //
        UtTest utTest = new UtTest(db);
        utTest.changeDbStructDropTable("appupdate");
        utTest.changeDbStructAddRandomTable();
        utTest.changeDbStruct("region");

        //
        struct = reader.readDbStruct();
        struct_rw.saveToFile(struct, "../_test-data/dbStruct_1.xml");
    }

    @Test
    /**
     * Проверяет корректность формирования аудита при цикле вставки и удаления влияющей записи:
     */
    public void test_auditAfterInsDel() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.make_InsDel(struct2, 2);

        // Формирование аудита
        test_ws2_handleSelfAudit();
    }


    //
    long waitInterval_SECONDS = 60;
    long waitInterval_SECONDS_short = 5;

    @Test
    public void loop_3_repl() throws Exception {
        while (true) {
            TimeUnit.SECONDS.sleep(waitInterval_SECONDS);

            try {
                // =======================================
                // Проверяем, что никто не молчит
                DataStore st = db.loadSql("select * from z_z_state_ws where enabled = 1");
                int muteCount = 0;
                int noMuteCount = 0;
                for (DataRecord rec : st) {
                    if (rec.getValueInt("mute_age") != 0) {
                        muteCount = muteCount + 1;
                    } else {
                        noMuteCount = noMuteCount + 1;
                    }
                }

                // Ждем пока "заговорят"
                if (muteCount != 0) {
                    UtData.outTable(st);
                    throw new XError("Кто-то молчит, muteCount = " + muteCount);
                }


                // =======================================
                System.out.println("Формируем сигнал 'всем молчать'");
                test_srvDbStructStart();

                //
                UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));


                // =======================================
                System.out.println("Ждем ответа на сигнал - проверяем состояние MUTE");
                //
                while (true) {
                    TimeUnit.SECONDS.sleep(waitInterval_SECONDS_short);

                    // Проверяем
                    st = db.loadSql("select * from z_z_state_ws where enabled = 1");
                    muteCount = 0;
                    noMuteCount = 0;
                    for (DataRecord rec : st) {
                        if (rec.getValueInt("mute_age") != 0) {
                            muteCount = muteCount + 1;
                        } else {
                            noMuteCount = noMuteCount + 1;
                        }
                    }

                    // Все получили сингал "mute"
                    if (noMuteCount == 0) {
                        System.out.println("Все MUTE");
                        UtData.outTable(st);
                        break;
                    }

                    //
                    System.out.println("noMuteCount = " + noMuteCount);
                }


                // =======================================
                System.out.println("Меняем структуру");
                //
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                changeDbStruct(db);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                changeDbStruct(db2);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                changeDbStruct(db3);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                //
                reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой


                // =======================================
                //
                System.out.println("Формируем сигнал 'всем говорить'");
                test_srvDbStructFinish();

                //
                UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));


                // =======================================
                System.out.println("Ждем ответа на сигнал - проверяем состояние UNMUTE");
                //
                while (true) {
                    TimeUnit.SECONDS.sleep(waitInterval_SECONDS_short);

                    // Проверяем
                    st = db.loadSql("select * from z_z_state_ws where enabled = 1");
                    muteCount = 0;
                    noMuteCount = 0;
                    for (DataRecord rec : st) {
                        if (rec.getValueInt("mute_age") != 0) {
                            muteCount = muteCount + 1;
                        } else {
                            noMuteCount = noMuteCount + 1;
                        }
                    }

                    // Все получили сингал "unmute"
                    if (muteCount == 0) {
                        System.out.println("Все UNMUTE");
                        UtData.outTable(st);
                        break;
                    }

                    //
                    System.out.println("muteCount = " + muteCount);
                }

                // Не злоупотребляем частой сменой структуры
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS * 30);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


}
