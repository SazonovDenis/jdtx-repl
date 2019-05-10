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


    @Test
    public void test_sync_changeDbStruct() throws Exception {
        sync_http();
        test_dumpTables();
        test_changeDbStruct();
    }

    @Test
    public void test_changeDbStruct() throws Exception {
        //
        test_ws2_makeChange();
        test_ws3_makeChange();

        // ===
        // Формируем сигнал "всем молчать"
        test_srvStart_DbStruct();

        //
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));


        // ===
        // Убеждаемся что рабочие станции молчат (из-из запрета)
        test_ws2_makeChange();
        test_ws3_makeChange();

        //
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();


        // ===
        // Меняем свою структуру
        test_ws_changeDbStruct(db);

        //
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http();
        sync_http();


        // ===
        // Меняем структуру на рабочих станциях
        test_ws_changeDbStruct(db2);
        test_ws_changeDbStruct(db3);
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой

        //
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Убеждаемся что рабочие станции молчат (из-за несовпадения струтуры)
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();


        // ===
        // Рассылаем сигнал "всем говорить"
        test_srv_Finish_DbStruct();

        //
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем ответа на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));


        // ===
        // Убеждаемся что рабочие станции говорят
        test_ws2_makeChange();
        test_ws3_makeChange();

        //
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        // Цикл синхронизации
        sync_http();
        sync_http();


        // ===
        reloadStruct_forTest(); // Чтобы тестовые фунции работали с новой структурой
        test_dumpTables();
    }

/*
    @Test
    public void test_ws1_changeDbStruct() throws Exception {
        test_ws_changeDbStruct(db);
    }

    @Test
    public void test_ws1_changeDb2Struct() throws Exception {
        test_ws_changeDbStruct(db2);
    }

    @Test
    public void test_ws1_changeDb3Struct() throws Exception {
        test_ws_changeDbStruct(db3);
    }
*/

    void test_ws_changeDbStruct(Db db) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct;

        //
        struct = reader.readDbStruct();
        struct_rw.saveToFile(struct, "../_test-data/dbStruct_0.xml");

        //
        UtTest utTest = new UtTest(db);
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
                //
                test_srvStart_DbStruct();

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
                test_ws_changeDbStruct(db);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                test_ws_changeDbStruct(db2);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                test_ws_changeDbStruct(db3);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                //
                reloadStruct_forTest();


                // =======================================
                //
                System.out.println("Формируем сигнал 'всем говорить'");
                test_srv_Finish_DbStruct();

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
