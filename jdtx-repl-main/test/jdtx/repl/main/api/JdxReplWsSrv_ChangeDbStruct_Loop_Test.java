package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.error.*;
import jdtx.repl.main.ut.*;
import org.junit.*;

import java.util.concurrent.*;

public class JdxReplWsSrv_ChangeDbStruct_Loop_Test extends JdxReplWsSrv_ChangeDbStruct_Test {

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
                modifyDbStruct_internal(db);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                modifyDbStruct_internal(db2);
                TimeUnit.SECONDS.sleep(waitInterval_SECONDS);
                modifyDbStruct_internal(db3);
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
