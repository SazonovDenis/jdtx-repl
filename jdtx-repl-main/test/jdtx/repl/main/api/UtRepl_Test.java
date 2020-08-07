package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.replica.*;
import org.junit.*;

/**
 */
public class UtRepl_Test extends JdxReplWsSrv_ChangeDbStruct_Test {

    /**
     * Проверяем команды MUTE, UNMUTE
     */
    @Test
    public void testWsMuteUnmute() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Цикл синхронизации
        sync_http();
        sync_http();


        // ===
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Станция 2 MUTE
        srv.srvSetWsMute(2);

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(2, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(1, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции MUTE
        srv.srvSetWsMute(0);

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем, что все станции MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Станция 3 UNMUTE
        srv.srvSetWsUnmute(3);

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(1, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(2, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции UNMUTE
        srv.srvSetWsUnmute(0);

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем, что все станции UNMUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));
    }

    /**
     * Проверяем, повторную отправку MUTE и возрастание отметки возраста -
     * нужно для гарантирования отсутствия реплик в репликационной сети при смене структуры базы.
     */
    @Test
    public void testWsMuteUnmute_TwoStep() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        srv.srvSetWsUnmute(0);

        // Первичный цикл синхронизации
        sync_http();
        sync_http();
        sync_http();


        // ===
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции MUTE
        srv.srvSetWsMute(0);

        // Цикл синхронизации только со станцией 2
        sync_http_1_2();
        sync_http_1_2();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(1, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(2, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Делаем изменения на станции 3, которые позже попадут на остальные станции
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init();
        for (int i = 0; i < 15; i++) {
            // Измемения
            test_ws3_makeChange();
            // Попытка репликации, с имитацией отсутствия связи
            ws3.handleSelfAudit();
            ws3.handleQueIn();
        }

        // Цикл синхронизации только со станцией 3
        sync_http_3();
        sync_http_3();

        // Проверяем, что все станции MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Будем ждать этого возраста после повторной команды
        long waitForMinAge = db.loadSql("select max(mute_age) max_age from z_z_state_ws where enabled = 1").getCurRec().getValueInt("max_age");
        System.out.println("Wait for min age: " + waitForMinAge);


        // ===
        // Все станции MUTE - повторная команда
        System.out.println("Все станции MUTE - повторная команда");
        srv.srvSetWsMute(0);


        // ===
        // Цикл синхронизации только со станцией 1
        sync_http_1();
        sync_http_1();

        // Еще не все
        System.out.println("Wait for min age: " + waitForMinAge);
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        long minMiteAge = db.loadSql("select min(mute_age) min_age from z_z_state_ws where enabled = 1").getCurRec().getValueInt("min_age");
        assertEquals(false, minMiteAge >= waitForMinAge);

        // Еще не все, однако 1 уже все
        long miteAge1 = db.loadSql("select mute_age from z_z_state_ws where ws_id = 1").getCurRec().getValueInt("mute_age");
        assertEquals(true, miteAge1 >= waitForMinAge);


        // ===
        // Цикл синхронизации только со станцией 3
        sync_http_3();
        sync_http_3();

        // Еще не все
        System.out.println("Wait for min age: " + waitForMinAge);
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        minMiteAge = db.loadSql("select min(mute_age) min_age from z_z_state_ws where enabled = 1").getCurRec().getValueInt("min_age");
        assertEquals(false, minMiteAge >= waitForMinAge);


        // ===
        // Цикл синхронизации только со станцией 2
        sync_http_2();
        sync_http_2();

        // Теперь все
        System.out.println("Wait for min age: " + waitForMinAge);
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        minMiteAge = db.loadSql("select min(mute_age) min_age from z_z_state_ws where enabled = 1").getCurRec().getValueInt("min_age");
        assertEquals(true, minMiteAge >= waitForMinAge);


        // ===
        // Все станции UNMUTE
        srv.srvSetWsUnmute(0);

        // Цикл синхронизации
        sync_http();
        sync_http();

        // Проверяем, что все станции UNMUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));
    }

    @Test
    public void test_findRecordInReplicas() throws Exception {
        UtRepl utRepl = new UtRepl(null, struct);
        IReplica replica = utRepl.findRecordInReplicas("lic", "11:1418", "d:/t/Anet/temp", false);
        if (replica == null) {
            System.out.println("Not found");
        } else {
            System.out.println("Found, file: " + replica.getFile().getAbsolutePath());
        }
    }

    @Test
    public void test_findRecordInReplicas_all() throws Exception {
        UtRepl utRepl = new UtRepl(null, struct);
        IReplica replica = utRepl.findRecordInReplicas("lic", "3:1001", "../_test-data/_test-data_ws2/ws_002/queIn", false);
        if (replica == null) {
            System.out.println("Not found");
        } else {
            System.out.println("Found, file: " + replica.getFile().getAbsolutePath());
        }
    }


}
