package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import org.junit.*;

/**
 */
public class UtRepl_Test extends JdxReplWsSrv_ChangeDbStruct_Test {

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

}
