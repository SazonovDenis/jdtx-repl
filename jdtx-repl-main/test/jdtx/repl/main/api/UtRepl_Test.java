package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 *
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
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Станция 2 MUTE
        srv.srvSetWsMute(2);

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(2, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(1, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции MUTE
        srv.srvSetWsMute(0);

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем, что все станции MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Станция 3 UNMUTE
        srv.srvSetWsUnmute(3);

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(1, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(2, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции UNMUTE
        srv.srvSetWsUnmute(0);

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

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
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


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
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем, что все станции UNMUTE
        UtData.outTable(db.loadSql("select * from z_z_state_ws where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from z_z_state_ws where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));
    }

    @Test
    public void test_findRecordInReplicas() throws Exception {
        UtRepl utRepl = new UtRepl(db, struct);
        long wsId = 1;
        String dirName = "D:/t/Anet/queOut.2.ws_003";

        IReplica replica = utRepl.findRecordInReplicas("lic", "11:1418", dirName, wsId, false);
        if (replica == null) {
            System.out.println("Not found");
        } else {
            System.out.println("Found, file: " + replica.getFile().getAbsolutePath());
        }

        IReplica replica2 = utRepl.findRecordInReplicas("lic", "1418", dirName, wsId, false);
        if (replica2 == null) {
            System.out.println("Not found");
        } else {
            System.out.println("Found, file: " + replica2.getFile().getAbsolutePath());
        }
    }

    @Test
    public void test_findRecordInReplicasWs3() throws Exception {
        UtRepl utRepl = new UtRepl(db3, struct3);
        long wsId = 3;
        String dirName = "D:/t/Anet/queOut.2.ws_003";

        IReplica replica = utRepl.findRecordInReplicas("lic", "1138", dirName, wsId, false, "temp/lic_1138.json");
        if (replica == null) {
            System.out.println("Not found");
        } else {
            System.out.println("Found, file: " + replica.getFile().getAbsolutePath());
        }
    }

    @Test
    public void test_findRecordInReplicas_all() throws Exception {
        UtRepl utRepl = new UtRepl(null, struct);
        long wsId = 1;
        IReplica replica = utRepl.findRecordInReplicas("lic", "3:1001", "../_test-data/_test-data_ws2/ws_002/queIn", wsId, false);
        if (replica == null) {
            System.out.println("Not found");
        } else {
            System.out.println("Found, file: " + replica.getFile().getAbsolutePath());
        }
    }

    @Test
    public void test_replicaRecrate() throws Exception {
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();

        //
        test_ws2_makeChange();
        ws2.handleSelfAudit();

        //
        long age = ws2.queOut.getMaxNo();
        System.out.println("age: " + age);

        // Нынешний вариант реплики
        IReplica replica0 = ws2.queOut.get(age);

        // Копируем для анализа
        System.out.println("Original replica file: " + replica0.getFile().getAbsolutePath());
        System.out.println("Original file size: " + replica0.getFile().length());
        File newReplicaFile0 = new File("../_test-data/" + ws2.getWsId() + "." + +age + ".original.zip");
        FileUtils.copyFile(replica0.getFile(), newReplicaFile0);

        // Портим файл реплики
        FileUtils.writeStringToFile(replica0.getFile(), "1qaz2wsx");

        // Копируем для анализа
        System.out.println("Damaged replica file: " + replica0.getFile().getAbsolutePath());
        System.out.println("Damaged file size: " + replica0.getFile().length());
        File newReplicaFile1 = new File("../_test-data/" + ws2.getWsId() + "." + +age + ".damaged.zip");
        FileUtils.copyFile(replica0.getFile(), newReplicaFile1);

        // Чиним файл реплики
        IReplica replica1 = ws2.recreateQueOutReplicaAge(age);
        //
        System.out.println("Recreated replica file: " + replica0.getFile().getAbsolutePath());
        System.out.println("Recreated file size: " + replica0.getFile().length());
        // Копируем для анализа
        File newReplicaFile2 = new File("../_test-data/" + ws2.getWsId() + "." + +age + ".new.zip");
        FileUtils.copyFile(replica1.getFile(), newReplicaFile2);

        //
        System.out.println("Original replica file: " + newReplicaFile0.getAbsolutePath());
        System.out.println("Damaged replica file : " + newReplicaFile1.getAbsolutePath());
        System.out.println("New replica file     : " + newReplicaFile2.getAbsolutePath());

        //
        assertEquals("Размер файлов", newReplicaFile0.length(), newReplicaFile2.length());
        assertEquals("Размер файлов", false, newReplicaFile1.length() == newReplicaFile2.length());
    }

    @Test
    public void test_replicaRecrateImpossible() throws Exception {
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();

        // Чиним файл реплики
        try {
            long age = 10;
            ws2.recreateQueOutReplicaAge(age);
            throw new Exception("Реплика этого типа не должна пересоздаться");
        } catch (Exception e) {
            if (e.getMessage().contains("Реплика этого типа не должна пересоздаться")) {
                throw e;
            }
        }

        System.out.println("Реплику этого типа невозможно пересоздать");
    }


}
