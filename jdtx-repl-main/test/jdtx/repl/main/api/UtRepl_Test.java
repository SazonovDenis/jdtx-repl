package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;
import java.util.*;

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
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Станция 2 MUTE
        srv.srvSendWsMute(2, "common");

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(2, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(1, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции MUTE
        srv.srvSendWsMute(0, "common");

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем, что все станции MUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(3, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Станция 3 UNMUTE
        srv.srvSendWsUnmute(3, "common");

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(1, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(2, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции UNMUTE
        srv.srvSendWsUnmute(0, "common");

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем, что все станции UNMUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));
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
        srv.srvSendWsUnmute(0, "common");

        // Первичный цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ===
        // Проверяем, что все станции пока работают
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Все станции MUTE
        srv.srvSendWsMute(0, "common");

        // Цикл синхронизации только со станцией 2
        sync_http_1_2();
        sync_http_1_2();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(1, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(2, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Делаем изменения на станции 3, которые позже попадут на остальные станции
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init();
        for (int i = 0; i < 15; i++) {
            // Измемения
            test_ws3_makeChange();
            // Попытка репликации, с имитацией отсутствия связи
            ws3.handleSelfAudit();
            ws3.handleAllQueIn();
        }

        // Цикл синхронизации только со станцией 3
        sync_http_3();
        sync_http_3();

        // Проверяем, что все станции MUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(0, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(3, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));


        // ===
        // Будем ждать этого возраста после повторной команды
        long waitForMinAge = db.loadSql("select max(mute_age) max_age from Z_Z_SRV_WORKSTATION_STATE where enabled = 1").getCurRec().getValueInt("max_age");
        System.out.println("Wait for min age: " + waitForMinAge);


        // ===
        // Все станции MUTE - повторная команда
        System.out.println("Все станции MUTE - повторная команда");
        srv.srvSendWsMute(0, "common");


        // ===
        // Цикл синхронизации только со станцией 1
        sync_http_1();
        sync_http_1();

        // Еще не все
        System.out.println("Wait for min age: " + waitForMinAge);
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        long minMiteAge = db.loadSql("select min(mute_age) min_age from Z_Z_SRV_WORKSTATION_STATE where enabled = 1").getCurRec().getValueInt("min_age");
        assertEquals(false, minMiteAge >= waitForMinAge);

        // Еще не все, однако 1 уже все
        long miteAge1 = db.loadSql("select mute_age from Z_Z_SRV_WORKSTATION_STATE where ws_id = 1").getCurRec().getValueInt("mute_age");
        assertEquals(true, miteAge1 >= waitForMinAge);


        // ===
        // Цикл синхронизации только со станцией 3
        sync_http_3();
        sync_http_3();

        // Еще не все
        System.out.println("Wait for min age: " + waitForMinAge);
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        minMiteAge = db.loadSql("select min(mute_age) min_age from Z_Z_SRV_WORKSTATION_STATE where enabled = 1").getCurRec().getValueInt("min_age");
        assertEquals(false, minMiteAge >= waitForMinAge);


        // ===
        // Цикл синхронизации только со станцией 2
        sync_http_2();
        sync_http_2();

        // Теперь все
        System.out.println("Wait for min age: " + waitForMinAge);
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        minMiteAge = db.loadSql("select min(mute_age) min_age from Z_Z_SRV_WORKSTATION_STATE where enabled = 1").getCurRec().getValueInt("min_age");
        assertEquals(true, minMiteAge >= waitForMinAge);


        // ===
        // Все станции UNMUTE
        srv.srvSendWsUnmute(0, "common");

        // Цикл синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверяем, что все станции UNMUTE
        UtData.outTable(db.loadSql("select * from Z_Z_SRV_WORKSTATION_STATE where enabled = 1"));
        assertEquals(3, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age = 0").getCurRec().getValueInt("cnt"));
        assertEquals(0, db.loadSql("select count(*) cnt from Z_Z_SRV_WORKSTATION_STATE where enabled = 1 and mute_age <> 0").getCurRec().getValueInt("cnt"));
    }

    @Test
    public void test_findRecordInReplicas_all() throws Exception {
        UtRepl utRepl = new UtRepl(null, struct);
        //String dirs = "../_test-data/_test-data_ws2/ws_002/que_in,../_test-data/_test-data_ws2/ws_002/que_in001,../_test-data/_test-data_ws2/ws_002/que_out";
        String dirs = "../_test-data/_test-data_ws2/ws_002/";
        //
        IReplica replica2 = utRepl.findRecordInReplicas("lic", "2:1361", dirs, true, true, "temp/LIC_2_1361-last.zip");
        System.out.println("File: " + replica2.getData().getAbsolutePath());
        System.out.println("");
        //
        IReplica replica0 = utRepl.findRecordInReplicas("lic", "2:1361", dirs, false, false, "temp/LIC_2_1361.zip");
        System.out.println("File: " + replica0.getData().getAbsolutePath());
        //
        IReplica replica1 = utRepl.findRecordInReplicas("lic", "0", dirs, false, false, "temp/LIC_0.zip");
        System.out.println("File: " + replica1.getData().getAbsolutePath());
    }

    @Test
    public void test_replicaRecrate() throws Exception {
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();

        //
        test_ws2_makeChange();
        ws2.handleSelfAudit();

        //
        long no = ws2.queOut.getMaxNo();
        System.out.println("no: " + no);

        // Нынешний вариант реплики
        IReplica replica0 = ws2.queOut.get(no);

        // Копируем для анализа
        System.out.println("Original replica file: " + replica0.getData().getAbsolutePath());
        System.out.println("Original file size: " + replica0.getData().length());
        File newReplicaFile0 = new File("../_test-data/" + ws2.getWsId() + "." + no + ".original.zip");
        FileUtils.copyFile(replica0.getData(), newReplicaFile0);

        // Портим файл реплики
        FileUtils.writeStringToFile(replica0.getData(), "1qaz2wsx");

        // Копируем для анализа
        System.out.println("Damaged replica file: " + replica0.getData().getAbsolutePath());
        System.out.println("Damaged file size: " + replica0.getData().length());
        File newReplicaFile1 = new File("../_test-data/" + ws2.getWsId() + "." + no + ".damaged.zip");
        FileUtils.copyFile(replica0.getData(), newReplicaFile1);

        // Чиним файл реплики
        IReplica replica1 = ws2.recreateQueOutReplica(no);
        //
        System.out.println("Recreated replica file: " + replica0.getData().getAbsolutePath());
        System.out.println("Recreated file size: " + replica0.getData().length());
        // Копируем для анализа
        File newReplicaFile2 = new File("../_test-data/" + ws2.getWsId() + "." + no + ".new.zip");
        FileUtils.copyFile(replica1.getData(), newReplicaFile2);

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
            long no = 10;
            ws2.recreateQueOutReplica(no);
            throw new Exception("Реплику этого типа невозможно пересоздать");
        } catch (Exception e) {
            if (e.getMessage().contains("Реплику этого типа невозможно пересоздать")) {
                throw e;
            }
        }

        System.out.println("Реплику этого типа невозможно пересоздать");
    }

    @Test
    public void test_checkNotOwnId() throws Exception {
        System.out.println("---");
        UtRepl utRepl = new UtRepl(db2, struct2);
        utRepl.checkNotOwnId();
        System.out.println();

/*
        //
        System.out.println("---");
        utRepl.dropReplication();
        utRepl.checkNotOwnId();
        System.out.println();

        //
        System.out.println("---");
        utRepl.createReplication(2, "qazwsxedc");
        utRepl.checkNotOwnId();
        System.out.println();
*/

        //
        System.out.println("---");
        JdxDbUtils dbu = new JdxDbUtils(db2, struct2);
        dbu.insertRec("CommentTip", UtCnv.toMap("id", 1000000000L, "Name", "qazwsx", "CommentMode", 0, "Deleted", 0));
        //
        utRepl.checkNotOwnId();
        System.out.println();
    }

    @Test
    public void test_findFilesInDirs() throws Exception {
        // Формируем каталоги
        String root = new File("temp/dirs").getAbsolutePath() + "/";
        UtFile.cleanDir(root);
        UtFile.mkdirs(root + "dir1");
        UtFile.mkdirs(root + "dir2");
        //
        UtFile.saveString(null, new File(root + "dir1/000000000.zip"));
        UtFile.saveString(null, new File(root + "dir1/000000002.zip"));
        UtFile.saveString(null, new File(root + "dir1/000000003.zip"));
        UtFile.saveString(null, new File(root + "dir1/000000004.zip"));
        UtFile.saveString(null, new File(root + "dir1/000000099.zip"));
        //
        UtFile.saveString(null, new File(root + "dir2/000000001.zip"));
        UtFile.saveString(null, new File(root + "dir2/000000002.zip"));
        UtFile.saveString(null, new File(root + "dir2/000000003.zip"));
        UtFile.saveString(null, new File(root + "dir2/000000004.zip"));
        UtFile.saveString(null, new File(root + "dir2/000000005.zip"));
        UtFile.saveString(null, new File(root + "dir2/000000020.tmp"));
        //
        UtFile.saveString(null, new File(root + "000000000.zip"));
        UtFile.saveString(null, new File(root + "000000010.zip"));
        UtFile.saveString(null, new File(root + "000000090.zip"));

        //
        System.out.println();
        List<File> files = UtRepl.findFilesInDirs(root + "dir2" + "," + root + "dir1", false);
        for (File file : files) {
            System.out.println(file);
        }

        //
        System.out.println();
        files = UtRepl.findFilesInDirs(root, false);
        for (File file : files) {
            System.out.println(file);
        }
    }

    @Test
    public void test_createReplicaSnapshotForTable() throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        //
        UtRepl ut = new UtRepl(db, struct);
        List<IJdxTable> tables = new ArrayList<>();
        tables.add(struct.getTable("CommentTip"));

        //
        List<IReplica> replicasRes = ut.createSnapshotForTablesFiltered(tables, ws.wsId, ws.wsId, ws.publicationOut, false);

        // Переносим файл
        for (int i = 0; i < replicasRes.size(); i++) {
            IReplica replica = replicasRes.get(i);
            File actualFile = new File("temp/" + tables.get(i).getName() + ".zip");
            actualFile.delete();
            FileUtils.moveFile(replica.getData(), actualFile);
            //
            System.out.println(actualFile.getCanonicalPath());
        }
    }


}
