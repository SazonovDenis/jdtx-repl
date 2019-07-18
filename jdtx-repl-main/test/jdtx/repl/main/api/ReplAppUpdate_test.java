package jdtx.repl.main.api;

import org.junit.*;

public class ReplAppUpdate_test extends JdxReplWsSrv_Test {

    @Test
    public void test_init_AppUpdate() throws Exception {
        allSetUp();
        sync_http();
        sync_http();
        //
        test_AppUpdate();
    }

    /**
     * Проверка отправки реплики на запуск обновления.
     */
    @Test
    public void test_AppUpdate() throws Exception {
        // ===
        // Реплика на обновление
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init(json_srv);

        //
        String exeFileName = "Z:/jdtx-repl/install/JadatexSync-301.exe";
        srv.srvAppUpdate(exeFileName);


        // ===
        // Цикл синхронизации
        sync_ws1();
        sync_ws1();
    }


    /**
     * Цикл синхронизации
     */
    @Test
    public void sync_ws1() throws Exception {
        test_ws1_handleSelfAudit();
        //
        test_ws1_send_receive();
        //
        test_sync_srv();
        //
        test_ws1_send_receive();
        //
        test_ws1_handleQueIn();
    }
}
