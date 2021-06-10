package jdtx.repl.main.api;

import org.junit.*;

public class ReplAppUpdate_test extends JdxReplWsSrv_Test {

    @Test
    public void test_Init_AppUpdate() throws Exception {
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();
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
        srv.init();

        //
        String exeFileName = "Z:/jdtx-repl/install/JadatexSync-664.exe";
        srv.srvAppUpdate(exeFileName);

        // ===
        // Цикл синхронизации ws1
        sync_ws1();
        sync_ws1();

        // ===
        // Цикл общей синхронизации
        sync_http_1_2_3();
        sync_http_1_2_3();
    }


    /**
     * Цикл синхронизации
     */
    @Test
    public void sync_ws1() throws Exception {
        test_ws1_doReplSession();
        //
        test_srv_doReplSession();
        //
        test_ws1_doReplSession();
    }
}
