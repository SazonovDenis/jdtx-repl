package jdtx.repl.main.api;

import jandcode.utils.rt.*;
import jdtx.repl.main.ext.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

public class DatabaseRestore_test extends JdxReplWsSrv_Test {

    @Test
    public void test_DatabaseRestore() throws Exception {
        // Первичная инициализация
        allSetUp();
        //
        sync_http();
        sync_http();


        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Синхронизация
        sync_http();
        sync_http();

        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // "Неполная" синхронизация ws2
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();
        JdxReplTaskWs replTask = new JdxReplTaskWs(ws);
        replTask.ws.handleSelfAudit();
        //test_ws2_doReplSession();

        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Сохраним "бэкап" базы для ws2
        testBackup(extWs2);


        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Синхронизация
        sync_http();
        sync_http();

        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // "Откат" базы
        testRestore(extWs2);


        // Синхронизация
        sync_http();
        sync_http();
    }

    private void testRestore(Jdx_Ext extWs) throws Exception {
        doDisconnectAll();
        //
        Rt rt = extWs.getApp().getRt().getChild("db/default");
        String dbName = rt.getValue("database").toString();
        String dbNameBackup = dbName + ".bak";
        FileUtils.copyFile(new File(dbNameBackup), new File(dbName));
        //
        doConnectAll();
    }

    private void testBackup(Jdx_Ext extWs) throws Exception {
        doDisconnectAll();
        //
        Rt rt = extWs.getApp().getRt().getChild("db/default");
        String dbName = rt.getValue("database").toString();
        String dbNameBackup = dbName + ".bak";
        FileUtils.copyFile(new File(dbName), new File(dbNameBackup));
        //
        doConnectAll();
    }

}
