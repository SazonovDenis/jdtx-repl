package jdtx.repl.main.api;

import jandcode.utils.rt.*;
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
        doDisconnectAll();
        //
        Rt rt = extWs2.getApp().getRt().getChild("db/default");
        String dbName = rt.getValue("database").toString();
        String dbNameBackup = dbName + ".bak";
        FileUtils.copyFile(new File(dbName), new File(dbNameBackup));
        //
        doConnectAll();


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
        doDisconnectAll();
        //
        FileUtils.copyFile(new File(dbNameBackup), new File(dbName));
        //
        doConnectAll();


        // Синхронизация
        sync_http();
        sync_http();
    }

}
