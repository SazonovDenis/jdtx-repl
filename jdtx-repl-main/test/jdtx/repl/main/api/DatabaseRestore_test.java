package jdtx.repl.main.api;

import jandcode.utils.rt.*;
import jdtx.repl.main.ext.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

public class DatabaseRestore_test extends JdxReplWsSrv_Test {

    @Test
    public void test_DatabaseRestore_step1() throws Exception {
        // Первичная инициализация
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Синхронизация
        sync_http_1_2_3();

        // Изменения в базах
        for (int i = 0; i <= 3; i++) {
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();
        }

        // Сохраним "бэкап" базы для ws2
        doBackup(extWs2);


        // Изменения в базах и синхронизация
        for (int i = 0; i <= 3; i++) {
            // Изменения в базах
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();

            // Синхронизация
            sync_http_1_2_3();
        }
        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();


        // "Неполная" синхронизация ws2
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();
        JdxReplTaskWs replTask = new JdxReplTaskWs(ws);
        replTask.ws.handleSelfAudit();


        // "Откат" базы
        doRestore(extWs2);


        // Попытка синхронизации (неудачная)
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);


        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Попытка синхронизации (неудачная)
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_DatabaseRestore_step2() throws Exception {
        // Ремонт
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();
        ws.repairAfterBackupRestore(true);

        // Синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    private void doRestore(Jdx_Ext extWs) throws Exception {
        doDisconnectAll();
        //
        Rt rt = extWs.getApp().getRt().getChild("db/default");
        String dbName = rt.getValue("database").toString();
        String dbNameBackup = dbName + ".bak";
        FileUtils.copyFile(new File(dbNameBackup), new File(dbName));
        //
        doConnectAll();
    }

    private void doBackup(Jdx_Ext extWs) throws Exception {
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
