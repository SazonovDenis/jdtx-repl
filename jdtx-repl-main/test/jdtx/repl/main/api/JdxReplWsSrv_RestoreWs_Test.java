package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.rt.*;
import jdtx.repl.main.ext.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Базовый класс для проверки восстановления репликации рабочей станции
 */
public class JdxReplWsSrv_RestoreWs_Test extends JdxReplWsSrv_Test {


    @Test
    public void test_assertDbEquals_1_2_3() throws Exception {
        assertDbEquals_1_2_3();
    }

    @Test
    public void test_assertDbNotEquals_1_2_3() throws Exception {
        assertDbNotEquals_1_2_3();
    }

    void doDelete_Dir(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        String sWsId = "ws_" + UtString.padLeft(String.valueOf(ws.wsId), 3, "0");
        String dirName = ws.dataRoot + sWsId;

        System.out.println("Удаляем содержимое: " + dirName + "(" + (new File(dirName).getAbsolutePath()) + ")");

        UtFile.cleanDir(dirName);
        new File(dirName).delete();
    }

    void doDelete_DirDb(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        String dirName = "../_test-data/_test-data_ws" + ws.wsId;

        System.out.println("Удаляем содержимое: " + dirName + "(" + (new File(dirName).getAbsolutePath()) + ")");

        doDisconnectAllForce();
        UtFile.cleanDir(dirName);
        new File(dirName).delete();
    }

    void doRestore(Jdx_Ext extWs) throws Exception {
        doDisconnectAll();
        //
        Rt rt = extWs.getApp().getRt().getChild("db/default");
        String dbName = rt.getValue("database").toString();
        // Удаление базы
        new File(dbName).delete();
        // Восстановление
        String dbNameBackup = dbName + ".bak";
        FileUtils.copyFile(new File(dbNameBackup), new File(dbName));
        //
        doConnectAll();
    }

    void doBackup(Jdx_Ext extWs) throws Exception {
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
