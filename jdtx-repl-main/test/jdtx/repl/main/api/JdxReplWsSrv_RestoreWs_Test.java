package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Базовый класс для проверки восстановления репликации рабочей станции
 */
public class JdxReplWsSrv_RestoreWs_Test extends JdxReplWsSrv_Test {


    String backupDirName = "temp/backup/";

    String dirName;
    String dbFileName;
    String backupFileName;
    File dbFile;
    File backupFile;

    @Test
    public void test_assertDbEquals_1_2_3() throws Exception {
        assertDbEquals(db, db2);
        assertDbEquals(db, db3);
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_assertDbEquals_1_2() throws Exception {
        assertDbEquals(db, db2);
    }

    @Test
    public void test_assertDbEquals_1_3() throws Exception {
        assertDbEquals(db, db3);
    }

    @Test
    public void test_assertDbNotEquals_1_2() throws Exception {
        assertDbNotEquals(db, db2);
    }

    @Test
    public void test_assertDbNotEquals_1_3() throws Exception {
        assertDbNotEquals(db, db3);
    }

    void initWsInfo(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.readIdGuid();
        String sWsId = "ws_" + UtString.padLeft(String.valueOf(ws.wsId), 3, "0");
        ws.initDataRoot();
        dirName = ws.dataRoot + sWsId;
        dbFileName = db.getDbSource().getDatabase();
        backupFileName = backupDirName + "db_" + sWsId + ".bak";
        dbFile = new File(dbFileName);
        backupFile = new File(backupFileName);
    }


    /**
     * Попытка ремонта
     */
    public void doStepRepair(Db db, boolean doRaise) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        try {
            ws.repairAfterBackupRestore(true, false);
        } catch (Exception e) {
            if (doRaise) {
                throw e;
            }
            System.out.println("Попытка ремонта: " + e.getMessage());
        }
    }

    void doDeleteDir(Db db) throws Exception {
        initWsInfo(db);

        //
        doDisconnectAll();

        //
        System.out.println("Удаляем содержимое: " + dirName + "(" + (new File(dirName).getAbsolutePath()) + ")");
        UtFile.cleanDir(dirName);
        FileUtils.forceDelete(new File(dirName));

        //
        connectAll();
    }

    void doDeleteDb(Db db) throws Exception {
        initWsInfo(db);

        //
        doDisconnectAll();

        //
        System.out.println("Удаляем: " + dbFileName + "(" + (dbFile.getAbsolutePath()) + ")");
        FileUtils.forceDelete(dbFile);

        //
        connectAll(false);
    }

    void doBackupDB(Db db) throws Exception {
        initWsInfo(db);

        //
        doDisconnectAll();

        //
        if (backupFile.exists()) {
            throw new XError("backupFile exists: " + backupFile);
        }
        //
        FileUtils.copyFile(dbFile, backupFile);

        //
        connectAll();
    }

    void doRestoreDB(Db db) throws Exception {
        initWsInfo(db);

        //
        doDisconnectAll();

        //
        FileUtils.forceDelete(dbFile);
        FileUtils.copyFile(backupFile, dbFile);

        //
        connectAll();
    }

    void doBackupDir(Db db) throws Exception {
        initWsInfo(db);

        //
        doDisconnectAll();

        //
        if (backupFile.exists()) {
            throw new XError("backupFile exists: " + backupFile);
        }

        //
        UtTest.doZipDir(dirName, backupFileName);

        //
        connectAll();
    }


    void doRestoreDir(Db db) throws Exception {
        initWsInfo(db);

        //
        doDisconnectAll();

        //
        UtFile.cleanDir(dirName);
        UtTest.doUnzipDir(backupFileName, dirName);

        //
        connectAll();
    }


}
