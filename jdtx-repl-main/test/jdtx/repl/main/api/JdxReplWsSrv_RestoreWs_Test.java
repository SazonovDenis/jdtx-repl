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

    String workDirName;
    String workDbName;
    String dbBackupName;
    String dirBackupName;
    File workDirFile;
    File workDbFile;
    File dbBackupFile;
    File dirBackupFile;

    @Test
    public void test_assertDbEquals_1_2_3() throws Exception {
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_assertDbEquals_1_2() throws Exception {
        compareDb(db, db2, equalExpected);
    }

    @Test
    public void test_assertDbEquals_full_1_2() throws Exception {
        compareDb(db, db2, expectedEqual_full);
    }

    @Test
    public void test_assertDbEquals_1_3() throws Exception {
        compareDb(db, db3, equalExpected);
    }

    @Test
    public void test_assertDbNotEquals_1_2() throws Exception {
        compareDb(db, db2, expectedNotEqual);
    }

    @Test
    public void test_assertDbNotEquals_1_3() throws Exception {
        compareDb(db, db3, expectedNotEqual);
    }

    void initWsInfo(Db db, String suffix) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.readIdGuid();
        String wsIdStr = "ws_" + UtString.padLeft(String.valueOf(ws.wsId), 3, "0");
        ws.initDataRoot();
        //
        workDirName = ws.dataRoot + wsIdStr;
        workDbName = db.getDbSource().getDatabase();
        dbBackupName = backupDirName + "db_" + wsIdStr + suffix + ".bak";
        dirBackupName = backupDirName + "dir_" + wsIdStr + suffix + ".bak";
        //
        workDirFile = new File(workDirName);
        workDbFile = new File(workDbName);
        dbBackupFile = new File(dbBackupName);
        dirBackupFile = new File(dirBackupName);
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
        initWsInfo(db, "");

        //
        doDisconnectAll();

        //
        System.out.println("Удаляем содержимое: " + workDirName + "(" + workDirFile.getAbsolutePath() + ")");
        UtFile.cleanDir(workDirName);
        FileUtils.forceDelete(workDirFile);

        //
        connectAll();
    }

    void doDeleteDb(Db db) throws Exception {
        initWsInfo(db, "");

        //
        doDisconnectAll();

        //
        System.out.println("Удаляем: " + workDbName + "(" + (workDbFile.getAbsolutePath()) + ")");
        FileUtils.forceDelete(workDbFile);

        //
        connectAll(false);
    }

    void doBackupDB(Db db, String suffix) throws Exception {
        initWsInfo(db, suffix);

        //
        doDisconnectAll();

        //
        if (dbBackupFile.exists()) {
            throw new XError("backupFile exists: " + dbBackupFile);
        }
        //
        FileUtils.copyFile(workDbFile, dbBackupFile);

        //
        connectAll();
    }

    void doRestoreDB(Db db, String suffix) throws Exception {
        initWsInfo(db, suffix);

        //
        doDisconnectAll();

        //
        FileUtils.forceDelete(workDbFile);
        FileUtils.copyFile(dbBackupFile, workDbFile);

        //
        connectAll();
    }

    void doBackupDir(Db db, String suffix) throws Exception {
        initWsInfo(db, suffix);

        //
        doDisconnectAll();

        //
        if (dirBackupFile.exists()) {
            throw new XError("backupFile exists: " + dirBackupFile);
        }

        //
        UtZip.doZipDir(workDirName, dirBackupName);

        //
        connectAll();
    }


    void doRestoreDir(Db db, String suffix) throws Exception {
        initWsInfo(db, suffix);

        //
        doDisconnectAll();

        //
        UtFile.cleanDir(workDirName);
        UtZip.doUnzipDir(dirBackupName, workDirName);

        //
        connectAll();
    }


}
