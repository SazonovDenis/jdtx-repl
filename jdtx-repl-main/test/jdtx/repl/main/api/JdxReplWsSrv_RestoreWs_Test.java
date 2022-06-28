package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.task.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Базовый класс для проверки восстановления репликации рабочей станции
 */
public class JdxReplWsSrv_RestoreWs_Test extends JdxReplWsSrv_Test {


    String backupDirName = "temp/backup/";

    String tempDirName;
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


    void initWs(long wsId, String suffix) throws Exception {
        if (wsId == 0) {
            initSrvInfo(1, suffix);
        } else {
            initWsInfo(wsId, suffix);
        }
    }

    void initWsInfo(long wsId, String suffix) throws Exception {
        Db db = dbList.get(wsId);
        //
        JdxReplWs ws = new JdxReplWs(db);
        ws.initDataRoot();
        String wsDataRoot = ws.getDataRoot();
        //
        String wsIdStr = "ws_" + UtString.padLeft(String.valueOf(wsId), 3, "0");
        //
        tempDirName = wsDataRoot + "temp/";
        workDirName = wsDataRoot + wsIdStr;
        workDbName = db.getDbSource().getDatabase();
        dbBackupName = backupDirName + "db_" + wsIdStr + suffix + ".bak";
        dirBackupName = backupDirName + "dir_" + wsIdStr + suffix + ".bak";
        //
        workDirFile = new File(workDirName);
        workDbFile = new File(workDbName);
        dbBackupFile = new File(dbBackupName);
        dirBackupFile = new File(dirBackupName);
    }

    void initSrvInfo(long wsId, String suffix) throws Exception {
        Db db = dbList.get(wsId);
        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.initDataRoot();
        String wsDataRoot = srv.getDataRoot();
        //
        String wsIdStr = "srv";
        //
        tempDirName = wsDataRoot + "temp/";
        workDirName = wsDataRoot + wsIdStr;
        workDbName = db.getDbSource().getDatabase();
        //dbBackupName = backupDirName + "db_" + wsIdStr + suffix + ".bak";
        dirBackupName = backupDirName + "dir_" + wsIdStr + suffix + ".bak";
        //
        workDirFile = new File(workDirName);
        workDbFile = new File(workDbName);
        //dbBackupFile = new File(dbBackupName);
        dirBackupFile = new File(dirBackupName);
    }


    /**
     * Сделаем вид, что мы с сервера отдали команду "можно ремонтировать",
     * т.е. создали файл "repair.info" в почтовом каталоге рабочей станции.
     * Теперь внутри процесса репликации отработает repairAfterBackupRestore с запуском ремонта.
     */
    public void checkNeedRepair_doAllowRepair(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        // Дадим станции возможность обнаружить проблему
        JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws);
        replTask.doTask();

        // Разрешим ремонт
        IVariantMap args = new VariantMap();
        args.put("ws", ws.wsId);
        extSrv.repl_allow_repair(args);
    }

    /**
     * Попытка ремонта
     */
    public void doStepRepair(Db db, boolean doRaise) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        try {
            ws_doReplSession(db);
        } catch (Exception e) {
            if (doRaise) {
                throw e;
            }
            System.out.println("Попытка ремонта: " + e.getMessage());
        }
    }

    void doDeleteDir(long wsId) throws Exception {
        initWs(wsId, "");

        //
        disconnectAll();

        //
        System.out.println("Удаляем содержимое: " + workDirName + "(" + workDirFile.getAbsolutePath() + ")");
        UtFile.cleanDir(workDirName);
        FileUtils.forceDelete(workDirFile);

        //
        connectAll();
    }

    void doDeleteDb(long wsId) throws Exception {
        initWs(wsId, "");

        //
        disconnectAll();

        //
        System.out.println("Удаляем: " + workDbName + "(" + (workDbFile.getAbsolutePath()) + ")");
        FileUtils.forceDelete(workDbFile);

        //
        connectAll(false);
    }

    void doBackupDB(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        disconnectAll();

        //
        if (dbBackupFile.exists()) {
            throw new XError("backupFile exists: " + dbBackupFile);
        }
        //
        FileUtils.copyFile(workDbFile, dbBackupFile);

        //
        connectAll();
    }

    void doBackupDir(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        if (dirBackupFile.exists()) {
            throw new XError("backupFile exists: " + dirBackupFile);
        }

        //
        UtZip.doZipDir(workDirName, dirBackupName);
    }

    void doBackupMail(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        dirBackupName = backupDirName + "mail" + suffix + ".bak";
        dirBackupFile = new File(dirBackupName);
        //
        String mailDirName = workDirFile.getParentFile().getParentFile().getParentFile().getCanonicalPath() + "/_lombard.systems_04/";
        File mailDirFile = new File(mailDirName);

        //
        disconnectAll();

        //
        if (dirBackupFile.exists()) {
            throw new XError("backupFile exists: " + dirBackupFile);
        }

        //
        UtZip.doZipDir(mailDirName, dirBackupName);

        //
        connectAll();
    }

    void doRestoreDB(long wsId, String suffix) throws Exception {
        disconnectAllForce();
        doRestoreDBInternal(wsId, suffix);
        connectAll();
    }

    void doRestoreDir(long wsId, String suffix) throws Exception {
        disconnectAll();
        doRestoreDirInternal(wsId, suffix);
        connectAll();
    }

    void doRestoreMail(long wsId, String suffix) throws Exception {
        disconnectAll();
        doRestoreMailInternal(wsId, suffix);
        connectAll();
    }

    void doRestoreDBInternal(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        if (workDbFile.exists()) {
            FileUtils.forceDelete(workDbFile);
        }
        FileUtils.copyFile(dbBackupFile, workDbFile);
        //
        System.out.println("restoreDB: " + dbBackupFile + " -> " + workDbFile);
    }

    void doRestoreDirInternal(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        UtFile.cleanDir(workDirName);
        UtZip.doUnzipDir(dirBackupName, workDirName);
        //
        UtFile.cleanDir(tempDirName);
        //
        System.out.println("restoreDir: " + dirBackupName + " -> " + workDirName);
    }

    void doRestoreMailInternal(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        dirBackupName = backupDirName + "mail" + suffix + ".bak";
        dirBackupFile = new File(dirBackupName);
        //
        String mailDirName = workDirFile.getParentFile().getParentFile().getParentFile().getCanonicalPath() + "/_lombard.systems_04/";
        File mailDirFile = new File(mailDirName);

        //
        UtFile.cleanDir(mailDirName);
        UtZip.doUnzipDir(dirBackupName, mailDirName);
        //
        System.out.println("restoreMail: " + dirBackupName + " -> " + mailDirName);
    }


}
