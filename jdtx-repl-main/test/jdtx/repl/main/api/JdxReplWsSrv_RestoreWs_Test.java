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
    String dataDirName;
    String dbName;
    String dbBackupName;
    String dirBackupName;
    File workDirFile;
    File workDbFile;
    File dbBackupFile;
    File dirBackupFile;

    @Test
    public void test_assertDbEquals_1_2_3() throws Exception {
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
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
        ws.initDataDir(wsId);
        tempDirName = ws.getDataRoot() + "temp/";
        dataDirName = ws.getDataDir();
        dbName = db.getDbSource().getDatabase();
        //
        String wsIdStr = "ws_" + UtString.padLeft(String.valueOf(wsId), 3, "0");
        dbBackupName = backupDirName + "db_" + wsIdStr + suffix + ".bak";
        dirBackupName = backupDirName + "dir_" + wsIdStr + suffix + ".bak";
        //
        workDirFile = new File(dataDirName);
        workDbFile = new File(dbName);
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
        dataDirName = wsDataRoot + wsIdStr;
        dbName = db.getDbSource().getDatabase();
        //dbBackupName = backupDirName + "db_" + wsIdStr + suffix + ".bak";
        dirBackupName = backupDirName + "dir_" + wsIdStr + suffix + ".bak";
        //
        workDirFile = new File(dataDirName);
        workDbFile = new File(dbName);
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
        try {
            JdxReplWs ws = new JdxReplWs(db);
            ws.init();
            ws.repairAfterBackupRestore(true, true);

            //
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
        System.out.println("Удаляем содержимое: " + dataDirName + "(" + workDirFile.getAbsolutePath() + ")");
        UtFile.cleanDir(dataDirName);
        FileUtils.forceDelete(workDirFile);

        //
        connectAll();
    }

    void doDeleteDb(long wsId) throws Exception {
        initWs(wsId, "");

        //
        disconnectAll();

        //
        System.out.println("Удаляем: " + dbName + "(" + (workDbFile.getAbsolutePath()) + ")");
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
        UtZip.doZipDir(dataDirName, dirBackupName);
    }

    void doBackupMail(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        dirBackupName = backupDirName + "mail" + suffix + ".bak";
        dirBackupFile = new File(dirBackupName);
        //
        String mailDirName = workDirFile.getParentFile().getParentFile().getParentFile().getCanonicalPath() + "/_data_root/";
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
        UtFile.cleanDir(dataDirName);
        UtZip.doUnzipDir(dirBackupName, dataDirName);
        //
        UtFile.cleanDir(tempDirName);
        //
        System.out.println("restoreDir: " + dirBackupName + " -> " + dataDirName);
    }

    void doRestoreMailInternal(long wsId, String suffix) throws Exception {
        initWs(wsId, suffix);

        //
        dirBackupName = backupDirName + "mail" + suffix + ".bak";
        dirBackupFile = new File(dirBackupName);
        //
        String mailDirName = workDirFile.getParentFile().getParentFile().getParentFile().getCanonicalPath() + "/_data_root/";
        File mailDirFile = new File(mailDirName);

        //
        UtFile.cleanDir(mailDirName);
        UtZip.doUnzipDir(dirBackupName, mailDirName);
        //
        System.out.println("restoreMail: " + dirBackupName + " -> " + mailDirName);
    }

    /**
     * Сохраним "бэкап" базы и папок для всех станций
     */
    public void doBackupNolmalLife() throws Exception {
        // Рабочие станции
        doBackupDB(1, "_all");
        doBackupDir(1, "_all");
        doBackupDB(2, "_all");
        doBackupDir(2, "_all");
        doBackupDB(3, "_all");
        doBackupDir(3, "_all");
        doBackupDB(5, "_all");
        doBackupDir(5, "_all");

        // Сервер
        doBackupDir(0, "_all");

        // Почтовый каталог
        doBackupMail(1, "_all");
    }

    /**
     * Восстановим базы и папкки для всех станций из "бэкапа"
     */
    public void doRestoreFromNolmalLife() throws Exception {
        disconnectAll();

        // Рабочие станции
        doRestoreDBInternal(1, "_all");
        doRestoreDirInternal(1, "_all");
        doRestoreDBInternal(2, "_all");
        doRestoreDirInternal(2, "_all");
        doRestoreDBInternal(3, "_all");
        doRestoreDirInternal(3, "_all");
        doRestoreDBInternal(5, "_all");
        doRestoreDirInternal(5, "_all");

        // Сервер
        doRestoreDirInternal(0, "_all");

        // Почтовый каталог
        doRestoreMailInternal(1, "_all");

        //
        connectAll();
    }


}
