package jdtx.repl.main.api;

import jandcode.utils.*;
import org.junit.*;

import java.io.*;

/**
 * Проверка восстановления репликации рабочей станции при восстановлении базы/папок из бэкапа.
 * <p>
 * Шаг test_DatabaseRestore_stepRepair - исправляет ошибку
 */
public class JdxReplWsSrv_RestoreWs_DbRestore_test extends JdxReplWsSrv_RestoreWs_Test {


    /**
     * Проверка при восстановлении устаревшей базы из бэкапа,
     * но при сохранении репликационных каталогов
     */
    @Test
    public void test_Db1() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийное событие
        System.out.println("Аварийное событие");

        // Восстановление старого состояния БД
        doRestoreDB(db2, "_1");


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


    /**
     * Проверка при восстановлении устаревшей базы из бэкапа,
     * но при сохранении репликационных каталогов
     */
    @Test
    public void test_Db2() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийное событие
        System.out.println("Аварийное событие");

        // Восстановление старого состояния БД
        doRestoreDB(db2, "_2");


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


    /**
     * Проверка при сохранении базы,
     * но при устаревшем состоянии репликационных каталогов
     */
    @Test
    public void test_Dir1() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийное событие
        System.out.println("Аварийное событие");

        // Восстановление строго состояния каталогов
        doRestoreDir(db2, "_1");


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


    /**
     * Проверка при сохранении базы,
     * но при полной потере репликационных каталогов,
     */
    @Test
    public void test_DirClean() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийное событие
        System.out.println("Аварийное событие");

        // Полная потеря рабочих каталогов для ws2
        doDeleteDir(db2);


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


    /**
     * Проверка при восстановлении устаревшей базы из бэкапа,
     * и при устаревшем состоянии репликационных каталогов,
     * при этом база более старая, чем каталоги
     */
    @Test
    public void test_Db1_Dir2() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийные события
        System.out.println("Аварийные события");

        // Восстановление старого состояния БД
        doRestoreDB(db2, "_1");

        // Восстановление строго состояния каталогов
        doRestoreDir(db2, "_2");


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


    /**
     * Проверка при восстановлении устаревшей базы из бэкапа,
     * и при устаревшем состоянии репликационных каталогов,
     * при этом каталоги более старые, чем база
     */
    @Test
    public void test_Db2_Dir1() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийные события
        System.out.println("Аварийные события");

        // Восстановление старого состояния БД
        doRestoreDB(db2, "_2");

        // Восстановление строго состояния каталогов
        doRestoreDir(db2, "_1");


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


    /**
     * Проверка при восстановлении устаревшей базы из бэкапа,
     * и полной потере репликационных каталогов
     */
    @Test
    public void test_Db1_DirClean() throws Exception {
        // ---
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийные события
        System.out.println("Аварийные события");

        // Восстановление старого состояния БД
        doRestoreDB(db2, "_1");

        // Полная потеря рабочих каталогов для ws2
        doDeleteDir(db2);


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }


/*
    @Test
    public void test_DatabaseRestore_stepRepair() throws Exception {
        doRepair();
    }
*/


    private void doRepair() throws Exception {
        // Первая попытка ремонта
        System.out.println();
        System.out.println("Первая попытка ремонта");

        // Разница должна быть
        compareDb(db, db2, expectedNotEqual);


        // Сервер ответит на просьбы о повторной отправке
        System.out.println();
        System.out.println("Сервер ответит на просьбы о повторной отправке");
        test_srv_doReplSession();


        // Последняя попытка ремонта
        System.out.println();
        System.out.println("Последняя попытка ремонта");
        doStepRepair(db2, false);
        doStepRepair(db2, false);
        doStepRepair(db2, false);
        doStepRepair(db2, true);


        // Финальная синхронизация
        System.out.println();
        System.out.println("Финальная синхронизация");
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Cинхронизация должна пройти нормально
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv3"));
    }


    // Создание репликации,
    // обычная работа - изменения в базах и синхронизация
    // По дороге создаем две контрольных точки
    private void doSetUp_doNolmalLife_BeforeFail() throws Exception {
        UtFile.mkdirs(backupDirName);
        UtFile.cleanDir(backupDirName);

        // Создание репликации
        allSetUp();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Проверим исходную синхронность после инициализации
        System.out.println("Базы должны быть в синхронном состоянии");
        compareDb(db, db2, expectedEqual_full);
        compareDb(db, db3, expectedEqual_full);


        // ---
        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        // Немного нагрузим 001
        test_srv_make001();

        // Синхронизация
        sync_http_1_2_3();


        // Проверим исходную синхронность после изменений
        System.out.println("Базы должны быть в синхронном состоянии");
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);


        // В бэкапе будет исходное состояние
        doBackupDB(db2, "");
        doBackupDir(db2, "");


        // Изменения в базах, частичная синхронизация
        doNolmalLife_Step();

        // Сохраним "бэкап" базы и папок для ws2
        doBackupDB(db2, "_1");
        doBackupDir(db2, "_1");


        // Изменения в базах, частичная синхронизация
        doNolmalLife_Step();

        // Сохраним "бэкап" базы и папок для ws2
        doBackupDB(db2, "_2");
        doBackupDir(db2, "_2");
    }


    // Изменения в базах, синхронизация,
    // снова изменения в базах, "неполная" синхронизация ws2 -
    // реплики останутся только в очереди (папке) ws2 (но НЕ отправлены на сервер)
    private void doNolmalLife_Step() throws Exception {
        // Изменения в базах, синхронизация
        for (int i = 0; i <= 2; i++) {
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();

            //
            sync_http_1_2_3();
            sync_http_1_2_3();
        }

        // Изменения в базах
        for (int i = 0; i <= 2; i++) {
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();
        }

        // "Неполная" синхронизация ws2 -
        // реплики останутся только в очередях (папках) ws2 (но НЕ отправлены на сервер)
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        ws2.handleSelfAudit();

        // Изменения в базах -
        // реплики останутся только в аудите
        for (int i = 0; i <= 2; i++) {
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();
        }
    }


    private void doLife_AfterFail() throws Exception {
        // Попытка синхронизации (неудачная для ws2)
        sync_http_1_2_3();

        //
        System.out.println("Попытка синхронизации была неудачная");
        compareDb(db, db2, expectedNotEqual);
        compareDb(db, db3, equalExpected);

        // Изменения в базах (добавим Ulz)
        UtTest utTest;
        utTest = new UtTest(db);
        utTest.makeChange_AddUlz(struct, 1);

        utTest = new UtTest(db2);
        utTest.makeChange_AddUlz(struct2, 2);

        utTest = new UtTest(db3);
        utTest.makeChange_AddUlz(struct3, 3);

        // Попытка синхронизации (неудачная для ws2)
        sync_http_1_2_3();

        //
        System.out.println("Попытка синхронизации была неудачная");
        compareDb(db, db2, expectedNotEqual);
        compareDb(db, db3, equalExpected);
    }


}
