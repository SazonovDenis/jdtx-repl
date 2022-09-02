package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.variant.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 * Проверка восстановления репликации рабочей станции при восстановлении базы/папок из бэкапа.
 */
public class JdxReplWsSrv_RestoreWs_DbRestore_test extends JdxReplWsSrv_RestoreWs_Test {

    boolean doNolmalLifeBromBackup = false;

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
        doRestoreDB(2, "_1");


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
        doRestoreDB(2, "_2");


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
        doRestoreDir(2, "_1");


        // ---
        // Жизнь после аварии
        doLife_AfterFail();


        // ---
        // Жизнь после аварии - идет нормально, без необходимости ремонта,
        // нужно лишь запросить пересоздание реплики
        // todo: этот номер определяет человег, взглянув на проблему в мониоринге.
        //  Хорошо бы в тестах определять номер автоматом
        long from_no = 151;
        doRequest(from_no);


        // ---
        // Финальная синхронизация
        System.out.println();
        System.out.println("Финальная синхронизация");
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Cинхронизация должна пройти нормально
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv3"));
    }


    /**
     * Проверка при сохранении базы,
     * но при полной потере репликационных каталогов
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
        doDeleteDir(2);


        // ---
        // Жизнь после аварии
        doLife_AfterFail();


        // ---
        // Жизнь после аварии - идет нормально, без необходимости ремонта,
        // нужно лишь запросить пересоздание реплики
        // todo: этот номер определяет человег, взглянув на проблему в мониоринге.
        //  Хорошо бы в тестах определять номер автоматом
        long from_no = 151;
        doRequest(from_no);


        // ---
        // Финальная синхронизация
        System.out.println();
        System.out.println("Финальная синхронизация");
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Cинхронизация должна пройти нормально
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv3"));
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
        doRestoreDB(2, "_1");

        // Восстановление строго состояния каталогов
        doRestoreDir(2, "_2");


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
        doRestoreDB(2, "_2");

        // Восстановление строго состояния каталогов
        doRestoreDir(2, "_1");

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
        doRestoreDB(2, "_1");

        // Полная потеря рабочих каталогов для ws2
        doDeleteDir(2);


        // ---
        // Жизнь после аварии
        doLife_AfterFail();

        // ---
        // Восстановление после аварии и проверка
        doRepair();
    }

    public void doRepair() throws Exception {
        doRepair(expectedNotEqual);
    }

    private void doRequest(long from_no) {
        IVariantMap args = new VariantMap();
        args.put("box", "from");
        args.put("executor", "ws");
        args.put("recreate", true);
        args.put("no", from_no);
        extWs2.repl_replica_request(args);
    }

    void doRepair(Map<String, String> expectedBeforeRepair) throws Exception {
        // Разрешим ремонт
        checkNeedRepair_doAllowRepair(db2);

        // Первая попытка ремонта
        System.out.println();
        System.out.println("Первая попытка ремонта");
        doStepRepair(db2, false);

        // Разница должна быть
        compareDb(db, db2, expectedBeforeRepair);


        // Сервер ответит на просьбы о повторной отправке
        System.out.println();
        System.out.println("Сервер ответит на просьбы о повторной отправке");
        test_srv_doReplSession();


        // Сейчас все готово для ремонта
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
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv3"));
    }


    // Создание репликации,
    // обычная работа - изменения в базах и синхронизация
    // По дороге создаем две контрольных точки
    void doSetUp_doNolmalLife_BeforeFail() throws Exception {
        if (doNolmalLifeBromBackup) {
            System.out.println("-------------");
            System.out.println("Делаем doRestoreFromNolmalLife из ранее созданной копии");
            System.out.println("-------------");
            doRestoreFromNolmalLife();
            System.out.println("-------------");
            System.out.println("doRestoreFromNolmalLife - ok");
            System.out.println("-------------");
            return;
        }

        // ---
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


        // ---
        // Очистим каталог от старых "бэкапов". По ходу теста каталог будет наполнятся промежуточными "бэкапами".
        UtFile.mkdirs(backupDirName);
        UtFile.cleanDir(backupDirName);


        // ---
        // В бэкапе будет исходное состояние
        doBackupDB(2, "");
        doBackupDir(2, "");


        // ---
        // Изменения в базах, частичная синхронизация
        doNolmalLife_Step(3);

        // Сохраним "бэкап" базы и папок для ws2
        doBackupDB(2, "_1");
        doBackupDir(2, "_1");


        // ---
        // Изменения в базах, частичная синхронизация
        doNolmalLife_Step(3);

        // Сохраним "бэкап" базы и папок для ws2
        doBackupDB(2, "_2");
        doBackupDir(2, "_2");


        // ---
        // Изменения в базах, частичная синхронизация
        doNolmalLife_Step(3);

        // ---
        // Для ускорения повторных тестов.
        // Для использования выставить doNolmalLifeBromBackup = true
        doBackupNolmalLife();
    }


    // Изменения в базах, синхронизация,
    // снова изменения в базах, "неполная" синхронизация ws2 -
    // реплики останутся только в очереди (папке) ws2 (но НЕ отправлены на сервер)
    private void doNolmalLife_Step(int stepCount) throws Exception {
        // Изменения в базах, синхронизация
        for (int i = 0; i < stepCount; i++) {
            // Станции
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();

            // Немного нагрузим 001
            test_srv_make001();

            //
            sync_http_1_2_3();
            sync_http_1_2_3();
        }

        // Изменения в базах
        for (int i = 0; i < stepCount; i++) {
            // Станции
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();

            // Немного нагрузим 001
            test_srv_make001();
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
    }


}
