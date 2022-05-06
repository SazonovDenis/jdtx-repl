package jdtx.repl.main.api;

import org.junit.*;

import java.io.*;

/**
 * Проверка восстановления рабочей станции
 * при восстановлении базы/папок из бэкапа
 */
public class JdxReplWsSrv_RestoreWs_DbRestore_test extends JdxReplWsSrv_RestoreWs_Test {


    /**
     * Проверка восстановления репликации рабочей станции
     * при восстановлении базы из бэкапа, но при сохранении репликационных каталогов
     * test_DatabaseRestore_stepRuinDB - провоцирует ошибку,
     * test_DatabaseRestore_stepRepair - исправляет ее
     */
    @Test
    public void test_DB() throws Exception {
        test_DatabaseRestore_stepRuinDB();
        test_DatabaseRestore_stepRepair();
    }

    /**
     * Проверка восстановления репликации рабочей станции
     * при восстановлении базы из бэкапа и потере репликационных каталогов
     * test_DatabaseRestore_stepRuinDirDB - провоцирует ошибку,
     * test_DatabaseRestore_stepRepair - исправляет ее
     */
    @Test
    public void test_DirDB() throws Exception {
        test_DatabaseRestore_stepRuinDirDB();
        test_DatabaseRestore_stepRepair();
    }

    @Test
    public void test_DatabaseRestore_stepRuinDB() throws Exception {
        // ---
        // Создание репликации
        // Работаем как обычно
        // Изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийное событие

        // Восстановление из "бэкапа" базы для ws2
        doRestore(extWs2);


        // ---
        // Жизнь после аварии

        // Попытка синхронизации (неудачная для ws2)
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        System.out.println("Попытка синхронизации была неудачная");
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);
        assertDbNotEquals_1_2_3();

        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Попытка синхронизации (неудачная для ws2)
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        System.out.println("Попытка синхронизации была неудачная");
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);
        assertDbNotEquals_1_2_3();
    }

    @Test
    public void test_DatabaseRestore_stepRuinDirDB() throws Exception {
        // ---
        // Создание репликации
        // Обычная работа: изменения в базах и синхронизация -
        // получаем синхронные базы
        doSetUp_doNolmalLife_BeforeFail();


        // ---
        // Аварийные события:

        // Восстановление из "бэкапа" базы для ws2
        doRestore(extWs2);

        // Удаление рабочих каталогов для ws2
        doDelete_Dir(db2);


        // ---
        // Жизнь после аварии

        // Попытка синхронизации (неудачная для ws2)
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        System.out.println("Попытка синхронизации была неудачная");
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);
        assertDbNotEquals_1_2_3();

        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Попытка синхронизации (неудачная для ws2)
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        System.out.println("Попытка синхронизации была неудачная");
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);
        assertDbNotEquals_1_2_3();
    }

    @Test
    public void test_DatabaseRestore_stepRepair() throws Exception {
        // Ремонт
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        // Первая попытка ремонта
        System.out.println();
        System.out.println("Первая попытка ремонта");
        try {
            ws.repairAfterBackupRestore(true, false);
        } catch (Exception e) {
            System.out.println("Первая попытка ремонта: " + e.getMessage());
        }


        // Сервер ответит на просьбы о повторной отправке
        System.out.println();
        System.out.println("Сервер ответит на просьбы о повторной отправке");
        test_srv_doReplSession();


        // Последняя попытка ремонта
        System.out.println();
        System.out.println("Последняя попытка ремонта");
        ws.repairAfterBackupRestore(true, false);


        // Финальная синхронизация
        System.out.println();
        System.out.println("Финальная синхронизация");
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Cинхронизация должна пройти нормально
        assertDbEquals_1_2_3();
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv3"));
    }

    private void doSetUp_doNolmalLife_BeforeFail() throws Exception {
        // Создание репликации
        allSetUp();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();


        // Проверим исходную синхронность после инициализации
        System.out.println("Базы должны быть в синхронном состоянии");
        assertDbEquals_1_2_3();


        // ---
        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // Синхронизация
        sync_http_1_2_3();


        // Проверим исходную синхронность после изменений
        System.out.println("Базы должны быть в синхронном состоянии");
        assertDbEquals_1_2_3();


        // ---
        // Изменения в базах и сохранение "бэкапа" базы ws2 -
        // в бэкапе будет необработанный аудит

        // Изменения в базах
        for (int i = 0; i <= 3; i++) {
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();
        }

        // Сохраним "бэкап" базы для ws2
        doBackup(extWs2);


        // ---
        // Изменения в базах и синхронизация ws2 -
        // на сервер попадут измения старше "бэкапа"
        for (int i = 0; i <= 3; i++) {
            // Изменения в базах
            test_ws1_makeChange_Unimportant();
            test_ws2_makeChange();
            test_ws3_makeChange();

            // Синхронизация
            sync_http_1_2_3();
        }


        // ---
        // Изменения в базах и "неполная" синхронизация ws2 -
        // в папках останутся измения старше "бэкапа", и старше, чем отправлено на сервер

        // Изменения в базах
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        // "Неполная" синхронизация ws2
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        ws2.handleSelfAudit();
    }

}
