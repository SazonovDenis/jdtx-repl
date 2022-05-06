package jdtx.repl.main.api;

import jandcode.utils.rt.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Проверка восстановления рабочей станции
 * по данным с сервера.
 */
public class JdxReplWsSrv_RestoreWs_FromSrv_Test extends JdxReplWsSrv_RestoreWs_Test {

    String cfg_json_snapshot;


    public JdxReplWsSrv_RestoreWs_FromSrv_Test() {
        super();
        cfg_json_snapshot = "test/etalon/publication_full_152_snapshot.json";
    }


    /**
     * Проверка восстановления репликации рабочей станции
     * при полной потере базы и репликационных каталогов, по данным с сервера.
     * test_DatabaseRestore_stepRuin - провоцирует ошибку,
     * test_DatabaseRestore_stepRepair - исправляет ее
     */
    @Test
    public void test_DirDB_srv() throws Exception {
        test_DatabaseRestore_stepRuin();
        test_DatabaseRestore_stepRepair();
    }

    @Test
    public void test_DatabaseRestore_stepRuin() throws Exception {
        // Создание репликации
        allSetUp();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Удобно различать записи
        JdxDbUtils dbu = new JdxDbUtils(db2, struct2);
        dbu.getNextGenerator("g_UsrLog", 2000);


        // ---
        // Обычная работа: изменения в базах и синхронизация -
        // получаем синхронные базы
        test_AllHttp();


        // ---
        // Проверим исходную синхронность
        System.out.println("Базы в синхронном состоянии");
        assertDbEquals_1_2_3();
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);
        //new File("../_test-data/csv").renameTo(new File("../_test-data/csv1"));


        // ---
        // Аварийное событие:
        // стираем текущую рабочую базу ws3 и ее рабочий каталог
        doDelete_DirDb(db3);


        // ---
        // Жизнь после аварии

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
        // Берем заготовку базы данных (на "рабочей станции")
        Rt rt = extWs3.getApp().getRt().getChild("db/default");
        String dbNameDest = rt.getValue("database").toString();
        String dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println("Эталонная база скопирована [" + dbNameDest + "]");
        //
        doConnectAll();


        // ---
        // Подаем команду для подготовки базы рабочей станции
        IVariantMap args = new VariantMap();
        args.clear();
        args.put("ws", 3);
        args.put("guid", "b5781df573ca6ee6.x-34f3cc20bea64503");
        args.put("file", cfg_json_ws);
        extWs3.repl_create(args);

        // Подаем команду "repair" для рабочей станции
        test_repairAfterBackupRestore_ws3();

        // ---
        // Сладко ли работается воскрешенной рабочей станции?
        sync_http_1_2_3();


        // ---
        // Подаем команду для подготовки snaphot (на сервере)
        args.clear();
        args.put("ws", 3);
        args.put("cfg_snapshot", cfg_json_snapshot);
        extSrv.repl_restore_ws(args);


        // ---
        // Сладко ли работается воскрешенной и РЕАНИМИРОВАННОЙ рабочей станции?
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверим синхронность после восстановления
        System.out.println("Cинхронизация прошла нормально");
        assertDbEquals_1_2_3();
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);
        //new File("../_test-data/csv").renameTo(new File("../_test-data/csv2"));

        // --- Работаем после восстановления
        test_AllHttp();
        test_AllHttp();

        // Проверим синхронность после работы
        System.out.println("Cинхронизация прошла нормально");
        assertDbEquals_1_2_3();
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv3"));
    }

    /**
     * Прогон сценария репликации: восстановление утраченной базы рабочей станции по данным с сервера,
     * с односторонним фильтром по LIC.
     */
    @Test
    public void test_All_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json";
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        cfg_json_snapshot = "test/etalon/publication_lic_152_ws.json"; // <-- cfg_json_snapshot эта натройка почему то все ломае
        //cfg_json_snapshot = "test/etalon/publication_lic_152_snapshot.json";
        test_DirDB_srv();
    }

    @Test
    public void test_repairAfterBackupRestore_ws3() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();
        ws.repairAfterBackupRestore(true, false);
    }

    @Test
    public void test_restoreWorkstation_ws3() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        srv.restoreWorkstation(3, cfg_json_snapshot);
    }


}
