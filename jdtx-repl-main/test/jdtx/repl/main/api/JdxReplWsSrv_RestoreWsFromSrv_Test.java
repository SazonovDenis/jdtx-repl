package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.rt.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

public class JdxReplWsSrv_RestoreWsFromSrv_Test extends JdxReplWsSrv_Test {

    String cfg_json_snapshot;


    public JdxReplWsSrv_RestoreWsFromSrv_Test() {
        super();
        cfg_json_snapshot = "test/etalon/publication_full_152_snapshot.json";
    }


    /**
     * Прогон сценария репликации: восстановление утраченной базы рабочей станции по данным с сервера.
     */
    @Test
    public void test_All() throws Exception {
        // ---
        // Инициализация
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Удобно различать записи
        JdxDbUtils dbu = new JdxDbUtils(db2, struct2);
        dbu.getNextGenerator("g_UsrLog", 2000);

        // --- Работаем как обычно
        test_AllHttp();

        // Проверим исходную синхронность
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv1"));


        // ---
        // Стираем текущую рабочую базу ws3 и рабочий каталог (на "рабочей станции")
        doDisconnectAllForce();
        UtFile.cleanDir("../_test-data/_test-data_ws3");
        new File("../_test-data/_test-data_ws3").delete();


        // ---
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
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        new File("../_test-data/csv").renameTo(new File("../_test-data/csv2"));

        // --- Работаем после восстановления
        test_AllHttp();
        test_AllHttp();

        // Проверим синхронность после работы
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
        cfg_json_snapshot = "test/etalon/publication_lic_152_snapshot.json";
        test_All();
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
