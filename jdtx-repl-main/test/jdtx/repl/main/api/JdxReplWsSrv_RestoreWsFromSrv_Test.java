package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.rt.*;
import jandcode.utils.variant.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Проверка восстановления утраченной базы рабочей станции по данным с сервера.
 */
public class JdxReplWsSrv_RestoreWsFromSrv_Test extends JdxReplWsSrv_Test {

    /**
     *
     */
    @Test
    public void test_All() throws Exception {
        // ---
        // Инициализация
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // --- Работаем как обычно
        test_AllHttp();
        test_AllHttp();

        // Проверим синхронность после изменений
        //do_DumpTables(db, db2, db3, struct, struct2, struct3);


        // ---
        // Стираем текущую рабочую базу ws3 и рабочий каталог (на "рабочей станции")

        doDisconnectAllForce();

        //
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
        extSrv.repl_restore_ws(args);


        // ---
        // Сладко ли работается воскрешенной и РЕАНИМИРОВАННОЙ рабочей станции?
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверим синхронность после восстановления
        do_DumpTables(db, db2, db3, struct, struct2, struct3);


        // --- Работаем после восстановления
        test_AllHttp();
        test_AllHttp();
        test_AllHttp();

        // Проверим синхронность после работы
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_repairAfterBackupRestore_ws3() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();
        ws.repairAfterBackupRestore(true);
    }


}
