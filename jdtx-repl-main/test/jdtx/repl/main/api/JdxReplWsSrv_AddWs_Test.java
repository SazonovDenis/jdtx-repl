package jdtx.repl.main.api;

import jandcode.utils.variant.*;
import org.junit.*;

/**
 *
 */
public class JdxReplWsSrv_AddWs_Test extends JdxReplWsSrv_Test {


    /**
     *
     */
    @Test
    public void test_all() throws Exception {
        // Создаем репликацию (ws1, ws2, ws3)
        allSetUp();
        sync_http();
        sync_http();

        // Идет репликация (ws1, ws2, ws3)
        test_AllHttp();
        test_AllHttp();

        //
        IVariantMap args = new VariantMap();

        // Инициализируем рабочую станцию (ws5)
        args.clear();
        args.put("ws", 5);
        args.put("guid", "b5781df573ca6ee6.x-50d3d7a3c104ae05");
        args.put("file", cfg_json_ws);
        extWs5.repl_create(args);

        System.out.println("---------------------------------");

        // Добавляем рабочую станцию в систему
        args.clear();
        args.put("ws", 5);
        args.put("name", "ws 5");
        args.put("guid", "b5781df573ca6ee6.x-50d3d7a3c104ae05");
        args.put("cfg_publications", cfg_json_publication_ws);
        args.put("cfg_decode", cfg_json_decode);
        extSrv.repl_add_ws(args);

        // Активируем рабочую станцию
        args.clear();
        args.put("ws", 5);
        extSrv.repl_ws_enable(args);

        // Создаем ящик рабочей станции
        args.clear();
        args.put("create", true);
        assertEquals("Ящики не созданы", true, extSrv.repl_mail_check(args));

        // Идет репликация (ws1, ws2, ws3, ws5)
        System.out.println("=================================");
        test_AllHttp_5();

        //
        test_DumpTables_1_2_5();
    }

    @Test
    public void test_all_filter() throws Exception {
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        test_all();
    }

    @Test
    public void test_DumpTables_1_2_5() throws Exception {
        do_DumpTables(db, db2, db5, struct, struct2, struct5);
    }

    @Test
    public void test_DumpTables_1_2_3() throws Exception {
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_AllHttp_5() throws Exception {
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        test_ws5_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_InsDel(struct2, 2);
        utTest2.make_InsDel_1(struct2, 2);

        //
        sync_http_5();
        sync_http_5();
        sync_http_5();
        sync_http_5();
    }

    @Test
    public void sync_http_5() throws Exception {
        test_ws1_doReplSession();
        test_ws2_doReplSession();
        test_ws3_doReplSession();
        test_ws5_doReplSession();

        test_srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
        test_ws3_doReplSession();
        test_ws5_doReplSession();
    }

    @Test
    public void test_ws5_doReplSession() throws Exception {
        JdxReplWs ws = new JdxReplWs(db5);
        JdxReplTaskWs replTask = new JdxReplTaskWs(ws);
        //
        replTask.doReplSession();
    }


}
