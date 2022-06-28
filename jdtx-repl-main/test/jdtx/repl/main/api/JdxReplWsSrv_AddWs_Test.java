package jdtx.repl.main.api;

import jandcode.utils.variant.*;
import org.junit.*;

/**
 *
 */
public class JdxReplWsSrv_AddWs_Test extends JdxReplWsSrv_Test {


    /**
     * Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время
     */
    @Test
    public void test_addWs() throws Exception {
        // Создаем репликацию (ws1, ws2, ws3)
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Идет репликация (ws1, ws2, ws3)
        test_AllHttp();
        test_AllHttp();

        //
        IVariantMap args = new VariantMap();

        // Инициализируем НОВУЮ рабочую станцию (ws5)
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

        // Синхронизация
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();

        // Идет репликация (ws1, ws2, ws3, ws5)
        System.out.println("=================================");
        test_AllHttp_5();

        //
        do_DumpTables(db, db2, db5, struct, struct2, struct5);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
        compareDb(db, db5, equalExpected);
    }

    /**
     * Прогон сценария репликации: добавление рабочей станции после того, как остальные уже поработали некоторое время,
     * с односторонним фильтром по LIC
     */
    @Test
    public void test_addWs_filter() throws Exception {
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        equalExpected = expectedEqual_filterLic;
        test_addWs();
    }

    @Test
    public void test_AllHttp_5_DumpTables() throws Exception {
        test_AllHttp_5();
        test_DumpTables_1_2_5();
    }

    @Test
    public void tableSnapshotRequest() throws Exception {
        IVariantMap args = new VariantMap();
        args.put("ws", 3);
        args.put("tables", "Lic,LicDocTip");
        extSrv.repl_snapshot_request(args);
    }

    @Test
    public void test_AllHttp_5() throws Exception {
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        test_ws5_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);

        //
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
    }

    @Test
    public void test_all_CommentTip() throws Exception {
        UtTest utTest1 = new UtTest(db);
        utTest1.makeChange_CommentTip(struct, 1);
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.makeChange_CommentTip(struct2, 2);
        //
        UtTest utTest3 = new UtTest(db3);
        utTest3.makeChange_CommentTip(struct3, 3);
        //
        UtTest utTest5 = new UtTest(db5);
        utTest5.makeChange_CommentTip(struct5, 5);

        //
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();

        //
        test_DumpTables_1_2_5();
    }

    @Test
    public void test_ws2_makeChange_CommentTip() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.makeChange_CommentTip(struct2, 2);
    }


}
