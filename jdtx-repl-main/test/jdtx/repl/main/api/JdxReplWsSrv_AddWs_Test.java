package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.jc.*;
import jandcode.jc.test.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.ext.*;
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
        test_DumpTables_1_2_5();
    }

    @Test
    public void test_AllHttp_5_DumpTables() throws Exception {
        test_AllHttp();
        test_DumpTables_1_2_5();
    }

    @Test
    public void test_allSetUp_TestAll_filter() throws Exception {
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        test_all();
    }

    @Test
    public void requestTableSnapshot() throws Exception {
        IVariantMap args = new VariantMap();
        args.put("ws", 3);
        args.put("tables", "Lic,LicDocTip");
        extSrv.repl_request_snapshot(args);
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
    public void test_all_CommentTip_Merge() throws Exception {
        // Готовим почву для сияния
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));
        db.execSql("update CommentTip set Name = 'Tip-ins-all' where Name like 'Tip-ins-ws%'");
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));


        // Сливаем записи
        TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript project = jc.loadProject("../ext/srv/project.jc");
        Merge_Ext ext = (Merge_Ext) project.createExt("jdtx.repl.main.ext.Merge_Ext");

        //
        IVariantMap args = new VariantMap();
        args.put("table", "CommentTip");
        args.put("file", "temp/_CommentTip.task");
        args.put("fields", "Name");
        args.put("cfg_group", "test/etalon/field_groups.json");

        //
        ext.rec_merge_find(args);

        //
        args.put("delete", true);
        //args.put("delete", false);
        ext.rec_merge_exec(args);

        //
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));


        // Синхронизируем
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();


/*
        // Удаляем лишние
        args.put("delete", true);
        ext.rec_merge_exec(args);

        //
        UtData.outTable(db.loadSql("select id, Name from CommentTip order by id"));


        // Синхронизируем
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
        sync_http_1_2_3_5();
*/


        //
        test_DumpTables_1_2_5();
    }

    @Test
    public void test_DumpTables_1_2_3() throws Exception {
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_DumpTables_1_2_5() throws Exception {
        do_DumpTables(db, db2, db5, struct, struct2, struct5);
    }

    @Test
    public void test_ws2_makeChange_CommentTip() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.makeChange_CommentTip(struct2, 2);
    }

    @Test
    public void sync_http_1_2_3_5() throws Exception {
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
