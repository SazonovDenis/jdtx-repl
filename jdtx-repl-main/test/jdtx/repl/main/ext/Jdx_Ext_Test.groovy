package jdtx.repl.main.ext

import jandcode.dbm.*
import jandcode.dbm.data.*
import jandcode.dbm.db.*
import jandcode.jc.*
import jandcode.jc.test.*
import jandcode.utils.variant.*
import org.junit.*

class Jdx_Ext_Test extends JcTestCase {

    Jdx_Ext extSrv
    Jdx_Ext extWs2
    Jdx_Ext extWs3

    String cfg_json_ws = "test/etalon/ws.json"


    @Override
    void setUp() throws Exception {
        super.setUp()
        ProjectScript p1 = jc.loadProject("srv/project.jc")
        extSrv = p1.createExt("jdtx.repl.main.ext.Jdx_Ext")
        ProjectScript p2 = jc.loadProject("ws2/project.jc")
        extWs2 = p2.createExt("jdtx.repl.main.ext.Jdx_Ext")
        ProjectScript p3 = jc.loadProject("ws3/project.jc")
        extWs3 = p3.createExt("jdtx.repl.main.ext.Jdx_Ext")
    }


    @Test
    void test_repl_info() {
        IVariantMap args = new VariantMap()

        extSrv.repl_info(args)
        System.out.println("=========================")
        extWs2.repl_info(args)
        System.out.println("=========================")
        extWs3.repl_info(args)
    }


    @Test
    void xxx() {
        IVariantMap args = new VariantMap()
        args.put("ws", 1)
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401")
        args.put("file", cfg_json_ws)

        extSrv.repl_create(args)
    }

    @Test
    void repl_find_record() {
        IVariantMap args = new VariantMap()
        args.put("id", "lic:11:1418")
        args.put("dir", "d:/t/Anet/temp")
        args.put("out", "../_test-data/lic_11_1418.zip")
        extWs2.repl_find_record(args)
    }

    @Test
    void repl_find_record_out() {
        IVariantMap args = new VariantMap()
        args.put("id", "region:2:1001")
        args.put("dir", "../_test-data/_test-data_ws2/ws_002/queOut")
        args.put("out", "../_test-data/region_2_1001.zip")
        extWs2.repl_find_record(args)
    }

    @Test
    void repl_find_record_out1() {
        IVariantMap args = new VariantMap()
        args.put("id", "regiontip:2:1001")
        args.put("dir", "D:/t/esilMK/003/data/ws_003/queIn")
        extWs2.repl_find_record(args)
    }

    @Test
    void repl_replica_use() {
        IVariantMap args = new VariantMap()
        args.put("file", "../_test-data/_test-data_ws2/ws_002/queIn/000000086.zip")
        extWs2.repl_replica_use(args)
    }

    @Test
    void repl_service_list() {
        IVariantMap args = new VariantMap()
        extSrv.repl_service_list(args)
    }

    @Test
    void repl_service_install() {
        IVariantMap args = new VariantMap()
        extSrv.repl_service_install(args)
    }

    @Test
    void repl_service_remove() {
        IVariantMap args = new VariantMap()
        extSrv.repl_service_remove(args)
    }

    @Test
    void repl_service_start() {
        IVariantMap args = new VariantMap()
        extSrv.repl_service_start(args)
    }

    @Test
    void repl_service_stop() {
        IVariantMap args = new VariantMap()
        extSrv.repl_service_stop(args)
    }


    @Test
    void repl_record_merge_file() {
        //TestExtJc jc = createExt(TestExtJc.class);
        ProjectScript project = jc.loadProject("one/project.jc");
        Merge_Ext ext = (Merge_Ext) project.createExt("jdtx.repl.main.ext.Merge_Ext");

        //
        IVariantMap args = new VariantMap();
        //args.put("table", "LicDocTip");
        //args.put("file", "temp/_LicDocTip.task");
        //args.put("fields", "Name,ShortName");
        args.put("table", "LicDocVid");
        args.put("file", "temp/_LicDocVid.task");
        args.put("fields", "Name");

        //
        ext.rec_merge_find(args);

        //
        ext.rec_merge_exec(args);
    }


    @Test
    void relocate() {
        ProjectScript p1 = jc.loadProject("ws2/project.jc")
        Merge_Ext ext = (Merge_Ext) p1.createExt("jdtx.repl.main.ext.Merge_Ext")

        //
        Db db = ext.getApp().service(ModelService.class).model.getDb()
        db.connect()

        //
        //UtData.outTable(db.loadSql("select * from Lic order by Id"));

        //
        IVariantMap args = new VariantMap();
        args.put("table", "Lic");
        args.put("sour", 1357)
        args.put("dest", 7777)

        //
        System.out.println("==================")
        System.out.println("rec_relocate_check")
        ext.rec_relocate_check(args);

        //
        System.out.println("==================")
        System.out.println("rec_relocate")
        ext.rec_relocate(args);
    }


}
