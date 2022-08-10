package jdtx.repl.main.ext


import jandcode.jc.*
import jandcode.jc.test.*
import jandcode.utils.variant.*
import org.junit.*

class Jdx_Ext_Test extends JcTestCase {

    Jdx_Ext extSrv
    Jdx_Ext extWs2
    Jdx_Ext extWs3

    String cfg_json_ws = "test/etalon/ws.json"
    String mailUrl = "http://localhost/lombard.systems/repl";


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
    void repl_check() {
        // ov repl-check -tables -fields -publications -file:Z:\jdtx-repl\jdtx-repl-main\test\etalon\alg\pub.json > 1.txt
        IVariantMap args = new VariantMap()
        //args.put("tables", true)
        //args.put("fields", true)
        args.put("publications", true)
        args.put("file", "test/etalon/alg/pub.json")
        extSrv.repl_check(args)
    }

    @Test
    void repl_create() {
        IVariantMap args = new VariantMap()
        args.put("ws", 1)
        args.put("guid", "b5781df573ca6ee6.x")
        args.put("mail", mailUrl);
        extSrv.repl_create(args)
    }

    @Test
    void repl_mail_check() {
        IVariantMap args = new VariantMap()
        args.put("mail", mailUrl)
        args.put("guid", "b5781df573ca6ee6.x")
        extSrv.repl_mail_check(args)
    }

    @Test
    void repl_mail_check_short() {
        IVariantMap args = new VariantMap()

        args.put("mail", mailUrl)
        extSrv.repl_mail_check(args)

        args.put("mail", mailUrl+"-no-url-404")
        extSrv.repl_mail_check(args)

        args.put("mail", "http://jadatex.ru/repl")
        extSrv.repl_mail_check(args)

        args.put("mail", "http://jadatex.ru/no-url-404")
        extSrv.repl_mail_check(args)

        args.put("mail", "http://no-server-123456.com")
        extSrv.repl_mail_check(args)
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
    void repl_replica_request() {
        IVariantMap args = new VariantMap()
        //args.put("ws", 2)
        args.put("box", "from")
        args.put("executor", "srv")
        args.put("from", 777)
        extWs2.repl_replica_request(args)
    }

    @Test
    void repl_allow_repair() {
        IVariantMap args = new VariantMap()
        args.put("ws", 2)
        extSrv.repl_allow_repair(args)
    }

    @Test
    void repl_service_state() {
        IVariantMap args = new VariantMap()
        extSrv.repl_service_state(args)
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
    void repl_snapshot_send_srv() {
        IVariantMap args = new VariantMap()
        args.put("ws", 2)
        args.put("tables", "CommentText,CommentTip,Usr,UsrGrp,UsrOtdel")
        extSrv.repl_snapshot_send(args)
    }

    @Test
    void repl_snapshot_send_ws() {
        IVariantMap args = new VariantMap()
        args.put("tables", "CommentText,CommentTip,Usr,UsrGrp,UsrOtdel")
        extWs2.repl_snapshot_send(args)
    }

    @Test
    void repl_snapshot_create_ws() {
        IVariantMap args = new VariantMap()

        args.put("file", "temp/t1")
        args.put("tables", "CommentText,CommentTip,Usr,UsrGrp,UsrOtdel")
        extWs2.repl_snapshot_create(args)

        args.put("file", "temp/t2")
        args.put("tables", "CommentTip,CommentText,UsrGrp,Usr,UsrOtdel")
        extWs2.repl_snapshot_create(args)

        args.put("file", "temp/PawnChitDat.zip")
        args.put("tables", "PawnChitDat")
        extWs2.repl_snapshot_create(args)
    }


}
