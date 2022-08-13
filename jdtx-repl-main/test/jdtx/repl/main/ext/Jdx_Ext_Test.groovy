package jdtx.repl.main.ext

import jandcode.jc.*
import jandcode.jc.test.*
import jandcode.utils.variant.*
import jdtx.repl.main.api.mailer.*
import org.joda.time.*
import org.json.simple.*
import org.junit.*

class Jdx_Ext_Test extends JcTestCase {

    Jdx_Ext extSrv
    Jdx_Ext extWs2
    Jdx_Ext extWs3

    String cfg_json_ws = "test/etalon/ws.json"
    String mailUrl = "http://localhost/lombard.systems/repl";
    String mailGuid = "b5781df573ca6ee6.x";
    String mailPass = "111";


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
        IVariantMap args = new VariantMap()
        args.put("publications", true)
        args.put("file", "test/etalon/alg/pub.json")
        extSrv.repl_check(args)
    }

    @Test
    void repl_mail_check() {
        IVariantMap args = new VariantMap()
        args.put("mail", mailUrl)
        args.put("guid", mailGuid)
        extSrv.repl_mail_check(args)
    }

    @Test
    void repl_mail_create() {
        IVariantMap args = new VariantMap()
        args.put("mail", mailUrl)
        args.put("guid", mailGuid)
        args.put("pass", mailPass)
        extSrv.repl_mail_create(args)
    }

    @Test
    void repl_mail_check_short() {
        IVariantMap args = new VariantMap()

        args.put("mail", mailUrl)
        extSrv.repl_mail_check(args)

        args.put("mail", mailUrl + "-no-url-404")
        extSrv.repl_mail_check(args)

        args.put("mail", "http://jadatex.ru/repl")
        extSrv.repl_mail_check(args)

        args.put("mail", "http://jadatex.ru/no-url-404")
        extSrv.repl_mail_check(args)

        args.put("mail", "http://no-server-123456.com")
        extSrv.repl_mail_check(args)
    }

    @Test
    void repl_mail_check_create_guid() {
        //String mailUrl = "http://localhost/lombard.systems/repl1";

        Random rnd = new Random();
        rnd.setSeed(new DateTime().getMillis());
        long wsIdRandom = 100 + rnd.nextInt(1000);
        long guidRandom = 100 + rnd.nextInt(1000);

        // Конфиг для мейлера
        JSONObject cfgMailer = new JSONObject()
        String guid = "test_guid_" + guidRandom
        cfgMailer.put("guid", guid);
        cfgMailer.put("url", mailUrl);
        cfgMailer.put("localDirTmp", "temp/mailer");

        // Мейлер
        MailerHttp mailer = new MailerHttp();
        mailer.init(cfgMailer);

        //
        String pass = null
        mailer.createGuid(guid, pass)

        //
        String guidWs = guid + "/test_ws_" + wsIdRandom;
        cfgMailer.put("guid", guidWs);
        mailer.init(cfgMailer);

        //
        String box = "test_box";
        mailer.createMailBox(box);
    }

    @Test
    void repl_mail_login() {
        String mailUrl = "http://localhost/lombard.systems/repl";

        // Конфиг для мейлера
        JSONObject cfgMailer = new JSONObject()
        cfgMailer.put("guid", "-");
        cfgMailer.put("url", mailUrl);
        cfgMailer.put("localDirTmp", "temp/mailer");

        // Мейлер
        MailerHttp mailer = new MailerHttp();
        mailer.init(cfgMailer);

        //
        String token = mailer.login("111")

        //
        println("token: " + token)
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
