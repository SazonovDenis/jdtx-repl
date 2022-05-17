package jdtx.repl.main.api


import jandcode.bgtasks.*
import jandcode.dbm.data.*
import jandcode.dbm.db.*
import jandcode.utils.*
import jandcode.utils.variant.*
import jdtx.repl.main.api.mailer.*
import jdtx.repl.main.api.manager.*
import jdtx.repl.main.api.struct.*
import jdtx.repl.main.api.util.*
import jdtx.repl.main.task.*
import jdtx.repl.main.ut.*
import org.apache.commons.io.*
import org.junit.*

import java.util.concurrent.*

// todo затестить рассылку и применение НОВЫХ конфигов при смене версии БД

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {


    String json_srv
    String json_ws

    public String cfg_json_ws
    public String cfg_json_decode
    public String cfg_json_publication_srv
    public String cfg_json_publication_ws

    public Map<String, String> equalExpected

    public Map<String, String> expectedEqual_full = [
            "USRLOG"         : "0X0",
            "PAWNCHIT"       : "000",
            "PAWNCHITSUBJECT": "000",
            "COMMENTTEXT"    : "000",
            "COMMENTTIP"     : "000",
            "LIC"            : "000",
            "LICDOCVID"      : "000",
            "LICDOCTIP"      : "000",
            "REGIONTIP"      : "000",
            "ULZ"            : "000",
            "REGION"         : "000",
    ]

    public Map<String, String> expectedEqual_noFilter = [
            "USRLOG"         : "0X0",
            "PAWNCHIT"       : "000",
            "PAWNCHITSUBJECT": "000",
            "COMMENTTEXT"    : "000",
            "COMMENTTIP"     : "000",
            "LIC"            : "000",
            "LICDOCVID"      : "000",
            "LICDOCTIP"      : "000",
            "REGIONTIP"      : "000",
            "ULZ"            : "000",
            "REGION"         : "000",
    ]

    public Map<String, String> expectedEqual_filterLic = [
            "USRLOG"         : "0X0",
            "PAWNCHIT"       : "000",
            "PAWNCHITSUBJECT": "000",
            "COMMENTTEXT"    : "0X0",
            "COMMENTTIP"     : "000",
            "LIC"            : "000",
            "LICDOCVID"      : "000",
            "LICDOCTIP"      : "000",
            "REGIONTIP"      : "000",
            "ULZ"            : "000",
            "REGION"         : "000",
    ]

    public Map<String, String> expectedNotEqual_2isEmpty = [
            "USRLOG"         : "0X0",
            "PAWNCHIT"       : "0X0",
            "PAWNCHITSUBJECT": "0X0",
            "COMMENTTEXT"    : "0X0",
            "COMMENTTIP"     : "0X0",
            "LIC"            : "0X0",
            "LICDOCVID"      : "0X0",
            "LICDOCTIP"      : "0X0",
            "REGIONTIP"      : "0X0",
            "ULZ"            : "0X0",
            "REGION"         : "0X0",
    ]

    public Map<String, String> expectedNotEqual = [
            "USRLOG"         : "XX0",
            "PAWNCHIT"       : "000",
            "PAWNCHITSUBJECT": "000",
            "COMMENTTEXT"    : "0XX",
            "COMMENTTIP"     : "000",
            "LIC"            : "XXX",
            "LICDOCVID"      : "000",
            "LICDOCTIP"      : "000",
            "REGIONTIP"      : "000",
            "ULZ"            : "?X0",
            "REGION"         : "0X0",
    ]


    public JdxReplWsSrv_Test() {
        super()

        json_srv = "test/etalon/mail_http_srv.json"
        json_ws = "test/etalon/mail_http_ws.json"

        cfg_json_ws = "test/etalon/ws.json"
        cfg_json_decode = "test/etalon/decode_strategy.json"
        cfg_json_publication_srv = "test/etalon/publication_full_152_srv.json"
        cfg_json_publication_ws = "test/etalon/publication_full_152_ws.json"

        equalExpected = expectedEqual_noFilter
    }

    /**
     * Прогон базового сценария репликации: создание репликации, полная двусторонняя репликация
     */
    @Test
    public void test_allSetUp_TestAll() throws Exception {
        // Создание репликации
        allSetUp()

        // Первичная синхронизация
        sync_http_1_2_3()
        sync_http_1_2_3()
        sync_http_1_2_3()

        // Прогон тестов
        test_AllHttp()

        //
        assertDbEquals(db, db2, expectedEqual_noFilter)
        assertDbEquals(db, db3, expectedEqual_noFilter)
        do_DumpTables(db, db2, db3, struct, struct2, struct3)
    }


    /**
     * Прогон базового сценария репликации: создание репликации, репликация с односторонним фильтром по LIC
     */
    @Test
    public void test_allSetUp_TestAll_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json"
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json"
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json"
        equalExpected = expectedEqual_filterLic;

        // Создание репликации, прогон тестов
        test_allSetUp_TestAll()
    }

    /**
     * Прогон сеанса репликациии с односторонним фильтром по LIC
     */
    @Test
    public void test_allSetUp_test_AllHttp_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json"
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json"
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json"
        equalExpected = expectedEqual_filterLic;

        // Прогон тестов
        test_AllHttp()

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3)
    }

    @Test
    public void allSetUp() throws Exception {
        disconnectAllForce()
        clearAllTestData()
        doPrepareEtalon()
        connectAll()
        reloadDbStructAll()

        //
        IVariantMap args = new VariantMap()

        // ---
        // На рабочих станциях

        // Инициализация баз и начальный конфиг рабочих станций
        args.clear()
        args.put("ws", 1)
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401")
        args.put("file", cfg_json_ws)
        extSrv.repl_create(args)

        args.clear()
        args.put("ws", 2)
        args.put("guid", "b5781df573ca6ee6.x-21ba238dfc945002")
        args.put("file", cfg_json_ws)
        extWs2.repl_create(args)

        args.clear()
        args.put("ws", 3)
        args.put("guid", "b5781df573ca6ee6.x-34f3cc20bea64503")
        args.put("file", cfg_json_ws)
        extWs3.repl_create(args)


        // ---
        // На сервере

        // Начальный конфиг сервера: напрямую задаем структуру публикаций (команда repl_set_cfg)
        args.clear()
        args.put("file", cfg_json_publication_srv)
        args.put("cfg", CfgType.PUBLICATIONS)
        extSrv.repl_set_cfg(args)

        // Добавляем рабочие станции
        args.clear()
        args.put("ws", 1)
        args.put("name", "Сервер")
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401")
        args.put("cfg_publications", cfg_json_publication_srv)
        args.put("cfg_decode", cfg_json_decode)
        extSrv.repl_add_ws(args)
        //
        args.clear()
        args.put("ws", 2)
        args.put("name", "ws 2")
        args.put("guid", "b5781df573ca6ee6.x-21ba238dfc945002")
        args.put("cfg_publications", cfg_json_publication_ws)
        args.put("cfg_decode", cfg_json_decode)
        extSrv.repl_add_ws(args)
        //
        args.clear()
        args.put("ws", 3)
        args.put("name", "ws 3")
        args.put("guid", "b5781df573ca6ee6.x-34f3cc20bea64503")
        args.put("cfg_publications", cfg_json_publication_ws)
        args.put("cfg_decode", cfg_json_decode)
        extSrv.repl_add_ws(args)
        //
        args.clear()
        args.put("ws", 4)
        args.put("name", "ws 4")
        args.put("guid", "b5781df573ca6ee6.x-444fed23da93ab04")
        args.put("cfg_publications", cfg_json_publication_ws)
        args.put("cfg_decode", cfg_json_decode)
        extSrv.repl_add_ws(args)

        //
        FileUtils.copyFile(new File("test/etalon/ws_list.json"), new File("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/b5781df573ca6ee6.x/ws_list.json"))

        // Активируем 3 рабочие станции
        args.clear()
        args.put("ws", 1)
        extSrv.repl_ws_enable(args)
        //
        args.clear()
        args.put("ws", 2)
        extSrv.repl_ws_enable(args)
        //
        args.clear()
        args.put("ws", 3)
        extSrv.repl_ws_enable(args)


        // Создаем ящики рабочих станций
        args.clear()
        args.put("create", true)
        assertEquals("Ящики не созданы", true, extSrv.repl_mail_check(args))
        //createBoxes_Local()


        // ---
        UtData.outTable(db.loadSql("select id, name, guid from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"))
    }

    /**
     * Стираем все каталоги с данными, почтой и т.п.
     */
    void clearAllTestData() {
        UtFile.cleanDir("../_test-data/_test-data_srv")
        UtFile.cleanDir("../_test-data/_test-data_ws1")
        UtFile.cleanDir("../_test-data/_test-data_ws2")
        UtFile.cleanDir("../_test-data/_test-data_ws3")
        UtFile.cleanDir("../_test-data/_test-data_ws4")
        UtFile.cleanDir("../_test-data/_test-data_ws5")
        new File("../_test-data/_test-data_srv").delete()
        new File("../_test-data/_test-data_ws1").delete()
        new File("../_test-data/_test-data_ws2").delete()
        new File("../_test-data/_test-data_ws3").delete()
        new File("../_test-data/_test-data_ws4").delete()
        new File("../_test-data/_test-data_ws5").delete()

        UtFile.cleanDir("../_test-data/csv")
        UtFile.cleanDir("../_test-data/csv1")
        UtFile.cleanDir("../_test-data/csv2")
        UtFile.cleanDir("../_test-data/csv3")
        UtFile.cleanDir("../_test-data/mail")
        UtFile.cleanDir("../_test-data/mail_local")
        new File("../_test-data/csv").delete()
        new File("../_test-data/csv1").delete()
        new File("../_test-data/csv2").delete()
        new File("../_test-data/csv3").delete()
        new File("../_test-data/mail").delete()
        new File("../_test-data/mail_local").delete()
        try {
            UtFile.cleanDir("../_test-data")
        } catch (Exception e) {
        }
        new File("d:/temp/dbm.log").delete()
        new File("d:/temp/jdtx.log").delete()
        UtFile.cleanDir("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/b5781df573ca6ee6.x")
    }


    @Test
    public void wsDbStructUpdate() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db)
        ws.init()
        JdxReplWs ws2 = new JdxReplWs(db2)
        ws2.init()
        JdxReplWs ws3 = new JdxReplWs(db3)
        ws3.init()

        //
        ws.dbStructApplyFixed(true)
        ws2.dbStructApplyFixed(true)
        ws3.dbStructApplyFixed(true)
    }

    @Test
    public void createBoxes_Http() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db)
        srv.init()
        for (Map.Entry<Long, IMailer> en : srv.mailerList.entrySet()) {
            long wsId = en.getKey()
            MailerHttp wsMailer = (MailerHttp) en.getValue()
            wsMailer.createMailBox("from")
            wsMailer.createMailBox("to")
            wsMailer.createMailBox("to001")
            System.out.println("wsId: " + wsId + ", boxes - ok")
        }
    }

    @Test
    public void createBoxes_Local() throws Exception {
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/17845f2f56f4d401/from")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/17845f2f56f4d401/to")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/21ba238dfc945002/from")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/21ba238dfc945002/to")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/34f3cc20bea64503/from")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/34f3cc20bea64503/to")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/444fed23da93ab04/from")
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/444fed23da93ab04/to")
    }

    @Test
    public void test_enable() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db)

        srv.disableWorkstation(1)
        srv.enableWorkstation(2)
        srv.disableWorkstation(3)
        srv.enableWorkstation(4)
        //
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"))
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_STATE" + "," + UtJdx.SYS_TABLE_PREFIX + "WS_STATE"))

        // Активируем рабочие станции
        srv.enableWorkstation(1)
        srv.enableWorkstation(2)
        srv.enableWorkstation(3)
        srv.enableWorkstation(4)
        //
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"))
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_STATE" + "," + UtJdx.SYS_TABLE_PREFIX + "WS_STATE"))

        //
        srv.disableWorkstation(4)
        //
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"))
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_STATE" + "," + UtJdx.SYS_TABLE_PREFIX + "WS_STATE"))
    }

    @Test
    public void test_region_http() throws Exception {
        UtTest utTest2 = new UtTest(db2)
        utTest2.make_Region_InsDel_0(struct2, 2)
        UtTest utTest3 = new UtTest(db3)
        utTest3.make_Region_InsDel_1(struct3, 3)

        //
        sync_http_1_2_3()

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3)
    }

    @Test
    public void test_AllHttp_DumpTables() throws Exception {
        test_AllHttp()
        do_DumpTables(db, db2, db3, struct, struct2, struct3)
    }

    @Test
    public void test_AllHttp() throws Exception {
        logOn()
        //
        test_ws1_makeChange_Unimportant()
        test_ws2_makeChange()
        test_ws3_makeChange()
        //
        UtTest utTest2 = new UtTest(db2)
        utTest2.make_Region_InsDel_0(struct2, 2)
        utTest2.make_Region_InsDel_1(struct2, 2)

        //
        sync_http_1_2_3()
        sync_http_1_2_3()
    }

/*
    @Test
    public void test_all_local() throws Exception {
        test_ws1_makeChange_Unimportant()
        test_ws2_makeChange()
        test_ws3_makeChange()

        //
        syncLocal()

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3)
    }
*/


    @Test
    public void sync_http_1_2_3() throws Exception {
        test_ws1_doReplSession()
        test_ws2_doReplSession()
        test_ws3_doReplSession()

        test_srv_doReplSession()

        test_ws1_doReplSession()
        test_ws2_doReplSession()
        test_ws3_doReplSession()
    }

    public void sync_http_1_2() throws Exception {
        test_ws1_doReplSession()
        test_ws2_doReplSession()

        test_srv_doReplSession()

        test_ws1_doReplSession()
        test_ws2_doReplSession()
    }

    public void sync_http_1_3() throws Exception {
        test_ws1_doReplSession()
        test_ws3_doReplSession()

        test_srv_doReplSession()

        test_ws1_doReplSession()
        test_ws3_doReplSession()
    }

    public void sync_http_1() throws Exception {
        test_ws1_doReplSession()
        test_srv_doReplSession()
        test_ws1_doReplSession()
    }

    public void sync_http_2() throws Exception {
        test_ws2_doReplSession()
        test_srv_doReplSession()
        test_ws2_doReplSession()
    }

    public void sync_http_3() throws Exception {
        test_ws3_doReplSession()
        test_srv_doReplSession()
        test_ws3_doReplSession()
    }


    @Test
    public void syncLocal() throws Exception {
/*
        test_ws1_handleSelfAudit()
        test_ws2_handleSelfAudit()
        test_ws3_handleSelfAudit()

        test_ws_sendLocal()
        test_ws_receiveLocal()

        test_sync_srv_Local()

        test_ws_sendLocal()
        test_ws_receiveLocal()

        test_ws1_handleQueIn()
        test_ws2_handleQueIn()
        test_ws3_handleQueIn()

        //
        test_DumpTables()
*/
    }

    @Test
    public void test_DumpTables_2_3_5() throws Exception {
        do_DumpTables(db2, db3, db5, struct2, struct3, struct5)
    }

    @Test
    public void test_DumpTables_1_2_3() throws Exception {
        do_DumpTables(db, db2, db3, struct, struct2, struct3)
    }

    @Test
    public void test_DumpTables_1_2_5() throws Exception {
        do_DumpTables(db, db2, db5, struct, struct2, struct5)
    }

    @Test
    public void sync_http_1_2_3_5() throws Exception {
        test_ws1_doReplSession()
        test_ws2_doReplSession()
        test_ws3_doReplSession()
        test_ws5_doReplSession()

        test_srv_doReplSession()

        test_ws1_doReplSession()
        test_ws2_doReplSession()
        test_ws3_doReplSession()
        test_ws5_doReplSession()
    }


    void assertDbNotEquals(Db db, Db db2) throws Exception {
        System.out.println("compare A: " + db.getDbSource().getDatabase())
        System.out.println("     vs B: " + db2.getDbSource().getDatabase())

        //
        System.out.println("db A")
        Map<String, Map<String, String>> dbCrcSrv = loadWsDbDataCrc(db)
        System.out.println()

        System.out.println("db B")
        Map<String, Map<String, String>> dbCrcWs2 = loadWsDbDataCrc(db2)
        System.out.println()

        //
        Map<String, Set<String>> diffCrc = new HashMap<>()
        Map<String, Set<String>> diffNewIn1 = new HashMap<>()
        Map<String, Set<String>> diffNewIn2 = new HashMap<>()

        //
        UtDbComparer.compareDbDataCrc(dbCrcSrv, dbCrcWs2, diffCrc, diffNewIn1, diffNewIn2)
        //
        assertEquals(true, diffCrc.get("USRLOG").size() == 0)
        assertEquals(true, diffNewIn1.get("USRLOG").size() != 0 || diffNewIn2.get("USRLOG").size() != 0)
        //
        assertEquals(true, diffCrc.get("PAWNCHIT").size() == 0)
        assertEquals(true, diffNewIn1.get("PAWNCHIT").size() != 0 || diffNewIn2.get("PAWNCHIT").size() != 0)
        //
        assertEquals(true, diffCrc.get("PAWNCHITSUBJECT").size() == 0)
        assertEquals(true, diffNewIn1.get("PAWNCHITSUBJECT").size() != 0 || diffNewIn2.get("PAWNCHITSUBJECT").size() != 0)
        //
        //assertEquals(true, diffCrc.get("COMMENTTEXT").size() != 0)
        assertEquals(true, diffNewIn1.get("COMMENTTEXT").size() != 0 || diffNewIn2.get("COMMENTTEXT").size() != 0)
        //
        //assertEquals(true, diffCrc.get("COMMENTTIP").size() == 0)
        //assertEquals(true, diffNewIn1.get("COMMENTTIP").size() == 0)
        //assertEquals(true, diffNewIn2.get("COMMENTTIP").size() == 0)
        //
        assertEquals(true, diffCrc.get("LIC").size() != 0 || diffNewIn1.get("LIC").size() != 0 || diffNewIn2.get("LIC").size() != 0)
        //
        assertEquals(true, diffCrc.get("LICDOCVID").size() == 0)
        assertEquals(true, diffNewIn1.get("LICDOCVID").size() != 0 || diffNewIn2.get("LICDOCVID").size() != 0)
        //
        assertEquals(true, diffCrc.get("LICDOCTIP").size() == 0)
        assertEquals(true, diffNewIn1.get("LICDOCTIP").size() != 0 || diffNewIn2.get("LICDOCTIP").size() != 0)
        //
        //assertEquals(true, diffCrc.get("ULZ").size() == 0) todo не долждно быть изменений записи, только Ins
        assertEquals(true, diffNewIn1.get("ULZ").size() != 0 || diffNewIn2.get("ULZ").size() != 0)
        //
        //assertEquals(true, diffCrc.get("REGION").size() != 0)
        assertEquals(true, diffNewIn1.get("REGION").size() != 0 || diffNewIn2.get("REGION").size() != 0)
        //
        assertEquals(true, diffCrc.get("REGIONTIP").size() == 0)
        assertEquals(true, diffNewIn1.get("REGIONTIP").size() != 0 || diffNewIn2.get("REGIONTIP").size() != 0)
        //
        System.out.println()
    }

    void do_DumpTables(Db db1, Db db2, Db db3, IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct struct3) throws Exception {
        String db1name = UtFile.removeExt(UtFile.filename(db1.getDbSource().getDatabase()))
        String db2name = UtFile.removeExt(UtFile.filename(db2.getDbSource().getDatabase()))
        String db3name = UtFile.removeExt(UtFile.filename(db3.getDbSource().getDatabase()))

        UtTest utt1 = new UtTest(db1)
        utt1.dumpTable("lic", "../_test-data/csv/ws1-lic.csv", "nameF")
        utt1.dumpTable("ulz", "../_test-data/csv/ws1-ulz.csv", "name")
        UtTest utt2 = new UtTest(db2)
        utt2.dumpTable("lic", "../_test-data/csv/ws2-lic.csv", "nameF")
        utt2.dumpTable("ulz", "../_test-data/csv/ws2-ulz.csv", "name")
        UtTest utt3 = new UtTest(db3)
        utt3.dumpTable("lic", "../_test-data/csv/ws3-lic.csv", "nameF")
        utt3.dumpTable("ulz", "../_test-data/csv/ws3-ulz.csv", "name")

        // Lic
        String sql = "select Lic.nameF, Lic.nameI, Lic.nameO, Region.name as RegionName, RegionTip.name as RegionTip, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) left join RegionTip on (Region.RegionTip = RegionTip.id) order by Lic.NameF, Lic.NameI, Lic.NameO"
        DataStore st1 = db1.loadSql(sql)
        OutTableSaver svr1 = new OutTableSaver(st1)
        //svr1.save().toFile("../_test-data/csv/ws1-all.csv")
        DataStore st2 = db2.loadSql(sql)
        OutTableSaver svr2 = new OutTableSaver(st2)
        //svr2.save().toFile("../_test-data/csv/ws2-all.csv")
        DataStore st3 = db3.loadSql(sql)
        OutTableSaver svr3 = new OutTableSaver(st3)
        //svr3.save().toFile("../_test-data/csv/ws3-all.csv")

        // CommentText
        String sql_commentText = "select " +
                "(case when CommentText.Lic=0 then 0 else 1 end) as Lic, (case when CommentText.PawnChit=0 then 0 else 1 end) PawnChit, " +
                "CommentText.CommentText, " +
                "(case when CommentText.CommentUsr=0 then null else 1 end) CommentUsr, " +
                "CommentText.CommentDt, CommentTip.Name as CommentTipName\n" +
                "from CommentText, CommentTip\n" +
                "where CommentText.id <> 0 and CommentText.CommentTip = CommentTip.id\n" +
                "order by 2, CommentText.CommentDt, CommentText.CommentText\n"  //CommentTip.Name,
        DataStore st1_ctx = db1.loadSql(sql_commentText)
        OutTableSaver svr1_ctx = new OutTableSaver(st1_ctx)
        DataStore st2_ctx = db2.loadSql(sql_commentText)
        OutTableSaver svr2_ctx = new OutTableSaver(st2_ctx)
        DataStore st3_ctx = db3.loadSql(sql_commentText)
        OutTableSaver svr3_ctx = new OutTableSaver(st3_ctx)

        // CommentText
        String sql_commentTip = "select * from CommentTip order by Name"
        DataStore st1_ctt = db1.loadSql(sql_commentTip)
        OutTableSaver svr1_ctt = new OutTableSaver(st1_ctt)
        DataStore st2_ctt = db2.loadSql(sql_commentTip)
        OutTableSaver svr2_ctt = new OutTableSaver(st2_ctt)
        DataStore st3_ctt = db3.loadSql(sql_commentTip)
        OutTableSaver svr3_ctt = new OutTableSaver(st3_ctt)

        // Region*
        String regionTestFields = ""
        for (IJdxField f : struct1.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields = regionTestFields + "Region." + f.getName() + ","
            }
        }
        String regionTestFields2 = ""
        for (IJdxField f : struct2.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields2 = regionTestFields2 + "Region." + f.getName() + ","
            }
        }
        String regionTestFields3 = ""
        for (IJdxField f : struct3.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields3 = regionTestFields3 + "Region." + f.getName() + ","
            }
        }
        String sql_ws1 = "select Region.Name as RegionName, " + regionTestFields + " RegionTip.Name as RegionTipName, RegionTip.ShortName as RegionTipShortName, regionTip.deleted as regionTipDeleted\n" +
                "from Region, RegionTip\n" +
                "where Region.id <> 0 and Region.regionTip = RegionTip.id\n" +
                "order by Region.Name, RegionTip.Name\n"
        String sql_ws2 = "select Region.Name as RegionName, " + regionTestFields2 + "  RegionTip.Name as RegionTipName, RegionTip.ShortName as RegionTipShortName, regionTip.deleted as regionTipDeleted\n" +
                "from Region, RegionTip\n" +
                "where Region.id <> 0 and Region.regionTip = RegionTip.id\n" +
                //"  and Region.id > 100000\n" + // доп условие - чтобы собственные данные ws1 не мозолили глаза
                "order by Region.Name, RegionTip.Name\n"
        String sql_ws3 = "select Region.Name as RegionName, " + regionTestFields3 + "  RegionTip.Name as RegionTipName, RegionTip.ShortName as RegionTipShortName, regionTip.deleted as regionTipDeleted\n" +
                "from Region, RegionTip\n" +
                "where Region.id <> 0 and Region.regionTip = RegionTip.id\n" +
                //"  and Region.id > 100000\n" + // доп условие - чтобы собственные данные ws1 не мозолили глаза
                "order by Region.Name, RegionTip.Name\n"
        DataStore st1_r = db1.loadSql(sql_ws1)
        OutTableSaver svr1_r = new OutTableSaver(st1_r)
        DataStore st2_r = db2.loadSql(sql_ws2)
        OutTableSaver svr2_r = new OutTableSaver(st2_r)
        DataStore st3_r = db3.loadSql(sql_ws3)
        OutTableSaver svr3_r = new OutTableSaver(st3_r)

        //
        String sql_bt = "select UsrLog.Info as UsrLogInfo\n" +
                "from UsrLog\n" +
                "where id <> 0 and Info <> ''\n" +
                "order by Info\n"
        DataStore st1_bt = db1.loadSql(sql_bt)
        OutTableSaver svr1_bt = new OutTableSaver(st1_bt)
        DataStore st2_bt = db2.loadSql(sql_bt)
        OutTableSaver svr2_bt = new OutTableSaver(st2_bt)
        DataStore st3_bt = db3.loadSql(sql_bt)
        OutTableSaver svr3_bt = new OutTableSaver(st3_bt)


        //
        String struct_t_XXX1 = dump_table_new_created(db1, struct1)
        String struct_t_XXX2 = dump_table_new_created(db2, struct2)
        String struct_t_XXX3 = dump_table_new_created(db3, struct3)

        //
        String fileName1 = "../_test-data/csv/" + db1name + "-all.csv"
        String fileName2 = "../_test-data/csv/" + db2name + "-all.csv"
        String fileName3 = "../_test-data/csv/" + db3name + "-all.csv"
        UtFile.saveString(svr1.save().toString() + "\n\n" + svr1_ctx.save().toString() + "\n\n" + svr1_ctt.save().toString() + "\n\n" + svr1_r.save().toString() + "\n\n" + struct_t_XXX1 + "\n\n" + svr1_bt.save().toString(), new File(fileName1))
        UtFile.saveString(svr2.save().toString() + "\n\n" + svr2_ctx.save().toString() + "\n\n" + svr2_ctt.save().toString() + "\n\n" + svr2_r.save().toString() + "\n\n" + struct_t_XXX2 + "\n\n" + svr2_bt.save().toString(), new File(fileName2))
        UtFile.saveString(svr3.save().toString() + "\n\n" + svr3_ctx.save().toString() + "\n\n" + svr3_ctt.save().toString() + "\n\n" + svr3_r.save().toString() + "\n\n" + struct_t_XXX3 + "\n\n" + svr3_bt.save().toString(), new File(fileName3))

        //
        startCmpDb(fileName1, fileName2, fileName3)
    }

    void startCmpDb(String fileName1, String fileName2, String fileName3) throws Exception {
        String batContent = "diff " + new File(fileName1).getAbsolutePath() + " " + new File(fileName2).getAbsolutePath() + " " + new File(fileName3).getAbsolutePath()
        File batFile = new File("temp/cmp_db.bat")
        UtFile.saveString(batContent, batFile)
        ProcessBuilder processBuilder = new ProcessBuilder("cmd", "/C", batFile.getAbsolutePath())
        processBuilder.directory(batFile.getParentFile())
        Process process = processBuilder.start()
        process.waitFor()
    }

    private String dump_table_new_created(Db db, IJdxDbStruct struct) throws Exception {
        String content = ""
        for (IJdxTable table : struct.getTables()) {
            if (table.getName().startsWith("TEST_TABLE_")) {
                String selectFields = "name"
                for (IJdxField field : table.getFields()) {
                    if (field.getName().startsWith("TEST_FIELD_")) {
                        selectFields = selectFields + "," + field.getName()
                    }
                }
                String sql = "select " + selectFields + " from " + table.getName() + " order by name"
                DataStore st = db.loadSql(sql)
                OutTableSaver svr = new OutTableSaver(st)
                content = content + table.getName() + "\n"
                content = content + svr.save().toString() + "\n\n"
            }
        }

        return content
    }

    @Test
    public void test_dump_table_testXXX() throws Exception {
        String struct_t_XXX = dump_table_new_created(db, struct)
        UtFile.saveString(struct_t_XXX, new File("../_test-data/csv/ws1-xxx.csv"))
    }

    @Test
    public void test_ws1_makeChange_Unimportant() throws Exception {
        UtTest utTest = new UtTest(db)
        utTest.makeChangeUnimportant(struct, 1)
    }

    @Test
    public void test_ws1_makeChange() throws Exception {
        UtTest utTest = new UtTest(db)
        utTest.makeChange(struct, 1)
    }

    @Test
    public void test_ws2_makeChange() throws Exception {
        UtTest utTest = new UtTest(db2)
        utTest.makeChange(struct2, 2)
    }

    @Test
    public void test_ws3_makeChange() throws Exception {
        UtTest utTest = new UtTest(db3)
        utTest.makeChange(struct3, 3)
    }

    @Test
    public void test_ws5_makeChange() throws Exception {
        UtTest utTest = new UtTest(db5)
        utTest.makeChange(struct5, 5)
    }

    @Test
    public void test_ws1_getInfoWs() throws Exception {
        JdxReplWs ws = new JdxReplWs(db)
        ws.init()

        //
        Map info = ws.getInfoWs()
        System.out.println(info)
    }

    @Test
    public void test_getInfoSrv() throws Exception {
        UtRepl urRepl = new UtRepl(db, null)
        UtData.outTable(urRepl.getInfoSrv())
    }

    @Test
    public void test_ws1_doReplSession() throws Exception {
        JdxReplWs ws = new JdxReplWs(db)
        JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws)
        replTask.doTask()
        //
        JdxTaskWsMailRequest mailTask = new JdxTaskWsMailRequest(ws)
        mailTask.doTask()
    }

    @Test
    public void test_ws2_doReplSession() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2)
        JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws)
        replTask.doTask()
        //
        JdxTaskWsMailRequest mailTask = new JdxTaskWsMailRequest(ws)
        mailTask.doTask()
    }

    @Test
    public void test_ws3_doReplSession() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3)
        JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws)
        replTask.doTask()
        //
        JdxTaskWsMailRequest mailTask = new JdxTaskWsMailRequest(ws)
        mailTask.doTask()
    }

    @Test
    public void test_ws5_doReplSession() throws Exception {
        JdxReplWs ws = new JdxReplWs(db5)
        JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws)
        replTask.doTask()
        //
        JdxTaskWsMailRequest mailTask = new JdxTaskWsMailRequest(ws)
        mailTask.doTask()
    }


/*
    @Test
    public void test_ws1_send_receive() throws Exception {
        JdxReplWs ws = new JdxReplWs(db)
        ws.init()

        //
        ws.send()
        ws.receive()
    }

    @Test
    public void test_ws2_send_receive() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2)
        ws.init()

        //
        ws.send()
        ws.receive()
    }

    @Test
    public void test_ws3_send_receive() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3)
        ws.init()

        //
        ws.send()
        ws.receive()
    }
*/


/*
    @Test
    public void test_ws1_handleQueIn() throws Exception {
        JdxReplWs ws = new JdxReplWs(db)
        ws.init()

        // Применяем входящие реплики
        ws.handleQueIn()
    }

    @Test
    public void test_ws2_handleQueIn() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2)
        ws.init()

        // Применяем входящие реплики
        ws.handleQueIn()
    }

    @Test
    public void test_ws3_handleQueIn() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3)
        ws.init()

        // Применяем входящие реплики
        ws.handleQueIn()
    }

*/

    @Test
    public void test_ws_sendLocal() throws Exception {
        // Рабочие станции
        JdxReplWs ws1 = new JdxReplWs(db)
        ws1.init()
        //
        JdxReplWs ws2 = new JdxReplWs(db2)
        ws2.init()
        //
        JdxReplWs ws3 = new JdxReplWs(db3)
        ws3.init()

        //
        //JdxStateManagerMail stateManager = new JdxStateManagerMail(db)
        //long srvSendAge = stateManager.getMailSendDone()
        //long selfQueOutAge = ws1.queOut.getMaxAge()
        ws1.sendToDir(json_ws, "../_test-data/mail_local", 0, 0, false)

        //
        //stateManager = new JdxStateManagerMail(db2)
        //srvSendAge = stateManager.getMailSendDone()
        //selfQueOutAge = ws2.queOut.getMaxAge()
        ws2.sendToDir(json_ws, "../_test-data/mail_local", 0, 0, false)

        //
        //stateManager = new JdxStateManagerMail(db3)
        //srvSendAge = stateManager.getMailSendDone()
        //selfQueOutAge = ws3.queOut.getMaxAge()
        ws3.sendToDir(json_ws, "../_test-data/mail_local/", 0, 0, false)
    }


    @Test
    public void test_ws_receiveLocal() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws1 = new JdxReplWs(db)
        ws1.init()
        //
        JdxReplWs ws2 = new JdxReplWs(db2)
        ws2.init()
        //
        JdxReplWs ws3 = new JdxReplWs(db3)
        ws3.init()

        //
        ws1.receiveFromDir(json_ws, "../_test-data/mail_local")
        ws2.receiveFromDir(json_ws, "../_test-data/mail_local")
        ws3.receiveFromDir(json_ws, "../_test-data/mail_local/")
    }

    @Test
    public void test_init_srv() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db)
        srv.init()
    }

    @Test
    public void test_init_ws2() throws Exception {
        JdxReplSrv ws2 = new JdxReplSrv(db2)
        ws2.init()
    }

    @Test
    public void test_srv_doReplSession() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db)
        //
        JdxTaskSrvMail mailTask = new JdxTaskSrvMail(srv)
        mailTask.doTask()
        //
        JdxTaskSrvRepl replTask = new JdxTaskSrvRepl(srv)
        replTask.doTask()
        //
        mailTask.doTask()
        //
        JdxTaskSrvMailRequest mailTaskRequest = new JdxTaskSrvMailRequest(srv)
        mailTaskRequest.doTask()
    }

    /* todo Разобраться с репликацией через папку - сейчас полностью сломано
    @Test
    public void test_sync_srv_Local() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db)
        srv.init()

        // Формирование общей очереди
        srv.srvHandleCommonQueFrom(json_srv, "../_test-data/mail_local")

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir(json_srv, "../_test-data/mail_local", null, 0, false)
    }

    @Test
    public void test_srvDispatchReplicasToDir_ws2() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db)
        srv.init()

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir(json_srv, "../_test-data/mail_local", null, 2, false)
    }

    @Test
    public void test_srvDispatchReplicasToDir_wsAll() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db)
        srv.init()

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir(json_srv, "../_test-data/mail_local", null, 0, false)
    }
     */

    @Test
    public void test_z() throws Exception {
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName")
        System.out.println(cfgFileName)  // todo: почему не накладывается _app.rt ?
    }


    @Test
    public void test_run_srv() throws Exception {
        while (true) {
            test_srv_doReplSession()
        }
    }

    @Test
    public void test_run_1() throws Exception {
        while (true) {
            test_ws1_makeChange_Unimportant()
            test_ws1_doReplSession()
        }
    }

    @Test
    public void test_run_2() throws Exception {
        while (true) {
            test_ws2_makeChange()
            test_ws2_doReplSession()
        }
    }

    @Test
    public void test_run_3() throws Exception {
        while (true) {
            test_ws3_makeChange()
            test_ws3_doReplSession()
        }
    }

    @Test
    public void test_loop_1_change() throws Exception {
        while (true) {
            try {
                reloadDbStructAll()
                test_ws1_makeChange_Unimportant()
                TimeUnit.SECONDS.sleep(5)
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg) || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg)
                } else {
                    throw e
                }
            }
        }
    }

    @Test
    public void loop_2_change() throws Exception {
        while (true) {
            try {
                reloadDbStructAll()
                test_ws2_makeChange()
                TimeUnit.SECONDS.sleep(5)
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg) || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg)
                } else {
                    throw e
                }
            }
        }
    }

    @Test
    public void loop_3_change() throws Exception {
        while (true) {
            try {
                reloadDbStructAll()
                test_ws3_makeChange()
                TimeUnit.SECONDS.sleep(5)
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg) || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg)
                } else {
                    throw e
                }
            }
        }
    }

    @Test
    public void loop_1_repl() throws Exception {
        while (true) {
            try {
                test_ws1_doReplSession()
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg)) {
                    System.out.println(msg)
                    e.printStackTrace()
                } else {
                    throw e
                }
            }
        }
    }

    @Test
    public void loop_2_repl() throws Exception {
        while (true) {
            try {
                test_ws2_doReplSession()
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg)) {
                    System.out.println(msg)
                    e.printStackTrace()
                } else {
                    throw e
                }
            }
        }
    }

    @Test
    public void loop_3_repl() throws Exception {
        while (true) {
            try {
                test_ws3_doReplSession()
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg)) {
                    System.out.println(msg)
                    e.printStackTrace()
                } else {
                    throw e
                }
            }
        }
    }


    @Test
    public void loop_srv() throws Exception {
        while (true) {
            try {
                test_srv_doReplSession()
                TimeUnit.SECONDS.sleep(5)
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg)) {
                    System.out.println(msg)
                    e.printStackTrace()
                } else {
                    throw e
                }
            }
        }
    }

/*   todo Разобраться с репликацией через папку - сейчас полностью сломано
    @Test
    public void loop_srv_local() throws Exception {
        while (true) {
            try {
                test_sync_srv_Local()
                TimeUnit.SECONDS.sleep(10)
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg)) {
                    System.out.println(msg)
                    e.printStackTrace()
                } else {
                    throw e
                }
            }
        }
    }


    @Test
    public void loop_ws_mail_Local() throws Exception {
        while (true) {
            try {
                test_ws_sendLocal()
                test_ws_receiveLocal()
                TimeUnit.SECONDS.sleep(20)
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e)
                if (canSkipException(msg)) {
                    System.out.println(msg)
                    e.printStackTrace()
                } else {
                    throw e
                }
            }
        }
    }
*/

    //
    boolean canSkipException(String msg) {
        return msg.contains("deadlock") ||
                msg.contains("Connection reset") ||
                msg.contains("Connection refused: connect") ||
                msg.contains("Connection timed out: connect") ||
                (msg.contains("Item info") && msg.contains("not found"))
    }


}

