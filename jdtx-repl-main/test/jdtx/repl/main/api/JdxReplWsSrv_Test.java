package jdtx.repl.main.api;

import jandcode.dbm.data.DataStore;
import jandcode.dbm.data.OutTableSaver;
import jandcode.dbm.data.UtData;
import jandcode.dbm.db.Db;
import jandcode.utils.UtFile;
import jandcode.utils.variant.IVariantMap;
import jandcode.utils.variant.VariantMap;
import jdtx.repl.main.api.mailer.IMailer;
import jdtx.repl.main.api.mailer.MailerHttp;
import jdtx.repl.main.api.manager.CfgType;
import jdtx.repl.main.api.struct.IJdxDbStruct;
import jdtx.repl.main.api.struct.IJdxField;
import jdtx.repl.main.api.struct.IJdxTable;
import jdtx.repl.main.api.util.UtJdx;
import jdtx.repl.main.log.JdtxStateContainer;
import jdtx.repl.main.task.*;
import jdtx.repl.main.ut.Ut;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

// todo затестить рассылку и применение НОВЫХ конфигов при смене версии БД

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {

    public String mailUrl;
    public String mailGuid;
    public String mailPass;

    public String cfg_json_ws;
    public String cfg_json_decode;
    public String cfg_json_publication_srv;
    public String cfg_json_publication_ws;

    public Map<String, String> equalExpected = JdxReplWsSrv_Expected.equalExpected;
    public Map<String, String> expectedEqual_full = JdxReplWsSrv_Expected.expectedEqual_full;
    public Map<String, String> expectedEqual_noFilter = JdxReplWsSrv_Expected.expectedEqual_noFilter;
    public Map<String, String> expectedEqual_filterLic = JdxReplWsSrv_Expected.expectedEqual_filterLic;
    public Map<String, String> expectedNotEqual_2isEmpty = JdxReplWsSrv_Expected.expectedNotEqual_2isEmpty;
    public Map<String, String> expectedNotEqual = JdxReplWsSrv_Expected.expectedNotEqual;

    String json_srv;
    String json_ws;

    Thread stateOuterFile = null;
    Thread stateOuterThread = null;

    public JdxReplWsSrv_Test() {
        super();

        json_srv = "test/etalon/mail_http_srv.json";
        json_ws = "test/etalon/mail_http_ws.json";

        mailGuid = "b5781df573ca6ee6.x";
        mailUrl = "http://localhost/lombard.systems/repl";
        //mailUrl = "http://jadatex.ru/repl";
        mailPass = "111";
        //mailPass = null;

        cfg_json_ws = "test/etalon/ws.json";
        cfg_json_decode = "test/etalon/decode_strategy.json";
        cfg_json_publication_srv = "test/etalon/publication_full_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_full_152_ws.json";

        equalExpected = expectedEqual_noFilter;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (stateOuterFile != null) {
            stateOuterFile.stop();
        }
        //
        if (stateOuterThread != null) {
            stateOuterThread.stop();
        }

        //
        super.tearDown();
    }

    /**
     * Прогон базового сценария репликации: создание репликации, полная двусторонняя репликация
     */
    @Test
    public void test_baseReplication() throws Exception {
        // Создание репликации
        allSetUp();

        //
        //stateOuterThread = createStateOuterMailer(db);
        //stateOuterThread.start();

        //
        //stateOuterFile = new Thread(new JdtxStateOuterFile(JdtxStateContainer.state, "temp/state.json"));
        //stateOuterFile.start();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Прогон тестов
        test_AllHttp();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
    }

    private Thread createStateOuterMailer(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        JdtxStateContainer.state.get().getValues().clear();
        Thread stateOuterThread = new Thread(new JdtxStateOuterMailer(JdtxStateContainer.state, ws.getMailer()));
        return stateOuterThread;
    }


    /**
     * Прогон базового сценария репликации: создание репликации, репликация с односторонним фильтром по LIC
     */
    @Test
    public void test_baseReplication_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json";
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        equalExpected = expectedEqual_filterLic;

        // Создание репликации, прогон тестов
        test_baseReplication();
    }

    /**
     * Прогон сеанса репликациии с односторонним фильтром по LIC
     */
    @Test
    public void test_allSetUp_test_AllHttp_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json";
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        equalExpected = expectedEqual_filterLic;

        // Прогон тестов
        test_AllHttp();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void allSetUp() throws Exception {
        disconnectAllForce();
        clearAllTestData();
        doPrepareEtalon();
        connectAll();
        reloadDbStructAll();

        //
        IVariantMap args = new VariantMap();

        // ---
        // На сервере

        // Инициализация баз и начальный конфиг рабочих станций
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 1);
        args.put("name", "Сервер");
        args.put("mail", mailUrl);
        args.put("guid", mailGuid);
        extSrv.repl_create(args);

        // Начальный конфиг сервера: напрямую задаем структуру публикаций (команда repl_set_cfg);
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("cfg", CfgType.DECODE);
        args.put("file", cfg_json_decode);
        extSrv.repl_set_cfg(args);
        args.put("file", cfg_json_publication_srv);
        extSrv.repl_set_struct(args);


        // Добавляем рабочие станции
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 2);
        args.put("name", "ws 2");
        extSrv.repl_add_ws(args);
        //
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 3);
        args.put("name", "ws 3");
        extSrv.repl_add_ws(args);
        //
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 4);
        args.put("name", "ws 4");
        extSrv.repl_add_ws(args);
        //
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 4);
        extSrv.repl_ws_disable(args);


        // Создаем ящики рабочих станций
        args.clear();
        args.put("pass", mailPass);
        assertEquals("Ящики не созданы", true, extSrv.repl_mail_create(args));
        //createBoxes_Local();
        //
        FileUtils.copyFile(new File("test/etalon/ws_list.json"), new File("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/" + mailGuid + "/ws_list.json"));


        // ---
        // Задаем и отправляем конфигурацию станций
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 2);
        args.put("cfg", CfgType.DECODE);
        args.put("file", cfg_json_decode);
        extSrv.repl_send_cfg(args);
        args.put("file", cfg_json_publication_ws);
        extSrv.repl_send_struct(args);
        //
        args.clear();
        args.put("notSaveServiceState", true);
        args.put("ws", 3);
        args.put("cfg", CfgType.DECODE);
        args.put("file", cfg_json_decode);
        extSrv.repl_send_cfg(args);
        args.put("file", cfg_json_publication_ws);
        extSrv.repl_send_struct(args);
        //
        // args.clear();
        // args.put("notSaveServiceState", true);
        // args.put("ws", 4);
        // args.put("cfg", CfgType.DECODE);
        // args.put("file", cfg_json_decode);
        // extSrv.repl_send_cfg(args);
        // args.put("file", cfg_json_publication_ws);
        // extSrv.repl_send_struct(args);


        // ---
        // На рабочих станциях

        // Инициализация баз и начальный конфиг рабочих станций
        args.clear();
        args.put("ws", 2);
        args.put("mail", mailUrl);
        args.put("guid", mailGuid);
        extWs2.repl_create(args);
        //
        args.clear();
        args.put("ws", 3);
        args.put("mail", mailUrl);
        args.put("guid", mailGuid);
        extWs3.repl_create(args);


        // ---
        UtData.outTable(db.loadSql("select id, name, guid from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"));
    }

    /**
     * Стираем все каталоги с данными, почтой и т.п.
     */
    void clearAllTestData() {
        UtFile.cleanDir("../_test-data");
        UtFile.cleanDir("../_data_root");
        UtFile.cleanDir("temp/MailerLocalFiles");
    }

    @Test
    public void createBoxes_Http() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        for (Map.Entry<Long, IMailer> en : srv.mailerList.entrySet()) {
            long wsId = en.getKey();
            MailerHttp wsMailer = (MailerHttp) en.getValue();
            wsMailer.createMailBox("from");
            wsMailer.createMailBox("to");
            wsMailer.createMailBox("to001");
            System.out.println("wsId: " + wsId + ", boxes - ok");
        }
    }

    @Test
    public void test_enable() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);

        srv.disableWorkstation(1);
        srv.enableWorkstation(2);
        srv.disableWorkstation(3);
        srv.enableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"));
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_STATE" + "," + UtJdx.SYS_TABLE_PREFIX + "WS_STATE"));

        // Активируем рабочие станции
        srv.enableWorkstation(1);
        srv.enableWorkstation(2);
        srv.enableWorkstation(3);
        srv.enableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"));
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_STATE" + "," + UtJdx.SYS_TABLE_PREFIX + "WS_STATE"));

        //
        srv.disableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST"));
        UtData.outTable(db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_STATE" + "," + UtJdx.SYS_TABLE_PREFIX + "WS_STATE"));
    }

    @Test
    public void test_region_http() throws Exception {
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        UtTest utTest3 = new UtTest(db3);
        utTest3.make_Region_InsDel_1(struct3, 3);

        //
        sync_http_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_AllHttp_DumpTables() throws Exception {
        test_AllHttp();
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

    @Test
    public void test_AllHttp() throws Exception {
        logOn();
        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);

        //
        sync_http_1_2_3();
        sync_http_1_2_3();
    }

    @Test
    public void test_AllHttp_ws2_Change() throws Exception {
        logOn();
        //
        test_ws2_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);

        //
        sync_http_1_2_3();
        sync_http_1_2_3();
    }

/*
    @Test
    public void test_all_local() throws Exception {
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        //
        syncLocal();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }
*/


    @Test
    public void sync_http_1_2_3() throws Exception {
        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
        test_ws3_doReplSession();

        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
        test_ws3_doReplSession();

        srv_doReplSession();
    }

    public void sync_http_1_2() throws Exception {
        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();

        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
    }

    @Test
    public void sync_http_1_3() throws Exception {
        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws3_doReplSession();

        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws3_doReplSession();
    }

    public void sync_http_1() throws Exception {
        test_ws1_doReplSession();
        srv_doReplSession();
        test_ws1_doReplSession();
    }

    public void sync_http_2() throws Exception {
        srv_doReplSession();
        test_ws2_doReplSession();
        srv_doReplSession();
        test_ws2_doReplSession();
    }

    public void sync_http_3() throws Exception {
        srv_doReplSession();
        test_ws3_doReplSession();
        srv_doReplSession();
        test_ws3_doReplSession();
    }


    @Test
    public void syncLocal() throws Exception {
/*
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        test_ws_sendLocal();
        test_ws_receiveLocal();

        test_sync_srv_Local();

        test_ws_sendLocal();
        test_ws_receiveLocal();

        test_ws1_handleQueIn();
        test_ws2_handleQueIn();
        test_ws3_handleQueIn();

        //
        test_DumpTables();
*/
    }

    @Test
    public void test_DumpTables_2_3_5() throws Exception {
        do_DumpTables(db2, db3, db5, struct2, struct3, struct5);
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
    public void sync_http_1_2_3_5() throws Exception {
        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
        test_ws3_doReplSession();
        test_ws5_doReplSession();

        srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
        test_ws3_doReplSession();
        test_ws5_doReplSession();
    }


    void assertDbNotEquals(Db db, Db db2) throws Exception {
        System.out.println("compare A: " + db.getDbSource().getDatabase());
        System.out.println("     vs B: " + db2.getDbSource().getDatabase());

        //
        System.out.println("db A");
        Map<String, Map<String, String>> dbCrcSrv = loadWsDbDataCrc(db);
        System.out.println();

        System.out.println("db B");
        Map<String, Map<String, String>> dbCrcWs2 = loadWsDbDataCrc(db2);
        System.out.println();

        //
        Map<String, Set<String>> diffCrc = new HashMap<>();
        Map<String, Set<String>> diffNewIn1 = new HashMap<>();
        Map<String, Set<String>> diffNewIn2 = new HashMap<>();

        //
        UtDbComparer.compareDbDataCrc(dbCrcSrv, dbCrcWs2, diffCrc, diffNewIn1, diffNewIn2);
        //
        assertEquals(true, diffCrc.get("USRLOG").size() == 0);
        assertEquals(true, diffNewIn1.get("USRLOG").size() != 0 || diffNewIn2.get("USRLOG").size() != 0);
        //
        assertEquals(true, diffCrc.get("PAWNCHIT").size() == 0);
        assertEquals(true, diffNewIn1.get("PAWNCHIT").size() != 0 || diffNewIn2.get("PAWNCHIT").size() != 0);
        //
        assertEquals(true, diffCrc.get("PAWNCHITSUBJECT").size() == 0);
        assertEquals(true, diffNewIn1.get("PAWNCHITSUBJECT").size() != 0 || diffNewIn2.get("PAWNCHITSUBJECT").size() != 0);
        //
        //assertEquals(true, diffCrc.get("COMMENTTEXT").size() != 0);
        assertEquals(true, diffNewIn1.get("COMMENTTEXT").size() != 0 || diffNewIn2.get("COMMENTTEXT").size() != 0);
        //
        //assertEquals(true, diffCrc.get("COMMENTTIP").size() == 0);
        //assertEquals(true, diffNewIn1.get("COMMENTTIP").size() == 0);
        //assertEquals(true, diffNewIn2.get("COMMENTTIP").size() == 0);
        //
        assertEquals(true, diffCrc.get("LIC").size() != 0 || diffNewIn1.get("LIC").size() != 0 || diffNewIn2.get("LIC").size() != 0);
        //
        assertEquals(true, diffCrc.get("LICDOCVID").size() == 0);
        assertEquals(true, diffNewIn1.get("LICDOCVID").size() != 0 || diffNewIn2.get("LICDOCVID").size() != 0);
        //
        assertEquals(true, diffCrc.get("LICDOCTIP").size() == 0);
        assertEquals(true, diffNewIn1.get("LICDOCTIP").size() != 0 || diffNewIn2.get("LICDOCTIP").size() != 0);
        //
        //assertEquals(true, diffCrc.get("ULZ").size() == 0) todo не долждно быть изменений записи, только Ins
        assertEquals(true, diffNewIn1.get("ULZ").size() != 0 || diffNewIn2.get("ULZ").size() != 0);
        //
        //assertEquals(true, diffCrc.get("REGION").size() != 0);
        assertEquals(true, diffNewIn1.get("REGION").size() != 0 || diffNewIn2.get("REGION").size() != 0);
        //
        assertEquals(true, diffCrc.get("REGIONTIP").size() == 0);
        assertEquals(true, diffNewIn1.get("REGIONTIP").size() != 0 || diffNewIn2.get("REGIONTIP").size() != 0);
        //
        System.out.println();
    }

    protected void do_DumpTables(Db db1, Db db2, Db db3, IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct struct3) throws Exception {
        String db1name = UtFile.removeExt(UtFile.filename(db1.getDbSource().getDatabase()));
        String db2name = UtFile.removeExt(UtFile.filename(db2.getDbSource().getDatabase()));
        String db3name = UtFile.removeExt(UtFile.filename(db3.getDbSource().getDatabase()));

        UtTest utt1 = new UtTest(db1);
        utt1.dumpTable("lic", "../_test-data/csv/ws1-lic.csv", "nameF");
        utt1.dumpTable("ulz", "../_test-data/csv/ws1-ulz.csv", "name");
        UtTest utt2 = new UtTest(db2);
        utt2.dumpTable("lic", "../_test-data/csv/ws2-lic.csv", "nameF");
        utt2.dumpTable("ulz", "../_test-data/csv/ws2-ulz.csv", "name");
        UtTest utt3 = new UtTest(db3);
        utt3.dumpTable("lic", "../_test-data/csv/ws3-lic.csv", "nameF");
        utt3.dumpTable("ulz", "../_test-data/csv/ws3-ulz.csv", "name");

        // Lic
        String sql = "select Lic.nameF, Lic.nameI, Lic.nameO, Region.name as RegionName, RegionTip.name as RegionTip, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) left join RegionTip on (Region.RegionTip = RegionTip.id) order by Lic.NameF, Lic.NameI, Lic.NameO";
        DataStore st1 = db1.loadSql(sql);
        OutTableSaver svr1 = new OutTableSaver(st1);
        //svr1.save().toFile("../_test-data/csv/ws1-all.csv");
        DataStore st2 = db2.loadSql(sql);
        OutTableSaver svr2 = new OutTableSaver(st2);
        //svr2.save().toFile("../_test-data/csv/ws2-all.csv");
        DataStore st3 = db3.loadSql(sql);
        OutTableSaver svr3 = new OutTableSaver(st3);
        //svr3.save().toFile("../_test-data/csv/ws3-all.csv");

        // CommentText
        String sql_commentText = "select " +
                "(case when CommentText.Lic=0 then 0 else 1 end) as Lic, (case when CommentText.PawnChit=0 then 0 else 1 end) PawnChit, " +
                "CommentText.CommentText, " +
                "(case when CommentText.CommentUsr=0 then null else 1 end) CommentUsr, " +
                "CommentText.CommentDt, CommentTip.Name as CommentTipName\n" +
                "from CommentText, CommentTip\n" +
                "where CommentText.id <> 0 and CommentText.CommentTip = CommentTip.id\n" +
                "order by 2, CommentText.CommentDt, CommentText.CommentText\n";  //CommentTip.Name,
        DataStore st1_ctx = db1.loadSql(sql_commentText);
        OutTableSaver svr1_ctx = new OutTableSaver(st1_ctx);
        DataStore st2_ctx = db2.loadSql(sql_commentText);
        OutTableSaver svr2_ctx = new OutTableSaver(st2_ctx);
        DataStore st3_ctx = db3.loadSql(sql_commentText);
        OutTableSaver svr3_ctx = new OutTableSaver(st3_ctx);

        // CommentText
        String sql_commentTip = "select * from CommentTip order by Name";
        DataStore st1_ctt = db1.loadSql(sql_commentTip);
        OutTableSaver svr1_ctt = new OutTableSaver(st1_ctt);
        DataStore st2_ctt = db2.loadSql(sql_commentTip);
        OutTableSaver svr2_ctt = new OutTableSaver(st2_ctt);
        DataStore st3_ctt = db3.loadSql(sql_commentTip);
        OutTableSaver svr3_ctt = new OutTableSaver(st3_ctt);

        // Region*
        String regionTestFields = "";
        for (IJdxField f : struct1.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields = regionTestFields + "Region." + f.getName() + ",";
            }
        }
        String regionTestFields2 = "";
        for (IJdxField f : struct2.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields2 = regionTestFields2 + "Region." + f.getName() + ",";
            }
        }
        String regionTestFields3 = "";
        for (IJdxField f : struct3.getTable("region").getFields()) {
            if (f.getName().startsWith("TEST_FIELD_")) {
                regionTestFields3 = regionTestFields3 + "Region." + f.getName() + ",";
            }
        }
        String sql_ws1 = "select Region.Name as RegionName, " + regionTestFields + " RegionTip.Name as RegionTipName, RegionTip.ShortName as RegionTipShortName, regionTip.deleted as regionTipDeleted\n" +
                "from Region, RegionTip\n" +
                "where Region.id <> 0 and Region.regionTip = RegionTip.id\n" +
                "order by Region.Name, RegionTip.Name\n";
        String sql_ws2 = "select Region.Name as RegionName, " + regionTestFields2 + "  RegionTip.Name as RegionTipName, RegionTip.ShortName as RegionTipShortName, regionTip.deleted as regionTipDeleted\n" +
                "from Region, RegionTip\n" +
                "where Region.id <> 0 and Region.regionTip = RegionTip.id\n" +
                //"  and Region.id > 100000\n" + // доп условие - чтобы собственные данные ws1 не мозолили глаза
                "order by Region.Name, RegionTip.Name\n";
        String sql_ws3 = "select Region.Name as RegionName, " + regionTestFields3 + "  RegionTip.Name as RegionTipName, RegionTip.ShortName as RegionTipShortName, regionTip.deleted as regionTipDeleted\n" +
                "from Region, RegionTip\n" +
                "where Region.id <> 0 and Region.regionTip = RegionTip.id\n" +
                //"  and Region.id > 100000\n" + // доп условие - чтобы собственные данные ws1 не мозолили глаза
                "order by Region.Name, RegionTip.Name\n";
        DataStore st1_r = db1.loadSql(sql_ws1);
        OutTableSaver svr1_r = new OutTableSaver(st1_r);
        DataStore st2_r = db2.loadSql(sql_ws2);
        OutTableSaver svr2_r = new OutTableSaver(st2_r);
        DataStore st3_r = db3.loadSql(sql_ws3);
        OutTableSaver svr3_r = new OutTableSaver(st3_r);

        //
        String sql_bt = "select UsrLog.Info as UsrLogInfo\n" +
                "from UsrLog\n" +
                "where id <> 0 and Info <> ''\n" +
                "order by Info\n";
        DataStore st1_bt = db1.loadSql(sql_bt);
        OutTableSaver svr1_bt = new OutTableSaver(st1_bt);
        DataStore st2_bt = db2.loadSql(sql_bt);
        OutTableSaver svr2_bt = new OutTableSaver(st2_bt);
        DataStore st3_bt = db3.loadSql(sql_bt);
        OutTableSaver svr3_bt = new OutTableSaver(st3_bt);


        //
        String struct_t_XXX1 = dump_table_new_created(db1, struct1);
        String struct_t_XXX2 = dump_table_new_created(db2, struct2);
        String struct_t_XXX3 = dump_table_new_created(db3, struct3);

        //
        String title = testName.getMethodName();
        String titleStr0 = "===== " + title + " =====" + "\n\n";
        String titleStr1 = "\n\n" + "===== " + title + " =====";

        //
        String fileName1 = "../_test-data/csv/" + db1name + "-all.csv";
        String fileName2 = "../_test-data/csv/" + db2name + "-all.csv";
        String fileName3 = "../_test-data/csv/" + db3name + "-all.csv";
        UtFile.saveString(titleStr0 + svr1.save().toString() + "\n\n" + svr1_ctx.save().toString() + "\n\n" + svr1_ctt.save().toString() + "\n\n" + svr1_r.save().toString() + "\n\n" + struct_t_XXX1 + "\n\n" + svr1_bt.save().toString() + titleStr1, new File(fileName1));
        UtFile.saveString(titleStr0 + svr2.save().toString() + "\n\n" + svr2_ctx.save().toString() + "\n\n" + svr2_ctt.save().toString() + "\n\n" + svr2_r.save().toString() + "\n\n" + struct_t_XXX2 + "\n\n" + svr2_bt.save().toString() + titleStr1, new File(fileName2));
        UtFile.saveString(titleStr0 + svr3.save().toString() + "\n\n" + svr3_ctx.save().toString() + "\n\n" + svr3_ctt.save().toString() + "\n\n" + svr3_r.save().toString() + "\n\n" + struct_t_XXX3 + "\n\n" + svr3_bt.save().toString() + titleStr1, new File(fileName3));

        //
        startCmpDb(fileName1, fileName2, fileName3);
    }

    void startCmpDb(String fileName1, String fileName2, String fileName3) throws Exception {
        String batContent = "diff " + new File(fileName1).getAbsolutePath() + " " + new File(fileName2).getAbsolutePath() + " " + new File(fileName3).getAbsolutePath();
        File batFile = new File("temp/cmp_db.bat");
        UtFile.saveString(batContent, batFile);
        ProcessBuilder processBuilder = new ProcessBuilder("cmd", "/C", batFile.getAbsolutePath());
        processBuilder.directory(batFile.getParentFile());
        Process process = processBuilder.start();
        //process.waitFor();
    }

    private String dump_table_new_created(Db db, IJdxDbStruct struct) throws Exception {
        String content = "";
        for (IJdxTable table : struct.getTables()) {
            if (table.getName().startsWith("TEST_TABLE_")) {
                String selectFields = "name";
                for (IJdxField field : table.getFields()) {
                    if (field.getName().startsWith("TEST_FIELD_")) {
                        selectFields = selectFields + "," + field.getName();
                    }
                }
                String sql = "select " + selectFields + " from " + table.getName() + " order by name";
                DataStore st = db.loadSql(sql);
                OutTableSaver svr = new OutTableSaver(st);
                content = content + table.getName() + "\n";
                content = content + svr.save().toString() + "\n\n";
            }
        }

        return content;
    }

    @Test
    public void test_dump_table_testXXX() throws Exception {
        String struct_t_XXX = dump_table_new_created(db, struct);
        UtFile.saveString(struct_t_XXX, new File("../_test-data/csv/ws1-xxx.csv"));
    }

    @Test
    public void test_ws1_makeChange_Unimportant() throws Exception {
        UtTest utTest = new UtTest(db);
        utTest.makeChangeUnimportant(struct, 1);
    }

    @Test
    public void test_ws3_makeChange_Unimportant() throws Exception {
        UtTest utTest = new UtTest(db3);
        utTest.makeChangeUnimportant(struct3, 3);
    }

    @Test
    public void test_srv_make001() throws Exception {
        UtTest utTest = new UtTest(db);
        utTest.test_srv_make001();
    }

    @Test
    public void test_ws1_makeChange() throws Exception {
        UtTest utTest = new UtTest(db);
        utTest.makeChange(struct, 1);
    }

    @Test
    public void test_ws2_makeChange() throws Exception {
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct2, 2);
    }

    @Test
    public void test_ws3_makeChange() throws Exception {
        UtTest utTest = new UtTest(db3);
        utTest.makeChange(struct3, 3);
    }

    @Test
    public void test_ws5_makeChange() throws Exception {
        UtTest utTest = new UtTest(db5);
        utTest.makeChange(struct5, 5);
    }

    @Test
    public void test_ws2_getInfoWs() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        //
        Map<String, Object> info = ws.getInfoWs();
        System.out.println(info);
    }

    @Test
    public void test_getInfoSrv() throws Exception {
        UtRepl urRepl = new UtRepl(db, null);
        UtData.outTable(urRepl.getInfoSrv());
    }

    public void ws_doReplSession(Db db) throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws);
        replTask.doTask();
        //
        JdxTaskWsMailRequest mailTask = new JdxTaskWsMailRequest(ws);
        mailTask.doTask();
    }

    @Test
    public void test_ws1_doReplSession() throws Exception {
        ws_doReplSession(db);
    }

    @Test
    public void test_ws2_doReplSession() throws Exception {
        ws_doReplSession(db2);
    }

    @Test
    public void test_ws3_doReplSession() throws Exception {
        ws_doReplSession(db3);
    }

    @Test
    public void test_ws5_doReplSession() throws Exception {
        ws_doReplSession(db5);
    }

    @Test
    public void test_init_srv() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
    }

    @Test
    public void test_init_ws2() throws Exception {
        JdxReplSrv ws2 = new JdxReplSrv(db2);
        ws2.init();
    }

    @Test
    public void srv_doReplSession() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        JdxTaskSrvMail mailTask = new JdxTaskSrvMail(srv);
        JdxTaskSrvRepl replTask = new JdxTaskSrvRepl(srv);
        JdxTaskSrvMailRequest mailTaskRequest = new JdxTaskSrvMailRequest(srv);
        JdxTaskSrvCleanupRepl cleanupTask = new JdxTaskSrvCleanupRepl(srv);
        //
        mailTask.doTask();
        //
        replTask.doTask();
        //
        mailTask.doTask();
        //
        mailTaskRequest.doTask();
        //
        //cleanupTask.doTask();
    }


    @Test
    public void test_run_srv() throws Exception {
        while (true) {
            srv_doReplSession();
        }
    }

    @Test
    public void test_run_1() throws Exception {
        while (true) {
            test_ws1_makeChange_Unimportant();
            test_ws1_doReplSession();
        }
    }

    @Test
    public void test_run_2() throws Exception {
        while (true) {
            test_ws2_makeChange();
            test_ws2_doReplSession();
        }
    }

    @Test
    public void test_run_3() throws Exception {
        while (true) {
            test_ws3_makeChange();
            test_ws3_doReplSession();
        }
    }

    @Test
    public void test_loop_1_change() throws Exception {
        while (true) {
            try {
                reloadDbStructAll();
                test_ws1_makeChange_Unimportant();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg) || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg);
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_2_change() throws Exception {
        while (true) {
            try {
                reloadDbStructAll();
                test_ws2_makeChange();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg) || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg);
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_3_change() throws Exception {
        while (true) {
            try {
                reloadDbStructAll();
                test_ws3_makeChange();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg) || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg);
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_1_repl() throws Exception {
        Thread stateOuterThread = createStateOuterMailer(db);
        stateOuterThread.start();

        while (true) {
            try {
                reloadDbStructAll();

                //test_ws1_makeChange();
                test_ws1_makeChange_Unimportant();

                test_ws1_doReplSession();
                Thread.sleep(1000);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println("loop_1_repl: " + msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_2_repl() throws Exception {
        Thread stateOuterThread = createStateOuterMailer(db2);
        stateOuterThread.start();

        while (true) {
            try {
                reloadDbStructAll();

                UtTest utTest = new UtTest(db2);
                utTest.makeChangeMany();

                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                //test_ws2_makeChange();
                test_ws2_doReplSession();
                Thread.sleep(2000);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println("loop_2_repl: " + msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_3_repl() throws Exception {
        Thread stateOuterThread = createStateOuterMailer(db3);
        stateOuterThread.start();

        while (true) {
            try {
                reloadDbStructAll();

                //UtTest utTest = new UtTest(db3);
                //utTest.makeChangeMany();

                test_ws3_makeChange();
                //test_ws3_makeChange();
                //test_ws3_makeChange();

                test_ws3_makeChange_Unimportant();

                test_ws3_doReplSession();
                Thread.sleep(2000);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println("loop_3_repl: " + msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_5_repl() throws Exception {
        Thread stateOuterThread = createStateOuterMailer(db5);
        stateOuterThread.start();

        while (true) {
            try {
                //test_ws5_makeChange();
                //test_ws5_makeChange();
                //test_ws5_makeChange();

                test_ws5_doReplSession();
                Thread.sleep(2000);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println("loop_5_repl: " + msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


    @Test
    public void loop_srv() throws Exception {
        Thread stateOuterThread = createStateOuterMailer(db);
        stateOuterThread.start();

        while (true) {
            try {
                test_ws1_makeChange_Unimportant();

                srv_doReplSession();
                Thread.sleep(5000);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println("loop_srv: " + msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


    @Test
    public void loop_srv_ws1() throws Exception {
        Thread stateOuterThread = createStateOuterMailer(db);
        stateOuterThread.start();

        while (true) {
            try {
                test_ws1_makeChange_Unimportant();

                srv_doReplSession();

                test_ws1_doReplSession();

                Thread.sleep(5000);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println("loop_srv_ws1: " + msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


    //
    boolean canSkipException(String msg) {
        return msg.contains("deadlock") ||
                msg.contains("Connection reset") ||
                msg.contains("Connection refused: connect") ||
                msg.contains("Connection timed out: connect") ||
                (msg.contains("Item info") && msg.contains("not found"));
    }


}

