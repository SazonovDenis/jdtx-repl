package jdtx.repl.main.api;

import jandcode.bgtasks.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ut.*;
import org.junit.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {


    String json_srv = "test/etalon/mail_http_srv.json";
    String json_ws = "test/etalon/mail_http_ws.json";

    String cfg_json_ws = "test/etalon/ws.json";
    String cfg_json_decode = "test/etalon/decode_strategy.json";
    //String cfg_json_publications_full_152 = "test/etalon/publication_full_152.json";
    String cfg_json_publication_lic_152_srv = "test/etalon/publication_lic_152_srv.json";
    String cfg_json_publication_lic_152_ws = "test/etalon/publication_lic_152_ws.json";


    @Test
    public void allSetUp() throws Exception {
        doDisconnectAll();
        prepareEtalon();
        doConnectAll();
        //
        clearAllTestData();

        //
        IVariantMap args = new VariantMap();

        // ---
        // На рабочих станциях

        // Первичная инициализация и начальный конфиг рабочих станций
        args.clear();
        args.put("ws", 1);
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401");
        args.put("file", cfg_json_ws);
        extSrv.repl_create(args);

        args.clear();
        args.put("ws", 2);
        args.put("guid", "b5781df573ca6ee6.x-21ba238dfc945002");
        args.put("file", cfg_json_ws);
        extWs2.repl_create(args);

        args.clear();
        args.put("ws", 3);
        args.put("guid", "b5781df573ca6ee6.x-34f3cc20bea64503");
        args.put("file", cfg_json_ws);
        extWs3.repl_create(args);


        // ---
        // На сервере

        // Начальный конфиг сервера: напрямую задаем структуру публикаций (команда repl_set_cfg)
        args.clear();
        //args.put("file", cfg_json_publications_full_152);
        args.put("file", cfg_json_publication_lic_152_srv);
        args.put("cfg", UtCfgType.PUBLICATIONS);
        extSrv.repl_set_cfg(args);

        // Добавляем рабочие станции
        args.clear();
        args.put("ws", 1);
        args.put("name", "Сервер");
        args.put("guid", "b5781df573ca6ee6.x-17845f2f56f4d401");
        extSrv.repl_add_ws(args);
        //
        args.clear();
        args.put("ws", 2);
        args.put("name", "ws 2");
        args.put("guid", "b5781df573ca6ee6.x-21ba238dfc945002");
        extSrv.repl_add_ws(args);
        //
        args.clear();
        args.put("ws", 3);
        args.put("name", "ws 3");
        args.put("guid", "b5781df573ca6ee6.x-34f3cc20bea64503");
        extSrv.repl_add_ws(args);
        //
        args.clear();
        args.put("ws", 4);
        args.put("name", "ws 4");
        args.put("guid", "b5781df573ca6ee6.x-444fed23da93ab04");
        extSrv.repl_add_ws(args);

        // Активируем 3 рабочие станции
        args.clear();
        args.put("ws", 1);
        extSrv.repl_enable(args);
        //
        args.clear();
        args.put("ws", 2);
        extSrv.repl_enable(args);
        //
        args.clear();
        args.put("ws", 3);
        extSrv.repl_enable(args);


        // Создаем ящики рабочих станций
        args.clear();
        args.put("create", true);
        assertEquals("Ящики не созданы", true, extSrv.repl_mail_check(args));
        //createBoxes_Local();

        // Сразу рассылаем настройки для всех станций
        args.clear();
        args.put("file", cfg_json_decode);
        args.put("cfg", UtCfgType.DECODE);
        extSrv.repl_send_cfg(args);
        //
        args.clear();
        args.put("file", cfg_json_publication_lic_152_srv);
        args.put("cfg", UtCfgType.PUBLICATIONS);
        args.put("ws", 1);
        extSrv.repl_send_cfg(args);
        //
        args.clear();
        args.put("file", cfg_json_publication_lic_152_ws);
        args.put("cfg", UtCfgType.PUBLICATIONS);
        args.put("ws", 2);
        extSrv.repl_send_cfg(args);
        //
        args.clear();
        args.put("file", cfg_json_publication_lic_152_ws);
        args.put("cfg", UtCfgType.PUBLICATIONS);
        args.put("ws", 3);
        extSrv.repl_send_cfg(args);

        // Для сервера - сразу инициируем фиксацию структуры БД
        args.clear();
        extSrv.repl_dbstruct_finish(args);


        // ---
        UtData.outTable(db.loadSql("select id, name, guid from " + JdxUtils.sys_table_prefix + "workstation_list"));
    }

    private void doConnectAll() throws Exception {
        db.connect();
        db2.connect();
        db3.connect();
    }

    private void doDisconnectAll() throws Exception {
        db.disconnect();
        db2.disconnect();
        db3.disconnect();
    }

    /**
     * Стираем все каталоги с данными, почтой и т.п.
     */
    private void clearAllTestData() {
        UtFile.cleanDir("../_test-data/csv");
        UtFile.cleanDir("../_test-data/mail");
        UtFile.cleanDir("../_test-data/mail_local");
        UtFile.cleanDir("../_test-data/srv");
        UtFile.cleanDir("../_test-data/ws_001");
        UtFile.cleanDir("../_test-data/ws_002");
        UtFile.cleanDir("../_test-data/ws_003");
        UtFile.cleanDir("../_test-data/ws_004");
        new File("../_test-data/csv").delete();
        new File("../_test-data/mail").delete();
        new File("../_test-data/mail_local").delete();
        new File("../_test-data/srv").delete();
        new File("../_test-data/ws_001").delete();
        new File("../_test-data/ws_002").delete();
        new File("../_test-data/ws_003").delete();
        new File("../_test-data/ws_004").delete();
        new File("d:/temp/dbm.log").delete();
        new File("d:/temp/jdtx.log").delete();
        UtFile.cleanDir("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/b5781df573ca6ee6.x");
    }


    @Test
    public void wsDbStructUpdate() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init();

        //
        ws.dbStructApplyFixed();
        ws2.dbStructApplyFixed();
        ws3.dbStructApplyFixed();
    }

    @Test
    public void createBoxes_Http() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        for (Map.Entry en : srv.mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            MailerHttp mailer = (MailerHttp) en.getValue();
            mailer.createMailBox("from");
            mailer.createMailBox("to");
            System.out.println("wsId: " + wsId + ", boxes - ok");
        }
    }

    @Test
    public void createBoxes_Local() throws Exception {
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/17845f2f56f4d401/from");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/17845f2f56f4d401/to");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/21ba238dfc945002/from");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/21ba238dfc945002/to");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/34f3cc20bea64503/from");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/34f3cc20bea64503/to");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/444fed23da93ab04/from");
        UtFile.cleanDir("../../lombard.systems/repl/v02/b5781df573ca6ee6.x/444fed23da93ab04/to");
    }

    @Test
    public void test_enable() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);

        srv.disableWorkstation(1);
        srv.enableWorkstation(2);
        srv.disableWorkstation(3);
        srv.enableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "state"));

        // Активируем рабочие станции
        srv.enableWorkstation(1);
        srv.enableWorkstation(2);
        srv.enableWorkstation(3);
        srv.enableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "state"));

        //
        srv.disableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "state"));
    }

    @Test
    public void test_region_http() throws Exception {
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_InsDel(struct2, 2);
        UtTest utTest3 = new UtTest(db3);
        utTest3.make_InsDel_1(struct3, 3);

        //
        sync_http();

        //
        test_dumpTables();
    }

    @Test
    public void test_all_http() throws Exception {
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_InsDel(struct2, 2);
        utTest2.make_InsDel_1(struct2, 2);

        //
        sync_http();
        sync_http();
        sync_http();
        sync_http();
        sync_http();

        //
        test_dumpTables();
    }

    @Test
    public void test_all_local() throws Exception {
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();

        //
        syncLocal();

        //
        test_dumpTables();
    }


    @Test
    public void sync_http() throws Exception {
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();

        test_ws1_send_receive();
        test_ws2_send_receive();
        test_ws3_send_receive();

        test_sync_srv();

        test_ws1_send_receive();
        test_ws2_send_receive();
        test_ws3_send_receive();

        test_ws1_handleQueIn();
        test_ws2_handleQueIn();
        test_ws3_handleQueIn();
    }

    @Test
    public void syncLocal() throws Exception {
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
        test_dumpTables();
    }

    @Test
    public void test_dumpTables() throws Exception {
        UtTest utt1 = new UtTest(db);
        utt1.dumpTable("lic", "../_test-data/csv/ws1-lic.csv", "nameF");
        utt1.dumpTable("ulz", "../_test-data/csv/ws1-ulz.csv", "name");
        UtTest utt2 = new UtTest(db2);
        utt2.dumpTable("lic", "../_test-data/csv/ws2-lic.csv", "nameF");
        utt2.dumpTable("ulz", "../_test-data/csv/ws2-ulz.csv", "name");
        UtTest utt3 = new UtTest(db3);
        utt3.dumpTable("lic", "../_test-data/csv/ws3-lic.csv", "nameF");
        utt3.dumpTable("ulz", "../_test-data/csv/ws3-ulz.csv", "name");

        //
        String sql = "select Lic.nameF, Lic.nameI, Lic.nameO, Region.name as RegionName, RegionTip.name as RegionTip, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) left join RegionTip on (Region.RegionTip = RegionTip.id) order by Lic.NameF";
        DataStore st1 = db.loadSql(sql);
        OutTableSaver svr1 = new OutTableSaver(st1);
        //svr1.save().saveToFile("../_test-data/csv/ws1-all.csv");
        DataStore st2 = db2.loadSql(sql);
        OutTableSaver svr2 = new OutTableSaver(st2);
        //svr2.save().saveToFile("../_test-data/csv/ws2-all.csv");
        DataStore st3 = db3.loadSql(sql);
        OutTableSaver svr3 = new OutTableSaver(st3);
        //svr3.save().saveToFile("../_test-data/csv/ws3-all.csv");

        // dumpTables Region*
        String regionTestFields = "";
        for (IJdxField f : struct.getTable("region").getFields()) {
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
        DataStore st1_r = db.loadSql(sql_ws1);
        OutTableSaver svr1_r = new OutTableSaver(st1_r);
        DataStore st2_r = db2.loadSql(sql_ws2);
        OutTableSaver svr2_r = new OutTableSaver(st2_r);
        DataStore st3_r = db3.loadSql(sql_ws3);
        OutTableSaver svr3_r = new OutTableSaver(st3_r);

        //
        String struct_t_XXX1 = dump_table_testXXX(db, struct);
        String struct_t_XXX2 = dump_table_testXXX(db2, struct2);
        String struct_t_XXX3 = dump_table_testXXX(db3, struct3);

        //
        UtFile.saveString(svr1.save().toString() + "\n\n" + svr1_r.save().toString() + "\n\n" + struct_t_XXX1, new File("../_test-data/csv/ws1-all.csv"));
        UtFile.saveString(svr2.save().toString() + "\n\n" + svr2_r.save().toString() + "\n\n" + struct_t_XXX2, new File("../_test-data/csv/ws2-all.csv"));
        UtFile.saveString(svr3.save().toString() + "\n\n" + svr3_r.save().toString() + "\n\n" + struct_t_XXX3, new File("../_test-data/csv/ws3-all.csv"));
    }

    private String dump_table_testXXX(Db db, IJdxDbStruct struct) throws Exception {
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
        String struct_t_XXX = dump_table_testXXX(db, struct);
        UtFile.saveString(struct_t_XXX, new File("../_test-data/csv/ws1-xxx.csv"));
    }

    @Test
    public void test_ws1_makeChange_Unimportant() throws Exception {
        UtTest utTest = new UtTest(db);
        utTest.makeChangeUnimportant(struct, 1);
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
    public void test_ws1_getInfoWs() throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        Map info = ws.getInfoWs();
        System.out.println(info);
    }

    @Test
    public void test_getInfoSrv() throws Exception {
        UtRepl urRepl = new UtRepl(db, null);
        UtData.outTable(urRepl.getInfoSrv());
    }

    @Test
    public void test_ws1_handleSelfAudit() throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
    }

    @Test
    public void test_ws2_handleSelfAudit() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
    }

    @Test
    public void test_ws3_handleSelfAudit() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
    }


    @Test
    public void test_ws1_send_receive() throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        ws.send();
        ws.receive();
    }

    @Test
    public void test_ws2_send_receive() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        //
        ws.send();
        ws.receive();
    }

    @Test
    public void test_ws3_send_receive() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();

        //
        ws.send();
        ws.receive();
    }


    @Test
    public void test_ws1_handleQueIn() throws Exception {
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        // Применяем входящие реплики
        ws.handleQueIn();
    }

    @Test
    public void test_ws2_handleQueIn() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        // Применяем входящие реплики
        ws.handleQueIn();
    }

    @Test
    public void test_ws3_handleQueIn() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();

        // Применяем входящие реплики
        ws.handleQueIn();
    }


    @Test
    public void test_ws_sendLocal() throws Exception {
        // Рабочие станции
        JdxReplWs ws1 = new JdxReplWs(db);
        ws1.init();
        //
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        //
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init();

        //
        //JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        //long srvSendAge = stateManager.getMailSendDone();
        //long selfQueOutAge = ws1.queOut.getMaxAge();
        ws1.sendToDir(json_ws, "../_test-data/mail_local", 0, 0, false);

        //
        //stateManager = new JdxStateManagerMail(db2);
        //srvSendAge = stateManager.getMailSendDone();
        //selfQueOutAge = ws2.queOut.getMaxAge();
        ws2.sendToDir(json_ws, "../_test-data/mail_local", 0, 0, false);

        //
        //stateManager = new JdxStateManagerMail(db3);
        //srvSendAge = stateManager.getMailSendDone();
        //selfQueOutAge = ws3.queOut.getMaxAge();
        ws3.sendToDir(json_ws, "../_test-data/mail_local/", 0, 0, false);
    }


    @Test
    public void test_ws_receiveLocal() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws1 = new JdxReplWs(db);
        ws1.init();
        //
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        //
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init();

        //
        ws1.receiveFromDir(json_ws, "../_test-data/mail_local");
        ws2.receiveFromDir(json_ws, "../_test-data/mail_local");
        ws3.receiveFromDir(json_ws, "../_test-data/mail_local/");
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
    public void test_sync_srv() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Формирование общей очереди
        srv.srvHandleCommonQue();

        // Тиражирование реплик
        srv.srvDispatchReplicas();
    }

    @Test
    public void test_sync_srv_Local() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Формирование общей очереди
        srv.srvHandleCommonQueFrom(json_srv, "../_test-data/mail_local");

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir(json_srv, "../_test-data/mail_local", 0, 0, 0, false);
    }

    @Test
    public void test_srvDispatchReplicasToDir_ws2() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir(json_srv, "../_test-data/mail_local", 0, 0, 2, false);
    }

    @Test
    public void test_srvDispatchReplicasToDir_wsAll() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir(json_srv, "../_test-data/mail_local", 0, 0, 0, false);
    }

    @Test
    public void test_z() throws Exception {
        BgTasksService bgTasksService = app.service(BgTasksService.class);
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName");
        System.out.println(cfgFileName);  // todo: почему не накладывается _app.rt ?
    }


    /////////////////////////////////////////////////

    @Test
    public void test_run_srv() throws Exception {
        while (true) {
            test_sync_srv();
        }
    }

    @Test
    public void test_run_1() throws Exception {
        while (true) {
            test_ws1_makeChange_Unimportant();
            test_ws1_handleSelfAudit();
            test_ws1_handleQueIn();
        }
    }

    @Test
    public void test_run_2() throws Exception {
        while (true) {
            test_ws2_makeChange();
            test_ws2_handleSelfAudit();
            test_ws2_handleQueIn();
        }
    }

    @Test
    public void test_run_3() throws Exception {
        while (true) {
            test_ws3_makeChange();
            test_ws3_handleSelfAudit();
            test_ws3_handleQueIn();
        }
    }

    /////////////////////////////////////////////////

    @Test
    public void test_loop_1_change() throws Exception {
        while (true) {
            try {
                reloadStruct_forTest();
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
                reloadStruct_forTest();
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
                reloadStruct_forTest();
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
        while (true) {
            try {
                test_ws1_handleSelfAudit();
                test_ws1_handleQueIn();
                test_ws1_send_receive();
                test_ws1_handleQueIn();
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_2_repl() throws Exception {
        while (true) {
            try {
                test_ws2_handleSelfAudit();
                test_ws2_handleQueIn();
                test_ws2_send_receive();
                test_ws2_handleQueIn();
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }

    @Test
    public void loop_3_repl() throws Exception {
        while (true) {
            try {
                test_ws3_handleSelfAudit();
                test_ws3_handleQueIn();
                test_ws3_send_receive();
                test_ws3_handleQueIn();
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


    @Test
    public void loop_srv() throws Exception {
        while (true) {
            try {
                test_sync_srv();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


    @Test
    public void loop_srv_local() throws Exception {
        while (true) {
            try {
                test_sync_srv_Local();
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


    @Test
    public void loop_ws_mail_Local() throws Exception {
        while (true) {
            try {
                test_ws_sendLocal();
                test_ws_receiveLocal();
                TimeUnit.SECONDS.sleep(20);
            } catch (Exception e) {
                String msg = Ut.getExceptionMessage(e);
                if (canSkipException(msg)) {
                    System.out.println(msg);
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
                msg.contains("Connection refused: connect") ||
                (msg.contains("Item info") && msg.contains("not found"));
    }


}

/*

^c
затестить применение конфигов при смене версии БД
реализовать невидимость таблиц, которых нет в конфиге
затестить инициализацию и смену версии БД на бинарной сборке

*/
