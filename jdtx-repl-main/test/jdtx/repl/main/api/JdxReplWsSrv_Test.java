package jdtx.repl.main.api;

import jandcode.bgtasks.*;
import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.junit.*;

import java.io.*;
import java.util.concurrent.*;

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_all_setUp() throws Exception {
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
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/17845f2f56f4d401/from");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/17845f2f56f4d401/to");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/21ba238dfc945002/from");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/21ba238dfc945002/to");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/34f3cc20bea64503/from");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/34f3cc20bea64503/to");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/444fed23da93ab04/from");
        UtFile.cleanDir("../../lombard.systems/repl/b5781df573ca6ee6/444fed23da93ab04/to");


        // db
        UtRepl utr = new UtRepl(db);
        utr.dropReplication();
        utr.createReplication(1, "b5781df573ca6ee6-17845f2f56f4d401");
        // db2
        UtRepl utr2 = new UtRepl(db2);
        utr2.dropReplication();
        utr2.createReplication(2, "b5781df573ca6ee6-21ba238dfc945002");
        // db3
        UtRepl utr3 = new UtRepl(db3);
        utr3.dropReplication();
        utr3.createReplication(3, "b5781df573ca6ee6-34f3cc20bea64503");


        // Добавляем рабочие станции для режима сервера
        UtDbObjectManager om = new UtDbObjectManager(db, struct);
        om.addWorkstation(1, "Сервер", "b5781df573ca6ee6-17845f2f56f4d401");
        om.addWorkstation(2, "ws 2", "b5781df573ca6ee6-21ba238dfc945002");
        om.addWorkstation(3, "ws 3", "b5781df573ca6ee6-34f3cc20bea64503");
        om.addWorkstation(4, "ws 4", "b5781df573ca6ee6-444fed23da93ab04");
        // Активируем рабочие станции
        om.enableWorkstation(1);
        om.enableWorkstation(2);
        om.enableWorkstation(3);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
    }

    @Test
    public void test_enable() throws Exception {
        UtDbObjectManager om = new UtDbObjectManager(db, struct);

        om.disableWorkstation(1);
        om.enableWorkstation(2);
        om.disableWorkstation(3);
        om.enableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "db_info"));

        // Активируем рабочие станции
        om.enableWorkstation(1);
        om.enableWorkstation(2);
        om.enableWorkstation(3);
        om.enableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "db_info"));

        //
        om.disableWorkstation(4);
        //
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation_list"));
        UtData.outTable(db.loadSql("select * from " + JdxUtils.sys_table_prefix + "db_info"));
    }

    @Test
    public void test_all_start() throws Exception {
        test_all_setUp();
        //
        test_ws2_CreateSnapshotReplica();
        test_ws3_CreateSnapshotReplica();
        //
        test_dumpTables();
    }

    @Test
    public void test_all_http() throws Exception {
        //test_ws1_makeChange();
        test_ws2_makeChange();
        test_ws3_makeChange();

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

        //
        test_dumpTables();
    }

    @Test
    public void test_all_local() throws Exception {
        //test_ws1_makeChange();
        test_ws2_makeChange();
        test_ws3_makeChange();

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
    public void test_all_syncLocal() throws Exception {
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
    public void test_ws2_CreateSnapshotReplica() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws2 = new JdxReplWs(db2);
        //ws2.init("test/etalon/ws.json");
        ws2.init("test/etalon/mail_http_ws.json");

        // Создаем установочную реплику
        ws2.createSnapshotReplica();
    }

    @Test
    public void test_ws3_CreateSnapshotReplica() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws3 = new JdxReplWs(db3);
        //ws3.init("test/etalon/ws.json");
        ws3.init("test/etalon/mail_http_ws.json");

        // Создаем установочную реплику
        ws3.createSnapshotReplica();
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

        DataStore st1 = db.loadSql("select Lic.nameF, Lic.nameI, Lic.nameO, Region.name as RegionName, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) order by Lic.NameF");
        OutTableSaver svr1 = new OutTableSaver(st1);
        svr1.save().toFile("../_test-data/csv/ws1-all.csv");
        DataStore st2 = db2.loadSql("select Lic.nameF, Lic.nameI, Lic.nameO, Region.name as RegionName, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) order by Lic.NameF");
        OutTableSaver svr2 = new OutTableSaver(st2);
        svr2.save().toFile("../_test-data/csv/ws2-all.csv");
        DataStore st3 = db3.loadSql("select Lic.nameF, Lic.nameI, Lic.nameO, Region.name as RegionName, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) order by Lic.NameF");
        OutTableSaver svr3 = new OutTableSaver(st3);
        svr3.save().toFile("../_test-data/csv/ws3-all.csv");
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
    public void test_ws1_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
    }

    @Test
    public void test_ws2_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db2);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
    }

    @Test
    public void test_ws3_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db3);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();
    }


    @Test
    public void test_ws1_send_receive() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        //
        ws.send();
        ws.receive();
    }

    @Test
    public void test_ws2_send_receive() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db2);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        //
        ws.send();
        ws.receive();
    }

    @Test
    public void test_ws3_send_receive() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db3);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        //
        ws.send();
        ws.receive();
    }


    @Test
    public void test_ws1_handleQueIn() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        // Применяем входящие реплики
        ws.handleQueIn();
    }

    @Test
    public void test_ws2_handleQueIn() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db2);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        // Применяем входящие реплики
        ws.handleQueIn();
    }

    @Test
    public void test_ws3_handleQueIn() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db3);
        //ws.init("test/etalon/ws.json");
        ws.init("test/etalon/mail_http_ws.json");

        // Применяем входящие реплики
        ws.handleQueIn();
    }


    @Test
    public void test_ws_sendLocal() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws1 = new JdxReplWs(db);
        ws1.init("test/etalon/mail_http_ws.json");
        //
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init("test/etalon/mail_http_ws.json");
        //
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init("test/etalon/mail_http_ws.json");

        //
        //JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        //long srvSendAge = stateManager.getMailSendDone();
        //long selfQueOutAge = ws1.queOut.getMaxAge();
        ws1.sendToDir("test/etalon/mail_http_ws.json", "../_test-data/mail_local", 0, 0, false);

        //
        //stateManager = new JdxStateManagerMail(db2);
        //srvSendAge = stateManager.getMailSendDone();
        //selfQueOutAge = ws2.queOut.getMaxAge();
        ws2.sendToDir("test/etalon/mail_http_ws.json", "../_test-data/mail_local", 0, 0, false);

        //
        //stateManager = new JdxStateManagerMail(db3);
        //srvSendAge = stateManager.getMailSendDone();
        //selfQueOutAge = ws3.queOut.getMaxAge();
        ws3.sendToDir("test/etalon/mail_http_ws.json", "../_test-data/mail_local/", 0, 0, false);
    }


    @Test
    public void test_ws_receiveLocal() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws1 = new JdxReplWs(db);
        ws1.init("test/etalon/mail_http_ws.json");
        //
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init("test/etalon/mail_http_ws.json");
        //
        JdxReplWs ws3 = new JdxReplWs(db3);
        ws3.init("test/etalon/mail_http_ws.json");

        //
        ws1.receiveFromDir("test/etalon/mail_http_ws.json", "../_test-data/mail_local");
        ws2.receiveFromDir("test/etalon/mail_http_ws.json", "../_test-data/mail_local");
        ws3.receiveFromDir("test/etalon/mail_http_ws.json", "../_test-data/mail_local/");
    }

    @Test
    public void test_sync_srv() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        //srv.init("test/etalon/srv.json");
        srv.init("test/etalon/mail_http_srv.json");

        // Формирование общей очереди
        srv.srvHandleCommonQue();

        // Тиражирование реплик
        srv.srvDispatchReplicas();
    }

    @Test
    public void test_sync_srv_Local() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        //srv.init("test/etalon/srv.json");
        srv.init("test/etalon/mail_http_srv.json");

        // Формирование общей очереди
        srv.srvHandleCommonQueFrom("test/etalon/mail_http_srv.json", "../_test-data/mail_local");

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir("test/etalon/mail_http_srv.json", "../_test-data/mail_local", 0, 0, 0, false);
    }

    @Test
    public void test_srvDispatchReplicasToDir_ws2() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        //srv.init("test/etalon/srv.json");
        srv.init("test/etalon/mail_http_srv.json");

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir("test/etalon/mail_http_srv.json", "../_test-data/mail_local", 0, 0, 2, false);
    }

    @Test
    public void test_srvDispatchReplicasToDir_wsAll() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        //srv.init("test/etalon/srv.json");
        srv.init("test/etalon/mail_http_srv.json");

        // Тиражирование реплик
        srv.srvDispatchReplicasToDir("test/etalon/mail_http_srv.json", "../_test-data/mail_local", 0, 0, 0, false);
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
            //test_ws1_makeChange();
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

/*
    @Test
    public void test_loop_1_change() throws Exception {
        while (true) {
            test_ws1_makeChange();
        }
    }
*/

    @Test
    public void loop_2_change() throws Exception {
        while (true) {
            try {
                test_ws2_makeChange();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
                if (msg.contains("deadlock") || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                test_ws3_makeChange();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
                if (msg.contains("deadlock") || msg.contains("violation of FOREIGN KEY constraint")) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                String msg;
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                } else {
                    msg = e.getMessage();
                }
                if (msg.contains("deadlock") ||
                        msg.contains("Connection reset") ||
                        msg.contains("Connection refused: connect") ||
                        msg.contains("Connection timed out: connect") ||
                        msg.contains("Connection refused: connect") ||
                        (msg.contains("Item info") && msg.contains("not found"))
                        ) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                String msg;
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                } else {
                    msg = e.getMessage();
                }
                if (msg == null) {
                    msg = e.toString();
                }
                if (msg.contains("deadlock") ||
                        msg.contains("Connection reset") ||
                        msg.contains("Connection refused: connect") ||
                        msg.contains("Connection timed out: connect") ||
                        msg.contains("Connection refused: connect") ||
                        (msg.contains("Item info") && msg.contains("not found"))
                        ) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                String msg;
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                } else {
                    msg = e.getMessage();
                }
                if (msg == null) {
                    msg = e.toString();
                }
                if (msg.contains("deadlock") ||
                        msg.contains("Connection reset") ||
                        msg.contains("Connection refused: connect") ||
                        msg.contains("Connection timed out: connect") ||
                        msg.contains("Connection refused: connect") ||
                        (msg.contains("Item info") && msg.contains("not found"))
                        ) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                String msg;
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                } else {
                    msg = e.getMessage();
                }
                if (msg == null) {
                    msg = e.toString();
                }
                if (msg.contains("deadlock") ||
                        msg.contains("Connection refused: connect") ||
                        msg.contains("Connection timed out: connect") ||
                        msg.contains("Connection refused: connect")
                        ) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                String msg;
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                } else {
                    msg = e.getMessage();
                }
                if (msg.contains("deadlock")) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
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
                String msg;
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                } else {
                    msg = e.getMessage();
                }
                if (msg.contains("deadlock") ||
                        (msg.contains("Item info") && msg.contains("not found")) ||
                        (msg.contains("Source") && msg.contains("does not exist"))
                        ) {
                    System.out.println(msg);
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                } else {
                    throw e;
                }
            }
        }
    }


}
