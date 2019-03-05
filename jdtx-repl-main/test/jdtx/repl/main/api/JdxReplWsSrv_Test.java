package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.junit.*;

import java.util.concurrent.*;

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_all_0() throws Exception {
        test_srv_setUp();
        //
        test_ws2_CreateSetupReplica();
        test_ws3_CreateSetupReplica();
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();
        test_srv_handleQue();
        test_ws1_ApplyReplica();
        test_ws2_ApplyReplica();
        test_ws3_ApplyReplica();
        //
        test_dumpTables();
    }

    @Test
    public void test_all_1() throws Exception {
        //test_ws1_makeChange();
        test_ws2_makeChange();
        test_ws3_makeChange();
        test_ws1_handleSelfAudit();
        test_ws2_handleSelfAudit();
        test_ws3_handleSelfAudit();
        test_srv_handleQue();
        test_ws1_ApplyReplica();
        test_ws2_ApplyReplica();
        test_ws3_ApplyReplica();
        //
        test_dumpTables();
    }

    @Test
    public void test_srv_setUp() throws Exception {
        UtFile.cleanDir("../_test-data/csv");
        UtFile.cleanDir("../_test-data/mail");
        UtFile.cleanDir("../_test-data/srv");
        UtFile.cleanDir("../_test-data/ws_srv");
        UtFile.cleanDir("../_test-data/ws2");
        UtFile.cleanDir("../_test-data/ws3");
        // db
        UtRepl utr = new UtRepl(db);
        utr.dropReplication();
        utr.createReplication();
        // db2
        UtRepl utr2 = new UtRepl(db2);
        utr2.dropReplication();
        utr2.createReplication();
        // db3
        UtRepl utr3 = new UtRepl(db3);
        utr3.dropReplication();
        utr3.createReplication();

        //
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        long wsId_2 = ut.addWorkstation("ws 2");
        long wsId_3 = ut.addWorkstation("ws 3");

        //
        System.out.println("wsId_2: " + wsId_2);
        System.out.println("wsId_3: " + wsId_3);
    }


    @Test
    public void test_ws2_CreateSetupReplica() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws2 = new JdxReplWs(db2, 2);
        ws2.init("test/etalon/mail_http_ws2.json");

        // Забираем установочную реплику
        ws2.createSetupReplica();

        //
        ws2.send();
    }

    @Test
    public void test_ws3_CreateSetupReplica() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws3 = new JdxReplWs(db3, 3);
        ws3.init("test/etalon/mail_http_ws3.json");

        // Забираем установочную реплику
        ws3.createSetupReplica();

        //
        ws3.send();
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

        DataStore st1 = db.loadSql("select Lic.nameF,  Lic.nameI,  Lic.nameO, Region.name as RegionName, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) order by Lic.NameF");
        OutTableSaver svr1 = new OutTableSaver(st1);
        svr1.save().toFile("../_test-data/csv/ws1-all.csv");
        DataStore st2 = db2.loadSql("select Lic.nameF,  Lic.nameI,  Lic.nameO, Region.name as RegionName, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) order by Lic.NameF");
        OutTableSaver svr2 = new OutTableSaver(st2);
        svr2.save().toFile("../_test-data/csv/ws2-all.csv");
        DataStore st3 = db3.loadSql("select Lic.nameF,  Lic.nameI,  Lic.nameO, Region.name as RegionName, UlzTip.name as UlzTip, Ulz.name as UlzName, Lic.Dom, Lic.Kv, Lic.tel from Lic left join Ulz on (Lic.Ulz = Ulz.id) left join UlzTip on (Ulz.UlzTip = UlzTip.id) left join Region on (Ulz.Region = Region.id) order by Lic.NameF");
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
        JdxReplWs ws = new JdxReplWs(db, 1);
        ws.init("test/etalon/mail_http_ws_srv.json");

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();

        //
        ws.send();
    }

    @Test
    public void test_ws2_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db2, 2);
        ws.init("test/etalon/mail_http_ws2.json");

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();

        //
        ws.send();
    }

    @Test
    public void test_ws3_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws = new JdxReplWs(db3, 3);
        ws.init("test/etalon/mail_http_ws3.json");

        // Отслеживаем и обрабатываем свои изменения
        ws.handleSelfAudit();

        //
        ws.send();
    }


    @Test
    public void test_ws1_ApplyReplica() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db, 1);
        ws.init("test/etalon/mail_http_ws_srv.json");

        // Забираем входящие реплики
        ws.receive();

        // Применяем входящие реплики
        ws.handleQueIn();
    }

    @Test
    public void test_ws2_ApplyReplica() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db2, 2);
        ws.init("test/etalon/mail_http_ws2.json");

        // Забираем входящие реплики
        ws.receive();

        // Применяем входящие реплики
        ws.handleQueIn();
    }

    @Test
    public void test_ws3_ApplyReplica() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db3, 3);
        ws.init("test/etalon/mail_http_ws3.json");

        // Забираем входящие реплики
        ws.receive();

        // Применяем входящие реплики
        ws.handleQueIn();
    }


    @Test
    public void test_srv_handleQue() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init("test/etalon/mail_http_srv.json");

        // Формирование общей очереди
        srv.srvFillCommonQue();

        // Тиражирование реплик
        srv.srvDispatchReplicas();
    }


    /////////////////////////////////////////////////

    @Test
    public void test_run_srv() throws Exception {
        while (true) {
            test_srv_handleQue();
        }
    }

    @Test
    public void test_run_1() throws Exception {
        while (true) {
            //test_ws1_makeChange();
            test_ws1_handleSelfAudit();
            test_ws1_ApplyReplica();
        }
    }

    @Test
    public void test_run_2() throws Exception {
        while (true) {
            test_ws2_makeChange();
            test_ws2_handleSelfAudit();
            test_ws2_ApplyReplica();
        }
    }

    @Test
    public void test_run_3() throws Exception {
        while (true) {
            test_ws3_makeChange();
            test_ws3_handleSelfAudit();
            test_ws3_ApplyReplica();
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
    public void test_loop_2_change() throws Exception {
        while (true) {
            try {
                test_ws2_makeChange();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
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
    public void test_loop_3_change() throws Exception {
        while (true) {
            try {
                test_ws3_makeChange();
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
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
    public void test_loop_1_repl() throws Exception {
        while (true) {
            try {
                test_ws1_handleSelfAudit();
                test_ws1_ApplyReplica();
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
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
    public void test_loop_2_repl() throws Exception {
        while (true) {
            try {
                test_ws2_handleSelfAudit();
                test_ws2_ApplyReplica();
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
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
    public void test_loop_3_repl() throws Exception {
        while (true) {
            try {
                test_ws3_handleSelfAudit();
                test_ws3_ApplyReplica();
            } catch (Exception e) {
                String msg = e.getCause().getMessage();
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
    public void test_loop_srv() throws Exception {
        while (true) {
            test_srv_handleQue();
        }
    }


}
