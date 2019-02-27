package jdtx.repl.main.api;

import org.junit.*;

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_srv_setUp() throws Exception {
        // db
        UtRepl utr = new UtRepl(db);
        utr.dropReplication();
        utr.createReplication();
        // db2
        UtRepl utr2 = new UtRepl(db2);
        utr2.dropReplication();
        utr2.createReplication();
        // db3
        //UtRepl utr3 = new UtRepl(db3);
        //utr3.dropReplication();
        //utr3.createReplication();

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
        ws2.init("test/etalon/ws2.json");

        // Забираем установочную реплику
        ws2.createSetupReplica();

        //
        ws2.send();
    }


    @Test
    public void test_ws2_makeChange() throws Exception {
        // Делаем изменения
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct2);
    }


    @Test
    public void test_ws2_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws2 = new JdxReplWs(db2, 2);
        ws2.init("test/etalon/ws2.json");

        // Отслеживаем и обрабатываем свои изменения
        ws2.handleSelfAudit();

        //
        ws2.send();
    }


    @Test
    public void test_ws2_ApplyReplica() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db2, 2);
        ws.init("test/etalon/ws2.json");

        // Забираем входящие реплики
        ws.receive();

        // Применяем входящие реплики
        ws.handleQueIn();
    }


    @Test
    public void test_srv_ApplyReplica() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db, 1);
        ws.init("test/etalon/ws_srv.json");

        // Забираем входящие реплики
        ws.receive();

        // Применяем входящие реплики
        ws.handleQueIn();
    }


    @Test
    public void test_srv_handleQue() throws Exception {
        // Сервер, настройка
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init("test/etalon/srv.json");

        // Формирование общей очереди
        srv.srvFillCommonQue();

        // Тиражирование реплик
        srv.srvDispatchReplicas();
    }


}
