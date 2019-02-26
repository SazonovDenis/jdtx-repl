package jdtx.repl.main.api;

import org.junit.*;

public class JdxReplWsSrv_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_srv_setUp() throws Exception {
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        UtDbObjectManager ut_2 = new UtDbObjectManager(db2, struct);
        //UtDbObjectManager ut_3 = new UtDbObjectManager(db3, struct);
        //
        ut.dropAudit();
        ut_2.dropAudit();
        //ut_3.dropAudit();
        //
        ut.createAudit();
        ut_2.createAudit();
        //ut_3.createAudit();

        //
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
