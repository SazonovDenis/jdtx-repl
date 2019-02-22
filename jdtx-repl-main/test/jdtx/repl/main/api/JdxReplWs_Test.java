package jdtx.repl.main.api;

import org.junit.*;

public class JdxReplWs_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_ws2_CreateSetupReplica() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init("test/etalon/ws2.json");


        // Забираем установочную реплику
        ws2.createSetupReplica();

        //
        ws2.send();
    }

    @Test
    public void test_makeChange() throws Exception {
        // Делаем изменения
        UtTest utTest = new UtTest(db2);
        utTest.makeChange(struct2);
    }

    @Test
    public void test_ws2_handleSelfAudit() throws Exception {
        // Рабочая станция, настройка
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init("test/etalon/ws2.json");


        // Отслеживаем и обрабатываем свои изменения
        ws2.handleSelfAudit();

        //
        ws2.send();
    }

    @Test
    public void test_srv_ApplyReplica() throws Exception {
        // Сервер, настройка
        JdxReplWs ws = new JdxReplWs(db);
        ws.init("test/etalon/ws_srv.json");


        // Забираем входящие реплики
        ws.receive();


        // Применяем входящие реплики
        ws.handleQueIn();

    }


}
