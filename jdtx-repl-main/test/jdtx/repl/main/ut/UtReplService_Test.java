package jdtx.repl.main.ut;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.junit.*;

public class UtReplService_Test extends AppTestCase {


    @Test
    public void test_serviceList() throws Exception {
        logOn();
        UtReplService.serviceList();
    }

    @Test
    public void test_install() throws Exception {
        // БД
        Db db = app.service(ModelService.class).getModel().getDb();
        db.connect();

        // Рабочая станция
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        UtReplService.install(ws);
    }

    @Test
    public void test_remove() throws Exception {
        // БД
        Db db = app.service(ModelService.class).getModel().getDb();
        db.connect();

        // Рабочая станция
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        UtReplService.remove(ws);
    }

    @Test
    public void test_start() throws Exception {
        UtReplService.start();
    }

    @Test
    public void test_processList() throws Exception {
        UtReplService.processList();
    }

    @Test
    public void test_stop() throws Exception {
        UtReplService.stop();
    }

}
