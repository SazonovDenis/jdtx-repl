package jdtx.repl.main.service;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.junit.*;

import java.util.*;

public class UtReplService_Test extends AppTestCase {


    // ---
    // Службы ServiceInfo
    // ---

    @Test
    public void test_ServiceListPrint() throws Exception {
        logOn();

        //
        List<ServiceInfo> serviceList = UtReplService.serviceList();
        ServiceInfo.printList(serviceList);
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

        //
        test_ServiceListPrint();
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

        //
        test_ServiceListPrint();
    }

    @Test
    public void test_removeAll() throws Exception {
        logOn();

        //
        UtReplService.removeAll();

        //
        test_ServiceListPrint();
    }


    // ---
    // Процессы (ProcessInfo)
    // ---

    @Test
    public void test_processList() throws Exception {
        logOn();

        //
        Collection<ProcessInfo> processList = UtReplService.processList();
        ProcessInfo.printList(processList);
    }

    @Test
    public void test_start() throws Exception {
        logOn();

        //
        UtReplService.start();
    }

    @Test
    public void test_stop_one() throws Exception {
        logOn();

        //
        UtReplService.stop(false);
    }

    @Test
    public void test_stop_all() throws Exception {
        logOn();

        //
        UtReplService.stop(true);
    }


}
