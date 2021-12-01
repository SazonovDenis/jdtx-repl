package jdtx.repl.main.service;

import jandcode.app.test.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
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
        UtReplService.printTaskList(serviceList);
    }

    @Test
    public void test_install() throws Exception {
        logOn();

        // БД
        Db db = app.service(ModelService.class).getModel().getDb();
        db.connect();

        //
        UtReplService.install(db);

        //
        test_ServiceListPrint();
    }

    @Test
    public void test_remove() throws Exception {
        logOn();

        // БД
        Db db = app.service(ModelService.class).getModel().getDb();
        db.connect();

        //
        UtReplService.remove(db);

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
        UtReplService.printProcessList(processList);
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

    @Test
    public void test_stop_uninstall() throws Exception {
        logOff();

        // БД
        Db db = app.service(ModelService.class).getModel().getDb();
        db.connect();

        //
        System.out.println("===");
        ReplServiceState serviceState = UtReplService.readServiceState(db);
        System.out.println("started: " + serviceState.isStarted);
        System.out.println("installed: " + serviceState.isInstalled);

        //
        System.out.println("===");
        System.out.println("UtReplService.stop");
        UtReplService.stop(false);
        UtReplService.processList();

        //
        System.out.println("===");
        System.out.println("UtReplService.remove");
        UtReplService.remove(db);

        //
        System.out.println("===");
        System.out.println("UtReplService.start");
        UtReplService.start();

        //
        System.out.println("===");
        System.out.println("UtReplService.install");
        UtReplService.install(db);

        //
        System.out.println("===");
        UtReplService.remove(db);
    }



}
