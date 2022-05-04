package jdtx.repl.main.api.que;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

public class JdxQueCommon_Test extends Database_Test {


    @Override
    public void setUp() throws Exception {
        super.setUp();
        logOff();
    }


    @Test
    public void test_x() throws Exception {
        // Ради смены версии БД
        logOn();
        JdxReplWs srv = new JdxReplWs(db1);
        srv.init();
    }


    @Test
    public void test_getNoByAuthorNo() throws Exception {

        // Общая очередь на сервере
        IJdxQueCommon queCommon = new JdxQueCommon(db1, UtQue.SRV_QUE_COMMON, UtQue.STATE_AT_SRV);

        //
        for (int wsId = 1; wsId <= 3; wsId++) {
            for (int noWs = 1; noWs <= 20; noWs++) {
                long noCommon = queCommon.getNoByAuthorNo(noWs, wsId);
                System.out.println("ws: " + wsId + ", noWs: " + noWs + ", noCommon: " + noCommon);
            }
            System.out.println();
        }


        //
        try {
            queCommon.getNoByAuthorNo(0, 1);
            throw new Exception("Test should fail");
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("Replica number not found")) {
                throw e;
            }
            System.out.println(UtJdxErrors.collectExceptionText(e));
        }

        //
        try {
            queCommon.getNoByAuthorNo(1, 0);
            throw new Exception("Test should fail");
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("Replica number not found")) {
                throw e;
            }
            System.out.println(UtJdxErrors.collectExceptionText(e));
        }

        //
        try {
            queCommon.getNoByAuthorNo(1, 9999);
            throw new Exception("Test should fail");
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("Replica number not found")) {
                throw e;
            }
            System.out.println(UtJdxErrors.collectExceptionText(e));
        }

        //
        try {
            queCommon.getNoByAuthorNo(99999, 1);
            throw new Exception("should fail");
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("Replica number not found")) {
                throw e;
            }
            System.out.println(UtJdxErrors.collectExceptionText(e));
        }
    }


}