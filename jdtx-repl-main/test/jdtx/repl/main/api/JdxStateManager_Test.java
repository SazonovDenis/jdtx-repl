package jdtx.repl.main.api;

import jdtx.repl.main.api.jdx_db_object.*;
import org.junit.*;

/**
 */
public class JdxStateManager_Test extends ReplDatabaseStruct_Test {


    long wsId_1 = 1;
    long wsId_2 = 2;
    long wsId_3 = 3;

    @Test
    public void test_StateManager_setUp() throws Exception {
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.dropAudit();
        ut.createRepl(wsId_1, "");

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.addWorkstation(wsId_1, "srv", "");
        srv.addWorkstation(wsId_2, "ws 2", "");
        srv.addWorkstation(wsId_3, "ws 3", "");

        //
        System.out.println("wsId_2: " + wsId_2);
        System.out.println("wsId_3: " + wsId_3);
    }

    @Test
    public void test_StateManager_Srv() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        //
        System.out.println("getCommonQueDispatchDone[" + wsId_1 + "]: " + stateManager.getCommonQueDispatchDone(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInAgeDone(wsId_1));
        //
        System.out.println("getCommonQueDispatchDone[" + wsId_2 + "]: " + stateManager.getCommonQueDispatchDone(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInAgeDone(wsId_2));

        // ---
        stateManager.setCommonQueDispatchDone(wsId_1, 20);
        stateManager.setWsQueInAgeDone(wsId_1, 30);
        //
        stateManager.setCommonQueDispatchDone(wsId_2, 21);
        stateManager.setWsQueInAgeDone(wsId_2, 31);

        //
        System.out.println("getCommonQueDispatchDone[" + wsId_1 + "]: " + stateManager.getCommonQueDispatchDone(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInAgeDone(wsId_1));
        //
        System.out.println("getCommonQueDispatchDone[" + wsId_2 + "]: " + stateManager.getCommonQueDispatchDone(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInAgeDone(wsId_2));

        // ---
        stateManager.setCommonQueDispatchDone(wsId_1, 920);
        stateManager.setWsQueInAgeDone(wsId_1, 930);
        //
        stateManager.setCommonQueDispatchDone(wsId_2, 921);
        stateManager.setWsQueInAgeDone(wsId_2, 931);

        //
        System.out.println("getCommonQueDispatchDone[" + wsId_1 + "]: " + stateManager.getCommonQueDispatchDone(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInAgeDone(wsId_1));
        //
        System.out.println("getCommonQueDispatchDone[" + wsId_2 + "]: " + stateManager.getCommonQueDispatchDone(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInAgeDone(wsId_2));
    }

    @Test
    public void test_StateManager_Ws() throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        System.out.println("getAuditAgeDone: " + stateManager.getAuditAgeDone());
        System.out.println("getQueInNoDone: " + stateManager.getQueInNoDone());

        // ---
        stateManager.setAuditAgeDone(20);
        stateManager.setQueInNoDone(30);

        //
        System.out.println("getAuditAgeDone: " + stateManager.getAuditAgeDone());
        System.out.println("getQueInNoDone: " + stateManager.getQueInNoDone());

        // ---
        stateManager.setAuditAgeDone(920);
        stateManager.setQueInNoDone(930);

        //
        System.out.println("getAuditAgeDone: " + stateManager.getAuditAgeDone());
        System.out.println("getQueInNoDone: " + stateManager.getQueInNoDone());
    }


    @Test
    public void test_StateManagerMailWs() throws Exception {
        JdxStateManagerMail stateMailManager = new JdxStateManagerMail(db);

        //
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());

        // ---
        stateMailManager.setMailSendDone(720);
        stateMailManager.setMailSendDone(730);
        stateMailManager.setMailSendDone(740);

        //
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());

        // ---
        stateMailManager.setMailSendDone(701);
        stateMailManager.setMailSendDone(701);

        //
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
        System.out.println("mailSendDone: " + stateMailManager.getMailSendDone());
    }


    @Test
    public void test_StateManagerMailSrv() throws Exception {
/*
        JdxStateManagerMail stateMailManager = new JdxStateManagerMail(db);

        //
        System.out.println("wsId: " + wsId_1 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_1));
        System.out.println("wsId: " + wsId_2 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_2));
        System.out.println("wsId: " + wsId_3 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_3));

        // ---
        stateMailManager.setMailSendDoneForWs(wsId_1, 20);
        stateMailManager.setMailSendDoneForWs(wsId_2, 30);
        stateMailManager.setMailSendDoneForWs(wsId_3, 40);

        //
        System.out.println("wsId: " + wsId_1 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_1));
        System.out.println("wsId: " + wsId_2 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_2));
        System.out.println("wsId: " + wsId_3 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_3));

        // ---
        stateMailManager.setMailSendDoneForWs(wsId_1, 201);
        stateMailManager.setMailSendDoneForWs(wsId_2, 301);

        //
        System.out.println("wsId: " + wsId_1 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_1));
        System.out.println("wsId: " + wsId_2 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_2));
        System.out.println("wsId: " + wsId_3 + ", mailSendDone: " + stateMailManager.getMailSendDoneForWs(wsId_3));
*/
    }


}
