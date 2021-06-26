package jdtx.repl.main.api.manager;

import jdtx.repl.main.api.*;
import org.junit.*;

/**
 */
public class JdxStateManager_Test extends ReplDatabaseStruct_Test {


    long wsId_1 = 1;
    long wsId_2 = 2;
    long wsId_3 = 3;

    String cfg_json_decode = "test/etalon/decode_strategy.json";
    String cfg_json_publication_ws = "test/etalon/publication_full_152_ws.json";

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_StateManager_setUp() throws Exception {
        UtRepl utRepl = new UtRepl(db, struct);
        utRepl.dropReplication();
        utRepl.createReplication(wsId_1, "");

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.addWorkstation(wsId_1, "srv", "", cfg_json_publication_ws, cfg_json_decode);
        srv.addWorkstation(wsId_2, "ws 2", "", cfg_json_publication_ws, cfg_json_decode);
        srv.addWorkstation(wsId_3, "ws 3", "", cfg_json_publication_ws, cfg_json_decode);

        //
        System.out.println("wsId_2: " + wsId_2);
        System.out.println("wsId_3: " + wsId_3);
    }

    @Test
    public void test_StateManager_Srv() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        //
        System.out.println("getDispatchDoneQueCommon[" + wsId_1 + "]: " + stateManager.getDispatchDoneQueCommon(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInNoDone(wsId_1));
        //
        System.out.println("getDispatchDoneQueCommon[" + wsId_2 + "]: " + stateManager.getDispatchDoneQueCommon(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInNoDone(wsId_2));

        // ---
        stateManager.setDispatchDoneQueCommon(wsId_1, 20);
        stateManager.setWsQueInNoDone(wsId_1, 30);
        //
        stateManager.setDispatchDoneQueCommon(wsId_2, 21);
        stateManager.setWsQueInNoDone(wsId_2, 31);

        //
        System.out.println("getDispatchDoneQueCommon[" + wsId_1 + "]: " + stateManager.getDispatchDoneQueCommon(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInNoDone(wsId_1));
        //
        System.out.println("getDispatchDoneQueCommon[" + wsId_2 + "]: " + stateManager.getDispatchDoneQueCommon(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInNoDone(wsId_2));

        // ---
        stateManager.setDispatchDoneQueCommon(wsId_1, 920);
        stateManager.setWsQueInNoDone(wsId_1, 930);
        //
        stateManager.setDispatchDoneQueCommon(wsId_2, 921);
        stateManager.setWsQueInNoDone(wsId_2, 931);

        //
        System.out.println("getDispatchDoneQueCommon[" + wsId_1 + "]: " + stateManager.getDispatchDoneQueCommon(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInNoDone(wsId_1));
        //
        System.out.println("getDispatchDoneQueCommon[" + wsId_2 + "]: " + stateManager.getDispatchDoneQueCommon(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInNoDone(wsId_2));
    }

    @Test
    public void test_StateManager_Ws() throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        System.out.println("getAuditAgeDone: " + stateManager.getAuditAgeDone());
        System.out.println("getQueInNoDone: " + stateManager.getQueNoDone("in"));

        // ---
        stateManager.setAuditAgeDone(20);
        stateManager.setQueNoDone("in", 30);

        //
        System.out.println("getAuditAgeDone: " + stateManager.getAuditAgeDone());
        System.out.println("getQueInNoDone: " + stateManager.getQueNoDone("in"));

        // ---
        stateManager.setAuditAgeDone(920);
        stateManager.setQueNoDone("in", 930);

        //
        System.out.println("getAuditAgeDone: " + stateManager.getAuditAgeDone());
        System.out.println("getQueInNoDone: " + stateManager.getQueNoDone("in"));
    }


    @Test
    public void test_StateManagerMailWs() throws Exception {
        JdxMailStateManagerWs stateMailManager = new JdxMailStateManagerWs(db);

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


}
