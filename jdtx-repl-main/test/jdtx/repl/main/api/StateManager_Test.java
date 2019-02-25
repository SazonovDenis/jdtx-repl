package jdtx.repl.main.api;

import org.junit.*;

/**
 */
public class StateManager_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_StateManager_setUp() throws Exception {
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.dropAudit();
        ut.createAudit();

        //
        long wsId_2 = ut.addWorkstation("ws 2");
        long wsId_3 = ut.addWorkstation("ws 3");

        //
        System.out.println("wsId_2: " + wsId_2);
        System.out.println("wsId_3: " + wsId_3);
    }

    @Test
    public void test_StateManager_Srv() throws Exception {
        long wsId_1 = 1;
        long wsId_2 = 2;

        //
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        //
        System.out.println("getCommonQueNoDone[" + wsId_1 + "]: " + stateManager.getCommonQueNoDone(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInAgeDone(wsId_1));
        //
        System.out.println("getCommonQueNoDone[" + wsId_2 + "]: " + stateManager.getCommonQueNoDone(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInAgeDone(wsId_2));

        // ---
        stateManager.setCommonQueNoDone(wsId_1, 20);
        stateManager.setWsQueInAgeDone(wsId_1, 30);
        //
        stateManager.setCommonQueNoDone(wsId_2, 21);
        stateManager.setWsQueInAgeDone(wsId_2, 31);

        //
        System.out.println("getCommonQueNoDone[" + wsId_1 + "]: " + stateManager.getCommonQueNoDone(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInAgeDone(wsId_1));
        //
        System.out.println("getCommonQueNoDone[" + wsId_2 + "]: " + stateManager.getCommonQueNoDone(wsId_2));
        System.out.println("getWsQueInAgeDone[" + wsId_2 + "]: " + stateManager.getWsQueInAgeDone(wsId_2));

        // ---
        stateManager.setCommonQueNoDone(wsId_1, 920);
        stateManager.setWsQueInAgeDone(wsId_1, 930);
        //
        stateManager.setCommonQueNoDone(wsId_2, 921);
        stateManager.setWsQueInAgeDone(wsId_2, 931);

        //
        System.out.println("getCommonQueNoDone[" + wsId_1 + "]: " + stateManager.getCommonQueNoDone(wsId_1));
        System.out.println("getWsQueInAgeDone[" + wsId_1 + "]: " + stateManager.getWsQueInAgeDone(wsId_1));
        //
        System.out.println("getCommonQueNoDone[" + wsId_2 + "]: " + stateManager.getCommonQueNoDone(wsId_2));
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


}
