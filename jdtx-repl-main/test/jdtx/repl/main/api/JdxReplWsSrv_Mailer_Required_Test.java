package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.task.*;
import org.json.simple.*;
import org.junit.*;


/**
 * Проверяем, как рабочая станциия и сервер реагируют на просьбы прислать реплики (required)
 */
public class JdxReplWsSrv_Mailer_Required_Test extends JdxReplWsSrv_Test {


    JdxReplSrv srv;
    JdxReplWs ws1;
    JdxReplWs ws2;
    JdxReplWs ws3;
    IMailer mailer_ws1;
    IMailer mailer_ws2;
    IMailer mailer_ws3;


    @Override
    public void setUp() throws Exception {
        super.setUp();

        srv = new JdxReplSrv(db);
        srv.errorCollector = new JdxErrorCollector();
        srv.init();

        ws1 = new JdxReplWs(db);
        ws1.errorCollector = new JdxErrorCollector();
        ws1.init();

        ws2 = new JdxReplWs(db2);
        ws2.errorCollector = new JdxErrorCollector();
        ws2.init();

        ws3 = new JdxReplWs(db3);
        ws3.errorCollector = new JdxErrorCollector();
        ws3.init();

        mailer_ws1 = ws1.getMailer();
        mailer_ws2 = ws2.getMailer();
        mailer_ws3 = ws3.getMailer();
    }


    @Test
    public void cleanMail() throws Exception {
        mailer_ws1.deleteAll("from", 10000);
        mailer_ws1.deleteAll("to", 10000);
        mailer_ws2.deleteAll("from", 10000);
        mailer_ws2.deleteAll("to", 10000);
        mailer_ws3.deleteAll("from", 10000);
        mailer_ws3.deleteAll("to", 10000);
    }

    /**
     * Проверяем рассылку по требованию
     */
    @Test
    public void test_doRequired() throws Exception {
        //
        cleanMail();

        //
        System.out.println();

        //
        long no_ws2_from = mailer_ws2.getSendDone("from");
        long no_ws2_to = mailer_ws2.getSendDone("to");
        System.out.println("ws: " + ws2.getWsId() + ", sendDone from: " + no_ws2_from);
        System.out.println("ws: " + ws2.getWsId() + ", sendDone to: " + no_ws2_to);
        assertEquals(no_ws2_from, ws2.queOut.getMaxNo());
        assertEquals(no_ws2_to, ws2.queIn.getMaxNo());

        //
        doRequiredWs(ws2, no_ws2_from, no_ws2_to, 3);

        //
        System.out.println();
        System.out.println("===");
        System.out.println();

        //
        doRequiredWs(ws2, 1, 1, 0);
    }

    private void doRequiredWs(JdxReplWs ws, long no_box_from, long no_box_to, int count) throws Exception {
        IMailer mailer = ws.getMailer();

        //
        System.out.println();

        // Проверим, что нет ничего
        checkBoxEmpty(mailer, "from", no_box_from);
        checkBoxEmpty(mailer, "to", no_box_to);
        System.out.println();

        // Покажем состояние rquired
        showRequired();
        checkRquiredInfo(ws1, false, false);
        checkRquiredInfo(ws2, false, false);
        checkRquiredInfo(ws3, false, false);
        System.out.println();


        // ---
        System.out.println();

        //
        RequiredInfo required_from = new RequiredInfo();
        required_from.requiredFrom = no_box_from - count;
        required_from.requiredTo = no_box_from;
        //
        RequiredInfo required_to = new RequiredInfo();
        required_to.requiredFrom = no_box_to - count;
        required_to.requiredTo = no_box_to;

        // Запросим from со станции ws
        // (правильно)
        System.out.println("Запросим 'from' со станции");
        required_from.executor = RequiredInfo.EXECUTOR_WS;
        mailer.setSendRequired("from", required_from);

        // Запросим to с сервера для станции ws
        // (неправильно)
        System.out.println("Запросим 'to' с сервера");
        required_to.executor = RequiredInfo.EXECUTOR_WS;
        mailer.setSendRequired("to", required_to);

        // Покажем состояние rquired
        showRequired();
        checkRquiredInfo(ws2, true, true);
        System.out.println();


        // ---
        System.out.println();

        // Выполним
        // (удачно для from, неудачно для to)
        doSendRequiredWs(ws2);
        System.out.println();

        // Покажем состояние rquired
        showRequired();
        checkRquiredInfo(ws2, false, true);
        System.out.println();

        // Проверим, что в ящиках
        checkBoxNotEmpty(ws.queOut, mailer, "from", no_box_from);
        checkBoxEmpty(mailer, "to", no_box_to);
        System.out.println();

        //
        cleanMail();
        checkBoxEmpty(mailer, "from", no_box_from);
        checkBoxEmpty(mailer, "to", no_box_to);


        // ---
        System.out.println();

        // Запросим from с сервера для станции ws
        // (правильно)
        System.out.println("Запросим 'from' с сервера");
        required_from.executor = RequiredInfo.EXECUTOR_SRV;
        mailer.setSendRequired("from", required_from);

        // Запросим to с сервера для станции ws
        // (правильно)
        System.out.println("Запросим 'to' с сервера");
        required_to.executor = RequiredInfo.EXECUTOR_SRV;
        mailer.setSendRequired("to", required_to);

        // Покажем состояние rquired
        showRequired();
        checkRquiredInfo(ws2, true, true);
        System.out.println();


        // ---
        System.out.println();

        // Выполним
        // (удачно для from и to)
        doSendRequiredSrv();
        System.out.println();

        // Покажем состояние rquired
        showRequired();
        checkRquiredInfo(ws2, false, false);
        System.out.println();

        // Проверим, что в ящиках
        checkBoxNotEmpty(ws.queOut, mailer, "from", no_box_from);
        checkBoxNotEmpty(ws.queIn, mailer, "to", no_box_to);
    }

    void showRequired() throws Exception {
        RequiredInfo ws1_box_from = ws1.getMailer().getSendRequired("from");
        RequiredInfo ws1_box_to = ws1.getMailer().getSendRequired("to");
        RequiredInfo ws1_box_to001 = ws1.getMailer().getSendRequired("to001");
        RequiredInfo ws2_box_from = ws2.getMailer().getSendRequired("from");
        RequiredInfo ws2_box_to = ws2.getMailer().getSendRequired("to");
        RequiredInfo ws2_box_to001 = ws2.getMailer().getSendRequired("to001");
        RequiredInfo ws3_box_from = ws3.getMailer().getSendRequired("from");
        RequiredInfo ws3_box_to = ws3.getMailer().getSendRequired("to");
        RequiredInfo ws3_box_to001 = ws3.getMailer().getSendRequired("to001");

        System.out.println("ws1, box from: " + ws1_box_from);
        System.out.println("       box to: " + ws1_box_to);
        System.out.println("    box to001: " + ws1_box_to001);
        System.out.println("ws2, box from: " + ws2_box_from);
        System.out.println("       box to: " + ws2_box_to);
        System.out.println("    box to001: " + ws2_box_to001);
        System.out.println("ws3, box from: " + ws3_box_from);
        System.out.println("       box to: " + ws3_box_to);
        System.out.println("    box to001: " + ws3_box_to001);
    }

    private void doSendRequiredWs(JdxReplWs ws) throws Exception {
        System.out.println("Send required, ws: " + ws.getWsId());
        ws.replicasSend_Required();
    }

    private void doSendRequiredSrv() throws Exception {
        System.out.println("Send required, srv");
        srv.replicasSend_Requied();
    }

    private void checkRquiredInfo(JdxReplWs ws, boolean isRequired_box_from, boolean isRequired_box_to) throws Exception {
        RequiredInfo requiredFrom = ws.getMailer().getSendRequired("from");
        RequiredInfo requiredTo = ws.getMailer().getSendRequired("to");
        assertEquals(isRequired_box_from, requiredFrom.requiredFrom != -1);
        assertEquals(isRequired_box_to, requiredTo.requiredTo != -1);
    }

    private void checkBoxEmpty(IMailer mailer, String box, long no) throws Exception {
        try {
            IReplicaInfo res = mailer.getReplicaInfo(box, no);
            throw new Exception("Test should fail, but replica found, box: " + box + ", no: " + no);
        } catch (Exception e) {
            if (!UtJdxErrors.errorIs_replicaMailNotFound(e)) {
                throw e;
            }
            System.out.println("Replica not found, box: " + box + ", no: " + no);
        }
    }

    private void checkBoxNotEmpty(IJdxQue que, IMailer mailer, String box, long no) throws Exception {
        IReplicaInfo replicaInfoMail = mailer.getReplicaInfo(box, no);
        IReplicaInfo replicaInfoQue = que.get(no).getInfo();
        if (!replicaInfoQue.getCrc().equalsIgnoreCase(replicaInfoMail.getCrc())) {
            throw new Exception("Replica que.crc <> mail.crc, box: " + box + ", no: " + no);
        }
        System.out.println("Replica found, box: " + box + ", no: " + replicaInfoMail.getNo() + ", crs: " + replicaInfoMail.getCrc());
    }


}

