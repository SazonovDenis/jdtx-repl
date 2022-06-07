package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;
import org.junit.*;


/**
 * Проверяем, как рабочая станциия и сервер реагируют на просьбы прислать реплики (required)
 */
public class JdxReplWsSrv_ExecRequired_Test extends JdxReplWsSrv_Test {


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
        srv.init();

        ws1 = new JdxReplWs(db);
        ws1.init();

        ws2 = new JdxReplWs(db2);
        ws2.init();

        ws3 = new JdxReplWs(db3);
        ws3.init();

        mailer_ws1 = ws1.getMailer();
        mailer_ws2 = ws2.getMailer();
        mailer_ws3 = ws3.getMailer();
    }


    @Test
    public void cleanMail() throws Exception {
        mailer_ws1.setSendRequired("from", new RequiredInfo());
        mailer_ws1.setSendRequired("to", new RequiredInfo());
        mailer_ws2.setSendRequired("from", new RequiredInfo());
        mailer_ws2.setSendRequired("to", new RequiredInfo());
        mailer_ws3.setSendRequired("from", new RequiredInfo());
        mailer_ws3.setSendRequired("to", new RequiredInfo());

        mailer_ws1.delete("from", 10000);
        mailer_ws1.delete("to", 10000);
        mailer_ws2.delete("from", 10000);
        mailer_ws2.delete("to", 10000);
        mailer_ws3.delete("from", 10000);
        mailer_ws3.delete("to", 10000);
    }

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
        assertEquals(no_ws2_from, +ws2.queOut.getMaxNo());
        assertEquals(no_ws2_to, +ws2.queIn.getMaxNo());

        //
        doRequiredWs(ws2, no_ws2_from, no_ws2_to);

        //
        System.out.println();
        System.out.println("===");
        System.out.println();

        //
        doRequiredWs(ws2, 1, 1);
    }

    /**
     * Проверяем, что если установлено SendRequired,
     * то запрошенные номера не удаляются при при вызове mailer.delete
     */
    @Test
    public void test_NoDeleteIfRequired() throws Exception {
        cleanMail();

        //
        IMailer mailerWs2 = ws2.getMailer();

        // ---
        // Проверяем, что при вызове mailer.delete
        // из яшика удаляются все, что младше указанного номера

        //
        JSONObject files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(0L, files.get("min"));
        assertEquals(0L, files.get("max"));

        //
        long count = 5;
        long maxNo = ws2.queOut.getMaxNo();
        long deleteNo = maxNo - 2;
        long requiredNo = maxNo - 2;

        //
        long no = maxNo - count;
        while (no <= maxNo) {
            IReplica replica = ws2.queOut.get(no);
            mailerWs2.send(replica, "from", no);
            no = no + 1;
        }

        //
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(maxNo - count, files.get("min"));
        assertEquals(maxNo, files.get("max"));


        // Удаляем не последнее
        mailerWs2.delete("from", deleteNo);

        //
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(deleteNo + 1, files.get("min"));
        assertEquals(maxNo, files.get("max"));


        // Удаляем последнее
        mailerWs2.delete("from", maxNo);

        //
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(0L, files.get("min"));
        assertEquals(0L, files.get("max"));


        // ---
        // Проверяем, что если установлено SendRequired,
        // то то запрошенные номера не удаляются при при вызове mailer.delete
        System.out.println();
        System.out.println("Проверяем если установлено SendRequired");

        //
        no = maxNo - count;
        while (no <= maxNo) {
            IReplica replica = ws2.queOut.get(no);
            mailerWs2.send(replica, "from", no);
            no = no + 1;
        }

        //
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(maxNo - count, files.get("min"));
        assertEquals(maxNo, files.get("max"));


        // Задаем SendRequired
        RequiredInfo required = new RequiredInfo();
        required.requiredFrom = requiredNo;
        required.executor = RequiredInfo.EXECUTOR_WS;
        mailerWs2.setSendRequired("from", required);


        // Удаляем последнее
        mailerWs2.delete("from", maxNo);

        // Удалилось НЕ до конца
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(requiredNo, files.get("min"));
        assertEquals(maxNo, files.get("max"));


        // Убираем SendRequired
        required = new RequiredInfo();
        mailerWs2.setSendRequired("from", required);


        // Удаляем последнее
        mailerWs2.delete("from", maxNo);

        // Удалилось до конца
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(0L, files.get("min"));
        assertEquals(0L, files.get("max"));
    }

    private void doRequiredWs(JdxReplWs ws, long no_from, long no_to) throws Exception {
        IMailer mailer = ws.getMailer();

        //
        System.out.println();

        // Проверим, что нет ничего
        checkBoxEmpty(mailer, "from", no_from);
        checkBoxEmpty(mailer, "to", no_to);
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
        required_from.requiredFrom = no_from;
        required_from.requiredTo = no_from;
        //
        RequiredInfo required_to = new RequiredInfo();
        required_to.requiredFrom = no_to;
        required_to.requiredTo = no_to;

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
        checkBoxNotEmpty(ws.queOut, mailer, "from", no_from);
        checkBoxEmpty(mailer, "to", no_to);
        System.out.println();

        //
        cleanMail();
        checkBoxEmpty(mailer, "from", no_from);
        checkBoxEmpty(mailer, "to", no_to);


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
        checkBoxNotEmpty(ws.queOut, mailer, "from", no_from);
        checkBoxNotEmpty(ws.queIn, mailer, "to", no_to);
    }

    void showRequired() throws Exception {
        RequiredInfo ws1_from = ws1.getMailer().getSendRequired("from");
        RequiredInfo ws1_to = ws1.getMailer().getSendRequired("to");
        RequiredInfo ws1_to001 = ws1.getMailer().getSendRequired("to001");
        RequiredInfo ws2_from = ws2.getMailer().getSendRequired("from");
        RequiredInfo ws2_to = ws2.getMailer().getSendRequired("to");
        RequiredInfo ws2_to001 = ws2.getMailer().getSendRequired("to001");
        RequiredInfo ws3_from = ws3.getMailer().getSendRequired("from");
        RequiredInfo ws3_to = ws3.getMailer().getSendRequired("to");
        RequiredInfo ws3_to001 = ws3.getMailer().getSendRequired("to001");

        System.out.println("ws1.from: " + ws1_from);
        System.out.println("      to: " + ws1_to);
        System.out.println("   to001: " + ws1_to001);
        System.out.println("ws2.from: " + ws2_from);
        System.out.println("      to: " + ws2_to);
        System.out.println("   to001: " + ws2_to001);
        System.out.println("ws3.from: " + ws3_from);
        System.out.println("      to: " + ws3_to);
        System.out.println("   to001: " + ws3_to001);
    }

    private void doSendRequiredWs(JdxReplWs ws) throws Exception {
        System.out.println("Send required, ws: " + ws.getWsId());
        ws.replicasSend_Required();
    }

    private void doSendRequiredSrv() throws Exception {
        System.out.println("Send required, srv");
        srv.replicasSend_Requied();
    }

    private void checkRquiredInfo(JdxReplWs ws, boolean rquiredFrom, boolean rquiredTo) throws Exception {
        RequiredInfo requiredFrom = ws.getMailer().getSendRequired("from");
        RequiredInfo requiredTo = ws.getMailer().getSendRequired("to");
        assertEquals(rquiredFrom, requiredFrom.requiredFrom != -1);
        assertEquals(rquiredTo, requiredTo.requiredTo != -1);
    }

    private void checkBoxEmpty(IMailer mailer, String box, long no) throws Exception {
        try {
            IReplicaInfo res = mailer.getReplicaInfo(box, no);
            throw new Exception("Test should fail, but replica found, box: " + box + ", no: " + no);
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("Replica not found, guid")) {
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

