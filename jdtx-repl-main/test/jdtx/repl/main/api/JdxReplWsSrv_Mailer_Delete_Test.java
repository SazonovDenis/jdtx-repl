package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.task.*;
import org.json.simple.*;
import org.junit.*;


/**
 * Проверяем, как рабочая станциия и сервер реагируют на просьбы прислать реплики (required)
 */
public class JdxReplWsSrv_Mailer_Delete_Test extends JdxReplWsSrv_Test {


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
     * Проверяем, что если не установлено all,
     * то запрошенные номера не удаляются при при вызове mailer.delete
     */
    @Test
    public void test_NoDeleteIfNotAll() throws Exception {
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
        long minNo = maxNo - count;

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
        assertEquals(minNo, files.get("min"));
        assertEquals(maxNo, files.get("max"));


        // Удаляем последнее, установлено all
        mailerWs2.deleteAll("from", maxNo);

        //
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(0L, files.get("min"));
        assertEquals(0L, files.get("max"));


        // ---
        // Проверяем, что если не установлено all,
        // то то запрошенные номера не удаляются при при вызове mailer.delete
        System.out.println();
        System.out.println("Проверяем если не установлено all");

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


        // Удаляем последнее, не установлено all
        mailerWs2.delete("from", maxNo);

        // Удалилось НЕ до конца
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(minNo, files.get("min"));
        assertEquals(maxNo - 1, files.get("max"));


        // Удаляем последнее, установлено all
        mailerWs2.deleteAll("from", maxNo);

        // Удалилось до конца
        files = (JSONObject) mailerWs2.getData("files", "from").get("files");
        System.out.println();
        System.out.println("files: " + files);
        assertEquals(0L, files.get("min"));
        assertEquals(0L, files.get("max"));
    }


}

