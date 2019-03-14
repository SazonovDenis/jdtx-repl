package jdtx.repl.main.api;

import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import java.io.FileReader;
import java.io.Reader;

/**
 */
public class UtMailerHttp_Test extends ReplDatabase_Test {


    JSONObject cfgData;
    IJdxMailer mailer;


    @Override
    public void setUp() throws Exception {
        super.setUp();

        Reader r = new FileReader("test/etalon/mail_http_ws.json");
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }


        mailer = new UtMailerHttp();
        mailer.init(cfgData);
    }


    @Test
    public void test_getSrv() throws Exception {
        System.out.println("getSrvState.from: " + mailer.getSrvSate("from"));
        System.out.println("getSrvSate.to: " + mailer.getSrvSate("to"));
    }


    @Test
    public void test_Receive() throws Exception {
        IReplica replica_1 = mailer.receive(1, "to");
        System.out.println("receive: " + replica_1.getFile());
        //
        IReplica replica_2 = mailer.receive(2, "to");
        System.out.println("receive: " + replica_2.getFile());
        //
        IReplica replica_3 = mailer.receive(3, "to");
        System.out.println("receive: " + replica_3.getFile());
    }

    @Test
    public void test_Ping() throws Exception {
        DateTime state_dt_0 = mailer.getPingDt("to");
        System.out.println("state_dt: " + state_dt_0);

        //
        mailer.ping("to");

        //
        DateTime state_dt_1 = mailer.getPingDt("to");
        System.out.println("state_dt: " + state_dt_1);
    }


    @Test
    public void test_Send() throws Exception {
        // ---
        System.out.println("getSrvSate.from: " + mailer.getSrvSate("from"));


        // ---
        UtRepl utr = new UtRepl(db);

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r);
        } finally {
            r.close();
        }

        // Увеличиваем возраст
        long age = utr.incAuditAge();
        System.out.println("new AuditAge = " + age);

        // Забираем установочную реплику
        IReplica replica = utr.createReplicaSnapshot(1, publication, age);


        // ---
        mailer.send(replica, age, "from");


        // ---
        System.out.println("new getSrvSate.from: " + mailer.getSrvSate("from"));
    }


    @Test
    public void test_delete() throws Exception {
        // ---
        long no = mailer.getSrvSate("from");
        System.out.println("getSrvSend: " + no);


        // ---
        mailer.delete(no, "from");


        // ---
        System.out.println("new getSrvSend.from: " + mailer.getSrvSate("from"));
    }


    @Test
    public void test_info() throws Exception {
        // ---
        long no = mailer.getSrvSate("from");
        System.out.println("getSrvSend: " + no);


        // ---
        JdxReplInfo info = mailer.getInfo(no, "from");


        // ---
        System.out.println("info: " + info);
    }


}
