package jdtx.repl.main.api;

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

        Reader r = new FileReader("test/etalon/mail_http_ws2.json");
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
        System.out.println("getSrvSend.from: " + mailer.getSrvSend("from"));
        System.out.println("getSrvReceive.to: " + mailer.getSrvReceive("to"));
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
    public void test_Send() throws Exception {
        // ---
        System.out.println("getSrvSend: " + mailer.getSrvSend("from"));


        // ---
        UtRepl utr = new UtRepl(db);

        // Загружаем правила публикации
        IPublication publcation = new Publication();
        Reader r = new FileReader("test/etalon/pub_full.json");
        try {
            publcation.loadRules(r);
        } finally {
            r.close();
        }

        // Увеличиваем возраст
        long age = utr.incAuditAge();
        System.out.println("new AuditAge = " + age);

        // Забираем установочную реплику
        IReplica replica = utr.createReplicaFull(1, publcation, age);


        // ---
        mailer.send(replica, age, "from");


        // ---
        System.out.println("new getSrvSend: " + mailer.getSrvSend("from"));
    }


}
