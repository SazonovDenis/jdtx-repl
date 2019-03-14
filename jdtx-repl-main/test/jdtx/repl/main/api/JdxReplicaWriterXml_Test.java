package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.web.*;
import org.apache.commons.io.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 *
 */
public class JdxReplicaWriterXml_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_1() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r0 = new FileReader("test/etalon/publication_full.json");
        try {
            publication.loadRules(r0);
        } finally {
            r0.close();
        }

        // Забираем установочную реплику
        UtRepl utr = new UtRepl(db2);
        IReplica replica = utr.createReplicaSnapshot(2, publication, 1);
        //
        File f = new File("../_test-data/ws2/tmp/000000001-src.xml");
        FileUtils.copyFile(replica.getFile(), f);
        //
        //System.out.println(replica.getFile());
        System.out.println("replica: " + f);


        //////////
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/mail_http_ws.json"));

        IJdxMailer mailer = new UtMailerHttp();
        mailer.init(cfgData);


        //////////
        mailer.send(replica, 1, "from");

        //
        IReplica replica_1 = mailer.receive(1, "from");
        //
        System.out.println("mailer.receive: " + replica_1.getFile());
    }

    @Test
    public void test_2() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r0 = new FileReader("test/etalon/publication_full.json");
        try {
            publication.loadRules(r0);
        } finally {
            r0.close();
        }

        // Забираем установочную реплику
        UtRepl utr = new UtRepl(db2);
        IReplica replica = utr.createReplicaSnapshot(2, publication, 1);
        //
        File f = new File("../_test-data/ws2/tmp/000000001-src.xml");
        FileUtils.copyFile(replica.getFile(), f);
        //
        //System.out.println(replica.getFile());
        System.out.println("replica: " + f);
    }


}
