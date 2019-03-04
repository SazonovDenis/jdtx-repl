package jdtx.repl.main.api;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

/**
 *
 */
public class JdxReplicaWriterXml_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_1() throws Exception {
        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r0 = new FileReader("test/etalon/pub_full.json");
        try {
            publication.loadRules(r0);
        } finally {
            r0.close();
        }

        // Забираем установочную реплику
        UtRepl utr = new UtRepl(db);
        IReplica replica = utr.createReplicaFull(2, publication, 1);
        //
        File f = new File("../_test-data/ws2/tmp/000000001-src.xml");
        FileUtils.copyFile(replica.getFile(), f);
        //
        //System.out.println(replica.getFile());
        System.out.println(f);


        //////////
        JSONObject cfgData;
        Reader r1 = new FileReader("test/etalon/mail_http_ws2.json");
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r1);
        } finally {
            r1.close();
        }

        IJdxMailer mailer = new UtMailerHttp();
        mailer.init(cfgData);


        //////////
        mailer.send(replica, 1, "from");

        //
        IReplica replica_1 = mailer.receive(1, "from");
        //
        System.out.println(replica_1.getFile());

/*
        /////////
        OutputStream ost = new FileOutputStream(replica.getFile());
        JdxReplicaWriterXml wr = new JdxReplicaWriterXml(ost);
*/
    }


}
