package jdtx.repl.main.api.replica;

import jandcode.utils.*;
import jandcode.utils.test.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.publication.*;
import org.apache.commons.io.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.zip.*;

/**
 *
 */
public class JdxReplicaWriterXml_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_getFull() throws Exception {
        long wsId = 2;

        // Загружаем правила публикации
        IPublication publication = new Publication();
        Reader r0 = new FileReader("test/etalon/publication_full.json");
        try {
            publication.loadRules(r0, struct2);
        } finally {
            r0.close();
        }


        // Забираем установочную реплику
        UtRepl utRepl = new UtRepl(db2, struct2);
        IReplica replica = utRepl.createReplicaTableSnapshot(wsId, publication.getData().getTable("ulz"), 1);


        // Копируем реплику для анализа
        File f = new File("../_test-data/ws_002/tmp/000000001-src.zip");
        FileUtils.copyFile(replica.getFile(), f);
        System.out.println("replica: " + f);
        System.out.println("replica.wsId = " + replica.getInfo().getWsId());
        System.out.println("replica.age = " + replica.getInfo().getAge());
        System.out.println("replica.replicaType = " + replica.getInfo().getReplicaType());
    }


    @Test
    public void test_getFull_header() throws Exception {
        // Готовим установочную реплику
        test_getFull();
        System.out.println("==========================");

        // Проверяем чтение заголовков
        File f = new File("../_test-data/ws_002/tmp/000000001-src.zip");
        IReplica replica = new ReplicaFile();
        replica.setFile(f);

        //
        JdxReplicaReaderXml.readReplicaInfo(replica);
        System.out.println("replica: " + f);
        System.out.println("replica.wsId = " + replica.getInfo().getWsId());
        System.out.println("replica.age = " + replica.getInfo().getAge());
        System.out.println("replica.replicaType = " + replica.getInfo().getReplicaType());
    }

    @Test
    public void test_getFull_send() throws Exception {
        long wsId = 2;

        // Готовим установочную реплику
        //test_getFull();
        //System.out.println("==========================");


        // Готовим mailer
        String guid = "b5781df573ca6ee6.x-21ba238dfc945002";

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/mail_http_ws.json"));
        String url = (String) cfgData.get("url");

        JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgWs.put("guid", guid);
        cfgWs.put("url", url);

        IMailer mailer = new MailerHttp();
        mailer.init(cfgWs);


        // Проверяем mailer.send
        File f = new File("../_test-data/ws_002/tmp/000000001-src.zip");
        IReplica replica = new ReplicaFile();
        replica.setFile(f);
        JdxReplicaReaderXml.readReplicaInfo(replica);
        mailer.send(replica, "from", 1);


        // Проверяем mailer.receive

        // Скачиваем реплику
        IReplica replica2 = mailer.receive("from", 1);
        // Копируем ее для анализа
        File f2 = new File("../_test-data/ws_002/tmp/000000001-receive.zip");
        FileUtils.copyFile(replica2.getFile(), f2);
        System.out.println("mailer.receive: " + f2);

        // Информацмия о реплике с почтового сервера
        ReplicaInfo info = mailer.getReplicaInfo("from", 1);
        System.out.println("receive.replica.md5: " + JdxUtils.getMd5File(replica2.getFile()));
        System.out.println("mailer.info.crc:     " + info.getCrc());
    }

}
