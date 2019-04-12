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
            publication.loadRules(r0);
        } finally {
            r0.close();
        }


        // Забираем установочную реплику
        UtRepl utRepl = new UtRepl(db2, struct2);
        IReplica replica = utRepl.createReplicaSnapshot(wsId, publication, 1);


        // Копируем ее для анализа
        File f = new File("../_test-data/ws_002/tmp/000000001-src.zip");
        FileUtils.copyFile(replica.getFile(), f);
        System.out.println("replica: " + f);
        System.out.println("replica.wsId = " + replica.getWsId());
        System.out.println("replica.age = " + replica.getAge());
        System.out.println("replica.replicaType = " + replica.getReplicaType());
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
        System.out.println("replica.wsId = " + replica.getWsId());
        System.out.println("replica.age = " + replica.getAge());
        System.out.println("replica.replicaType = " + replica.getReplicaType());
    }

    @Test
    public void test_getFull_send() throws Exception {
        long wsId = 2;

        // Готовим установочную реплику
        //test_getFull();
        //System.out.println("==========================");


        // Готовим mailer
        String guid = "b5781df573ca6ee6-21ba238dfc945002";

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
        mailer.send(replica, 1, "from");


        // Проверяем mailer.receive

        // Скачиваем реплику
        IReplica replica2 = mailer.receive(1, "from");
        // Копируем ее для анализа
        File f2 = new File("../_test-data/ws_002/tmp/000000001-receive.zip");
        FileUtils.copyFile(replica2.getFile(), f2);
        System.out.println("mailer.receive: " + f2);

        // Информацмия о реплике с почтового сервера
        ReplicaInfo info = mailer.getInfo(1, "from");
        System.out.println("receive.replica.md5: " + JdxUtils.getMd5File(replica2.getFile()));
        System.out.println("mailer.info.crc:     " + info.crc);
    }

    @Test
    public void test_putToZip() throws Exception {
        File[] fIn = {
                new File("../_test-data/idea.2018.02.07.zip"),
                new File("../_test-data/jdk-1.8.0_202.zip")
        };
        File fZip = new File("../_test-data/test.zip");
        //
        zip(fIn, fZip);
        //
        System.out.println(fZip);
    }

    @Test
    public void test_zipReadHead() throws Exception {
        File fZip = new File("../_test-data/test.zip");
        //
        printZipContent(fZip);
    }

    void zip(File[] fInArr, File fZip) throws IOException {
        int buffSize = 1024 * 10;
        byte[] buffer = new byte[buffSize];

        String outFileName = fZip.getPath();

        try (
                FileOutputStream outputStream = new FileOutputStream(outFileName);
                ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
        ) {
            for (File fIn : fInArr) {

                StopWatch sw = new StopWatch();
                sw.start();

                String inFilename = fIn.getPath();
                FileInputStream inputStream = new FileInputStream(inFilename);
                System.out.println(inFilename);

                // ---
                ZipEntry zipEntry_head = new ZipEntry(fIn.getName() + ".json");
                zipOutputStream.putNextEntry(zipEntry_head);
                String head = "{size: \"" + fIn.length() + "\"}";
                zipOutputStream.write(head.getBytes());
                //
                zipOutputStream.closeEntry();


                // ---
                ZipEntry zipEntry = new ZipEntry(fIn.getName());
                zipOutputStream.putNextEntry(zipEntry);

                //
                long done = 0;
                long done_printed = 0;
                while (inputStream.available() > 0) {
                    int n = inputStream.read(buffer);
                    zipOutputStream.write(buffer, 0, n);
                    done = done + n;
                    //
                    if (done - done_printed > 1024 * 1024 * 10) {
                        done_printed = done;
                        System.out.print("\r" + done / 1024 + " Kb");
                    }
                }

                // закрываем текущую запись для новой записи
                zipOutputStream.closeEntry();

                //
                System.out.println("\r" + done / 1024 + " Kb done");
                sw.stop();

            }
        }
    }


    void printZipContent(File fZip) throws Exception {
        int buffSize = 1024 * 10;
        byte[] buffer = new byte[buffSize];


        try (
                ZipInputStream inputStream = new ZipInputStream(new FileInputStream(fZip))
        ) {
            System.out.println(fZip);

            ZipEntry entry;
            while ((entry = inputStream.getNextEntry()) != null) {
                StopWatch sw = new StopWatch();
                sw.start();

                //
                String name = entry.getName(); // получим название файла
                long size = entry.getSize();  // получим его размер в байтах
                System.out.printf("%s, size: %d\n", name, size);

                if (name.endsWith(".json")) {
                    // распаковка
                    String outFileName = fZip.getParent() + "/" + name;
                    FileOutputStream outputStream = new FileOutputStream(outFileName);
                    //
                    for (int n = inputStream.read(buffer); n > 0; n = inputStream.read(buffer)) {
                        outputStream.write(buffer, 0, n);
                    }
                    //
                    outputStream.close();

                    if (name.endsWith(".json")) {
                        String s = UtFile.loadString(outFileName);
                        System.out.println(s);
                    }
                }


                //
                inputStream.closeEntry();

                //
                sw.stop();
                System.out.println("");
            }
        }
    }


}
