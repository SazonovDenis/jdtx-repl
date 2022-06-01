package jdtx.repl.main.api.mailer;

import jandcode.app.test.*;
import jandcode.utils.*;
import jandcode.utils.test.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.*;
import org.joda.time.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class MailerHttp_Test extends AppTestCase {


    IMailer mailer;
    String guid = "b5781df573ca6ee6.x-17845f2f56f4d401";


    @Override
    public void setUp() throws Exception {
        super.setUp();

        //long wsId = 2;

        JSONObject cfgData = UtRepl.loadAndValidateJsonFile("test/etalon/mail_http_ws.json");
        String url = (String) cfgData.get("url");

        JSONObject cfgWs = new JSONObject(); // (JSONObject); cfgData.get(String.valueOf(wsId));
        cfgWs.put("url", url);
        cfgWs.put("guid", guid);
        cfgWs.put("localDirTmp", "../_test-data/temp");

        mailer = new MailerHttp();
        mailer.init(cfgWs);
    }

    @Test
    public void test_MailerSendReceive() throws Exception {
        long wsId = 2;

        // Берем реплику
        File fileSnapshot = new File("../_test-data/_test-data_srv/srv/que_common/000000/000000001.zip");
        IReplica replicaSnapshot = new ReplicaFile();
        replicaSnapshot.setData(fileSnapshot);


        // --------------------------------
        // Проверяем mailer.send

        // Готовим mailer
        JSONObject wsCfgData = UtRepl.loadAndValidateJsonFile("test/etalon/mail_http_ws.json");
        String url = (String) wsCfgData.get("url");
        String guid = "b5781df573ca6ee6.x-21ba238dfc945002";
        JSONObject cfgWs = (JSONObject) wsCfgData.get(String.valueOf(wsId));
        cfgWs.put("guid", guid);
        cfgWs.put("url", url);
        //
        IMailer mailer = new MailerHttp();
        mailer.init(cfgWs);


        // Отправляем реплику
        File fileReplica = new File(replicaSnapshot.getData().getAbsolutePath());
        IReplica replica = new ReplicaFile();
        replica.setData(fileReplica);
        JdxReplicaReaderXml.readReplicaInfo(replica);
        mailer.send(replica, "from", 1);


        // --------------------------------
        // Проверяем mailer.receive

        // Скачиваем реплику
        IReplica replica2 = mailer.receive("from", 1);

        // Копируем ее для анализа
        File f2 = new File("../_test-data/ws_002/tmp/000000001-receive.zip");
        FileUtils.copyFile(replica2.getData(), f2);
        System.out.println("mailer.receive: " + f2);

        // Информацмия о реплике с почтового сервера
        IReplicaInfo info = mailer.getReplicaInfo("from", 1);
        System.out.println("receive.replica.md5: " + UtJdx.getMd5File(replica2.getData()));
        System.out.println("mailer.info.crc:     " + info.getCrc());
    }

    @Test
    public void test_MailerSendCrc() throws Exception {
        long wsId = 2;

        // Готовим реплику
        IReplica replica1 = new ReplicaFile();
        File fileSnapshot = new File("../_test-data/_test-data_srv/srv/queOut001_ws_001/000000001.zip");
        replica1.setData(fileSnapshot);


        // --------------------------------
        // Проверяем mailer.send

        // Готовим mailer
        JSONObject wsCfgData = UtRepl.loadAndValidateJsonFile("test/etalon/mail_http_ws.json");
        String url = (String) wsCfgData.get("url");
        String guid = "b5781df573ca6ee6.x-21ba238dfc945002";
        JSONObject cfgMailer = (JSONObject) wsCfgData.get(String.valueOf(wsId));
        String mailLocalDirTmp = "../_test-data/_test-data_srv/srv/ws_001_tmp/";
        cfgMailer.put("localDirTmp", mailLocalDirTmp);
        cfgMailer.put("guid", guid);
        cfgMailer.put("url", url);
        //
        IMailer mailer = new MailerHttp();
        mailer.init(cfgMailer);


        // Отправляем реплику
        JdxReplicaReaderXml.readReplicaInfo(replica1);
        mailer.send(replica1, "from", 1);


        // --------------------------------
        // Проверяем mailer.receive

        // Скачиваем реплику
        IReplica replica2 = mailer.receive("from", 1);

        // Копируем ее для анализа
        File f2 = new File("../_test-data/ws_002/tmp/000000001-receive.zip");
        FileUtils.copyFile(replica2.getData(), f2);
        System.out.println("mailer.receive: " + f2);

        // Информацмия о реплике с почтового сервера
        IReplicaInfo info = mailer.getReplicaInfo("from", 1);
        System.out.println("receive.replica.md5: " + UtJdx.getMd5File(replica2.getData()));
        System.out.println("mailer.info.crc:     " + info.getCrc());
    }

    @Test
    public void test_MailerSendReceive_1() throws Exception {
        logOn();

        long wsId = 2;


        // --------------------------------
        // Проверяем mailer.send

        // Готовим mailer
        JSONObject wsCfgData = UtRepl.loadAndValidateJsonFile("test/etalon/mail_http_ws.json");
        String url = (String) wsCfgData.get("url");
        //String guid = "b5781df573ca6ee6.x-21ba238dfc945002";
        url = "http://jadatex.ru/repl/";
        String guid = "0000000000000000.test-0100000000000001";
        JSONObject cfgWs = (JSONObject) wsCfgData.get(String.valueOf(wsId));
        cfgWs.put("guid", guid);
        cfgWs.put("url", url);
        cfgWs.put("localDirTmp", "../_test-data/temp/");
        //
        IMailer mailer = new MailerHttp();
        mailer.init(cfgWs);


        // Отправляем реплику
        File fileReplica = new File("test/etalon/000000067.zip");
        IReplica replica1 = new ReplicaFile();
        replica1.setData(fileReplica);
        JdxReplicaReaderXml.readReplicaInfo(replica1);
        mailer.send(replica1, "from", 100);


        // --------------------------------
        // Проверяем mailer.receive

        // Скачиваем реплику
        IReplica replica2 = mailer.receive("from", 100);

        // Копируем ее для анализа
        File f2 = new File("../_test-data/000000067-receive.zip");
        FileUtils.copyFile(replica2.getData(), f2);
        System.out.println("mailer.receive: " + f2);

        // Информацмия о реплике с почтового сервера
        IReplicaInfo info = mailer.getReplicaInfo("from", 100);
        System.out.println("send.replica.md5:    " + UtJdx.getMd5File(fileReplica));
        System.out.println("receive.replica.md5: " + UtJdx.getMd5File(replica2.getData()));
        System.out.println("mailer.info.crc:     " + info.getCrc());
    }


    @Test
    public void test_send() throws Exception {
        logOn();

        // ---
        // Файл - наполнение реплики - из мусора - ее содержимое неважно, проверяем только транспорт
        //File srcFile = new File("../_test-data/_test-data_srv/srv/queCommon/000000001.zip");
        //File srcFile = new File("D:/Install/apache-tomcat-6.0.44.zip");  // 7.2 Mb
        //File srcFile = new File("D:/Install/tortoisehg-4.1.0-x64.msi");   // 26 Mb
        File srcFile = new File("D:/Install/jdk-1.8.0_202.zip");   // 179 Mb

        assertEquals("Исходный файл не существует", srcFile.exists(), true);
        //
        UtFile.cleanDir("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/" + guid.replace("-", "/") + "/from");
        File destBoxFile = new File("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/" + guid.replace("-", "/") + "/from/000000999.000");
        File infoBoxFile = new File("../../lombard.systems/repl/" + MailerHttp.REPL_PROTOCOL_VERSION + "/" + guid.replace("-", "/") + "/from/last.info");
        assertEquals("Конечный ящик не удалось очистить", destBoxFile.exists() || infoBoxFile.exists(), false);
        //
        File destReplicaFile = new File(((MailerHttp) mailer).localDirTmp + "~000000999.zip");
        destReplicaFile.delete();
        assertEquals("Конечный файл реплики не удалось удалить", destReplicaFile.exists(), false);

        // ---
        System.out.println("getBoxState.from: " + mailer.getBoxState("from"));

        // ---
        // Создаем реплику (из мусора - ее содержимое неважно)
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setDbStructCrc("00000000000000000000");
        replica.getInfo().setWsId(1);
        replica.getInfo().setAge(999);
        replica.setData(srcFile);


        // ---
        // Отправляем реплику
        mailer.send(replica, "from", 999);


        // ---
        // Проверяем
        System.out.println("new getBoxState.from: " + mailer.getBoxState("from"));

        //
        //assertEquals("Файл не скопировался", destBoxFile.exists(), true);
        //assertEquals("Размер исходного и конечного файла не совпадает", srcFile.length(), destBoxFile.length());
        //assertEquals("Контрольная сумма исходного и конечного файла не совпадает", JdxUtils.getMd5File(srcFile), JdxUtils.getMd5File(destBoxFile));


        // ---
        // Получаем реплику
        IReplica replicaReceive = mailer.receive("from", 999);


        //
        assertEquals("Размер исходного и конечного файла не совпадает", srcFile.length(), replicaReceive.getData().length());
        assertEquals("Контрольная сумма исходного и конечного файла не совпадает", UtJdx.getMd5File(srcFile), UtJdx.getMd5File(replicaReceive.getData()));
    }


    @Test
    public void test_receive() throws Exception {
        IReplica replica_1 = mailer.receive("from", 999);
        System.out.println("receive: " + replica_1.getData());
    }


    @Test
    public void test_part_state() throws Exception {
        String box = "from";

        // =====================
        long no = 1;

        //
        JSONObject resInfo = ((MailerHttp) mailer).getInfo_internal(box, no);
        JSONObject part_info = (JSONObject) resInfo.get("part_info");

        //
        System.out.println("resInfo: " + resInfo);

        //
        long total_bytes = (long) part_info.get("total_bytes");
        long part_max_no = (long) part_info.get("part_max_no");

        //
        System.out.println("total_bytes: " + total_bytes + ", part_max_no: " + part_max_no);


        // =====================
        no = 9991;

        //
        resInfo = ((MailerHttp) mailer).getInfo_internal(box, no);
        part_info = (JSONObject) resInfo.get("part_info");

        //
        System.out.println("resInfo: " + resInfo);

        //
        total_bytes = (long) part_info.get("total_bytes");
        part_max_no = (long) part_info.get("part_max_no");

        //
        System.out.println("total_bytes: " + total_bytes + ", part_max_no: " + part_max_no);
    }


    @Test
    public void test_http() throws Exception {
        StopWatch sw = new StopWatch();
        sw.start();

        //
        HttpGet httpGet = new HttpGet("http://lombard.systems/repl/repl_part_receive.php?seed=4751547061763885136&guid=b5781df573ca6ee6.x-17845f2f56f4d401&box=from&no=1&part=0");

        //
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(httpGet);

        //
        byte[] res = EntityUtils.toByteArray(response.getEntity());
        //
        FileOutputStream outputStream = new FileOutputStream("../_test-data/xxx.dat");
        outputStream.write(res);
        outputStream.close();

        //
        sw.stop();
    }


    @Test
    public void test_delete() throws Exception {
        // ---
        long no = mailer.getBoxState("from");
        System.out.println("getSrvSend: " + no);


        // ---
        mailer.delete("from", no);


        // ---
        System.out.println("new getSrvSend.from: " + mailer.getBoxState("from"));
    }


    @Test
    public void test_delete_999() throws Exception {
        long no = 999;
        System.out.println("getSrvSend: " + no);

        //
        mailer.delete("from", no);
    }


    @Test
    public void test_getState() throws Exception {
        String box = "to";
        System.out.println("box: " + box);


        //
        printState(box);
        System.out.println();

        //
        mailer.setData(null, "ping.write", box);
        mailer.setData(null, "ping.read", box);

        //
        printState(box);
        System.out.println();


        //
        box = "from";
        System.out.println("box: " + box);


        //
        printState(box);
        System.out.println();

        //
        mailer.setData(null, "ping.write", box);
        mailer.setData(null, "ping.read", box);

        //
        printState(box);
    }

    private void printState(String box) throws Exception {
        JSONObject res_files = mailer.getData("files", box);
        JSONObject res_ping_read = mailer.getData("ping.read", box);
        JSONObject res_ping_write = mailer.getData("ping.write", box);
        JSONObject res_last_info = mailer.getData("last.dat.info", box);
        JSONObject res_required_info = mailer.getData("required.info", box);
        //
        System.out.println("files: " + res_files);
        System.out.println("ping_read: " + res_ping_read);
        System.out.println("ping_write: " + res_ping_write);
        System.out.println("last_info: " + res_last_info);
        System.out.println("required_info: " + res_required_info);
    }


    @Test
    public void test_info() throws Exception {
        // ---
        long no = mailer.getBoxState("from");
        System.out.println("getSrvSend: " + no);


        // ---
        IReplicaInfo info = mailer.getReplicaInfo("from", no);


        // ---
        System.out.println("info: " + info);
    }


    @Test
    public void test_ReceiveDone() throws Exception {
        long receiveDone_from = mailer.getReceiveDone("from");
        long receiveDone_to001 = mailer.getReceiveDone("to001");
        long receiveDone_to = mailer.getReceiveDone("to");
        System.out.println("receive from: " + receiveDone_from);
        System.out.println("receive to001: " + receiveDone_to001);
        System.out.println("receive to: " + receiveDone_to);
        System.out.println();
        long sendDone_from = mailer.getSendDone("from");
        long sendDone_to001 = mailer.getSendDone("to001");
        long sendDone_to = mailer.getSendDone("to");
        System.out.println("send from: " + sendDone_from);
        System.out.println("send to001: " + sendDone_to001);
        System.out.println("send to: " + sendDone_to);
    }

    @Test
    public void test_required() throws Exception {
        RequiredInfo requiredInfo0 = mailer.getSendRequired("from");
        System.out.println("requiredFrom: " + requiredInfo0.requiredFrom);
        System.out.println("requiredTo: " + requiredInfo0.requiredTo);
        System.out.println("recreate: " + requiredInfo0.recreate);
        System.out.println();

        // ---
        RequiredInfo requiredInfo00 = new RequiredInfo();
        requiredInfo00.requiredFrom = 2;
        requiredInfo00.requiredTo = 4;
        requiredInfo00.recreate = true;
        requiredInfo00.executor = RequiredInfo.EXECUTOR_SRV;
        mailer.setSendRequired("from", requiredInfo00);
        //
        RequiredInfo requiredInfo1 = mailer.getSendRequired("from");
        System.out.println("requiredFrom: " + requiredInfo1.requiredFrom);
        System.out.println("requiredTo: " + requiredInfo1.requiredTo);
        System.out.println("recreate: " + requiredInfo1.recreate);
        System.out.println("executor: " + requiredInfo1.executor);
        System.out.println();
        //
        assertEquals(2, requiredInfo1.requiredFrom);
        assertEquals(4, requiredInfo1.requiredTo);
        assertEquals(true, requiredInfo1.recreate);
        assertEquals(RequiredInfo.EXECUTOR_SRV, requiredInfo1.executor);


        // ---
        RequiredInfo requiredInfo11 = new RequiredInfo();
        mailer.setSendRequired("from", requiredInfo11);
        //
        RequiredInfo requiredInfo2 = mailer.getSendRequired("from");
        System.out.println("requiredFrom: " + requiredInfo2.requiredFrom);
        System.out.println("requiredTo: " + requiredInfo2.requiredTo);
        System.out.println("recreate: " + requiredInfo2.recreate);
        System.out.println("executor: " + requiredInfo2.executor);
        //
        assertEquals(-1, requiredInfo2.requiredFrom);
        assertEquals(-1, requiredInfo2.requiredTo);
        assertEquals(false, requiredInfo2.recreate);
        assertEquals(null, requiredInfo2.executor);
    }


    @Test
    public void test_getSrvState() throws Exception {
        long no_from = mailer.getBoxState("from");
        long no_to = mailer.getBoxState("to");

        System.out.println("sate.from: " + no_from);
        System.out.println("sate.to: " + no_to);
    }

    @Test
    public void test_getData() throws Exception {
        String name = "last.dat.info";
        String box = "to";
        //
        JSONObject lastInfo = mailer.getData(name, box);
        //
        System.out.println("res: " + lastInfo);
        JSONObject file_info = (JSONObject) lastInfo.get("data");
        System.out.println("data: " + file_info);
        System.out.println("crc: " + file_info.getOrDefault("crc", ""));
    }

    @Test
    public void test_getLastReplicaInfo() throws Exception {
        String box = "to";
        //
        System.out.println();
        System.out.println("mailer.guid: " + ((MailerHttp) mailer).guid);
        System.out.println("mailer.remoteUrl: " + ((MailerHttp) mailer).remoteUrl);
        //
        IReplicaInfo replicaInfo = ((MailerHttp) mailer).getLastReplicaInfo(box);
        //
        System.out.println();
        System.out.println("replicaSrv:");
        System.out.println("lastInfo: " + replicaInfo);
        System.out.println("Crc: " + replicaInfo.getCrc());
        System.out.println("DbStructCrc: " + replicaInfo.getDbStructCrc());
        System.out.println("Age: " + replicaInfo.getAge());
        System.out.println("WsId: " + replicaInfo.getWsId());
        System.out.println("DtFrom: " + replicaInfo.getDtFrom());
        System.out.println("DtTo: " + replicaInfo.getDtTo());

        //
        // Читаем С СЕРВЕРА информацию о реплике, которую последней отправили на сервер
        String crcSrv = replicaInfo.getCrc();

        // Берем ИЗ СВОЕЙ очереди ту реплику, которую последней отправили на сервер
        String fileName = "../_test-data/_test-data_srv/srv/que_in_ws_001/000000/000000001.zip";
        File fileReplica = new File(fileName);
        IReplica replicaWs = new ReplicaFile();
        replicaWs.setData(fileReplica);
        JdxReplicaReaderXml.readReplicaInfo(replicaWs);

        // Вычисляем crc файла данных
        String crcFile = UtJdx.getMd5File(replicaWs.getData());
        replicaWs.getInfo().setCrc(crcFile);

        //
        System.out.println();
        System.out.println("replicaWs:");
        System.out.println("lastInfo: " + replicaWs.getInfo());
        System.out.println("Crc: " + replicaWs.getInfo().getCrc());
        System.out.println("DbStructCrc: " + replicaWs.getInfo().getDbStructCrc());
        System.out.println("Age: " + replicaWs.getInfo().getAge());
        System.out.println("WsId: " + replicaWs.getInfo().getWsId());
        System.out.println("DtFrom: " + replicaWs.getInfo().getDtFrom());
        System.out.println("DtTo: " + replicaWs.getInfo().getDtTo());

        // Сравниваем CRC реплик
        System.out.println();
        if (UtJdx.equalReplicaCrc(replicaWs, crcSrv)) {
            System.out.println("Equal ReplicaCrc");
        } else {
            System.out.println("Not equal ReplicaCrc");
        }
    }

    @Test
    public void test_setData() throws Exception {
        Map data = new HashMap();
        data.put("dt_now", new DateTime());

        //
        String name = "test-1.xxx";
        String box = null;
        mailer.setData(data, name, box);
        System.out.println("res: " + mailer.getData(name, box));

        //
        name = "test-2.xxx";
        box = "from";
        mailer.setData(data, name, box);
        System.out.println("res: " + mailer.getData(name, box));

        //
        name = "test-3.xxx";
        data = null;
        box = null;
        mailer.setData(data, name, box);
        System.out.println("res: " + mailer.getData(name, box));

        //
        name = "test-4.xxx";
        data = null;
        box = "from";
        mailer.setData(data, name, box);
        System.out.println("res: " + mailer.getData(name, box));
    }

    @Test
    public void test_setData_Complex() throws Exception {
        JSONObject res1 = mailer.getData("test.xxx", null);
        System.out.println("res: " + res1);

        JSONObject res2 = mailer.getData("test.xxx", "from");
        System.out.println("res: " + res2);

        //
        System.out.println("===");

        //
        Map data = new HashMap();
        data.put("key", "value");
        data.put("key1", "value1");
        Map values = new HashMap();
        values.put("key11", "value11");
        values.put("key22", "value22");
        values.put("key33", "value33");
        data.put("values", values);
        List list = new ArrayList();
        list.add(12);
        list.add(23);
        list.add(34);
        list.add(45);
        Map values_list = new HashMap();
        values_list.put("key1", "value1");
        values_list.put("key2", "value2");
        list.add(values_list);
        data.put("list", list);

        //
        mailer.setData(data, "test.xxx", "from");

        //
        res1 = mailer.getData("test.xxx", null);
        System.out.println("res: " + res1);

        res2 = mailer.getData("test.xxx", "from");
        System.out.println("res: " + res2);
    }


}
