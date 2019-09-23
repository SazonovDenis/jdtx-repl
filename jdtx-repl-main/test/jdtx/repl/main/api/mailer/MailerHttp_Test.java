package jdtx.repl.main.api.mailer;

import jandcode.app.test.*;
import jandcode.utils.*;
import jandcode.utils.test.*;
import jandcode.web.*;
import jdtx.repl.main.api.replica.*;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;

/**
 */
public class MailerHttp_Test extends AppTestCase {


    JSONObject cfgData;
    IMailer mailer;
    String guid = "b5781df573ca6ee6.x-21ba238dfc945002";


    @Override
    public void setUp() throws Exception {
        super.setUp();

        long wsId = 2;

        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString("test/etalon/mail_http_ws.json"));
        String url = (String) cfgData.get("url");

        JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgWs.put("url", url);
        cfgWs.put("guid", guid);
        cfgWs.put("localDirTmp", "../_test-data/temp");

        mailer = new MailerHttp();
        mailer.init(cfgWs);
    }


    @Test
    public void test_send() throws Exception {
        // ---
        System.out.println("getSrvState.from: " + mailer.getSrvState("from"));

        // ---
        File srcFile = new File("../_test-data/srv/queCommon/000000001.zip");
        assertEquals("Исходный файл не существует", srcFile.exists(), true);
        //
        File destFile = new File("../../lombard.systems/repl/02/" + guid.replace("-", "/") + "/from/000000999.000");
        destFile.delete();
        assertEquals("Конечный файл не удалось удалить", destFile.exists(), false);
        //
        File infoFile = new File("../../lombard.systems/repl/02/" + guid.replace("-", "/") + "/from/last.info");
        infoFile.delete();
        assertEquals("Служебный файл 'last.info' не удалось удалить", infoFile.exists(), false);
        //
/*
        File datFile = new File("../../lombard.systems/repl/02/" + guid + "/from/last.info");
        infoFile.delete();
        assertEquals("Служебный файл 'last.info' не удалось удалить", infoFile.exists(), false);
*/

        // ---
        // Создаем реплику (из мусора - ее содержимое неважно)
        IReplica replica = new ReplicaFile();
        replica.getInfo().setDbStructCrc("00000000000000000000");
        replica.getInfo().setWsId(1);
        replica.getInfo().setAge(999);
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.setFile(srcFile);


        // ---
        // Отправляем реплику
        mailer.send(replica, "from", 999);


        // ---
        // Проверяем
        System.out.println("new getSrvState.from: " + mailer.getSrvState("from"));

        //
        assertEquals("Файл не скопировался", destFile.exists(), true);
        assertEquals("Размер исходного и конечного файла не совпадает", destFile.length(), srcFile.length());
    }


    @Test
    public void test_receive() throws Exception {
        IReplica replica_1 = mailer.receive("from", 999);
        System.out.println("receive: " + replica_1.getFile());
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
        HttpGet httpGet = new HttpGet("http://lombard.systems/repl/repl_part_receive.php?seed=4751547061763885136&guid=98178b66d083dd79.jovid-0324d9edabc5b860&box=from&no=1&filePart=0");

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
        long no = mailer.getSrvState("from");
        System.out.println("getSrvSend: " + no);


        // ---
        mailer.delete("from", no);


        // ---
        System.out.println("new getSrvSend.from: " + mailer.getSrvState("from"));
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
        JSONObject res0 = ((MailerHttp) mailer).getState_internal("to");
        System.out.println("ping_read: " + res0.get("ping_read"));
        System.out.println("ping_write: " + res0.get("ping_write"));

        //
        mailer.pingWrite("to");
        mailer.pingRead("to");

        //
        JSONObject res1 = ((MailerHttp) mailer).getState_internal("to");
        System.out.println("ping_read: " + res1.get("ping_read"));
        System.out.println("ping_write: " + res1.get("ping_write"));
    }


    @Test
    public void test_info() throws Exception {
        // ---
        long no = mailer.getSrvState("from");
        System.out.println("getSrvSend: " + no);


        // ---
        ReplicaInfo info = mailer.getReplicaInfo("from", no);


        // ---
        System.out.println("info: " + info);
    }


    @Test
    public void test_required() throws Exception {
        long required0 = mailer.getSendRequired("from");
        System.out.println("required: " + required0);

        // ---
        mailer.setSendRequired("from", required0 + 2);


        // ---
        long required1 = mailer.getSendRequired("from");
        System.out.println("required: " + required1);

        // ---
        mailer.setSendRequired("from", -1);


        // ---
        long required2 = mailer.getSendRequired("from");
        System.out.println("required: " + required2);
    }


    @Test
    public void test_getSrvState() throws Exception {
        long no_from = mailer.getSrvState("from");
        long no_to = mailer.getSrvState("to");

        System.out.println("sate.from: " + no_from);
        System.out.println("sate.to: " + no_to);
    }


}
