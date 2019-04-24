package jdtx.repl.main.api.mailer;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.entity.*;
import org.apache.http.entity.mime.*;
import org.apache.http.entity.mime.content.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.*;
import org.joda.time.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 */
public class MailerHttp implements IMailer {


    String remoteUrl;
    public String guid;
    String localDirTmp;
    Random rnd;

    protected static Log log = LogFactory.getLog("jdtx");

    // 32 Mb
    int HTTP_FILE_MAX_SIZE = 1024 * 1024 * 32;
    String REPL_PROTOCOL_VERSION = "2.0";
    //int HTTP_FILE_MAX_SIZE = 512;


    @Override
    public void init(JSONObject cfg) {
        remoteUrl = (String) cfg.get("url");
        guid = (String) cfg.get("guid");
        localDirTmp = (String) cfg.get("mailLocalDirTmp");
        //
        if (remoteUrl == null || remoteUrl.length() == 0) {
            throw new XError("Invalid remoteUrl");
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Invalid guid");
        }
        if (localDirTmp == null || localDirTmp.length() == 0) {
            throw new XError("Invalid localDirTmp");
        }
        //
        localDirTmp = UtFile.unnormPath(localDirTmp) + "/";
        //
        UtFile.mkdirs(localDirTmp);
        //
        rnd = new Random();
        rnd.setSeed(new DateTime().getMillis());
    }


    @Override
    public long getSrvSate(String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_state") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);
        //
        JSONObject res = parseResult(response);

        //
        JSONObject result = (JSONObject) res.get("result");
        return Long.valueOf(String.valueOf(result.get("max")));
    }

    @Override
    public void send(IReplica replica, long no, String box) throws Exception {
        log.info("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);


        // Проверки: правильность типа реплики
        JdxUtils.validateReplica(replica);


        // Закачиваем
        HttpClient client = HttpClientBuilder.create().build();

        // Закачиваем по частям
        long filePart = 0;
        long sentBytes = 0;
        long totalBytes = replica.getFile().length();


        // Большие письма отправляем с докачкой, для чего сначала выясняем, что уже успели закачать
        if (totalBytes > HTTP_FILE_MAX_SIZE) {
            JSONObject res = getPartState_internal(no, box);
            sentBytes = (long) res.get("total_bytes");
            filePart = (long) res.get("part_max_no");
            //
            if (sentBytes > 0) {
                log.info("mailer.send, already sent part: " + filePart + ", sent bytes: " + sentBytes + "/" + totalBytes);
            }
            //
            filePart = filePart + 1;
        }


        // Часть за частью
        while (sentBytes < totalBytes) {
            //
            HttpPost post = new HttpPost(remoteUrl + "repl_send_part.php");

            //
            StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_no = new StringBody(String.valueOf(no), ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_part = new StringBody(String.valueOf(filePart), ContentType.MULTIPART_FORM_DATA);
            byte[] buff = readFilePart(replica.getFile(), sentBytes, HTTP_FILE_MAX_SIZE);
            ByteArrayBody byteBody = new ByteArrayBody(buff, "file");

            //
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            builder.addPart("guid", stringBody_guid);
            builder.addPart("box", stringBody_box);
            builder.addPart("no", stringBody_no);
            builder.addPart("file_part", stringBody_part);
            builder.addPart("file", byteBody);
            HttpEntity entity = builder.build();
            //
            post.setEntity(entity);

            //
            HttpResponse response = client.execute(post);

            //
            handleErrors(response);
            //
            parseResult(response);

            //
            filePart = filePart + 1;
            sentBytes = sentBytes + buff.length;

            //
            log.info("mailer.send, part: " + filePart + ", sentBytes: " + sentBytes + "/" + totalBytes);
        }


        // Завершение закачки
        ReplicaInfo info = new ReplicaInfo();
        info.setWsId(replica.getInfo().getWsId());
        info.setAge(replica.getInfo().getAge());
        info.setDtFrom(replica.getInfo().getDtFrom());
        info.setDtTo(replica.getInfo().getDtTo());
        info.setReplicaType(replica.getInfo().getReplicaType());
        info.setCrc(JdxUtils.getMd5File(replica.getFile()));

        //
        sendCommit_internal(no, box, info, filePart, totalBytes);
    }


    @Override
    public IReplica receive(long no, String box) throws Exception {
        log.info("mailer.receive, no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);

        // Читаем данные об очередном письме
        JSONObject fileInfo = getInfo_internal(no, box);

        // Проверим протокол репликатора
        String protocolVersion = (String) fileInfo.get("protocolVersion");
        if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
            throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
        }

        // Сколько частей надо скачивать
        long filePartsCount = (long) fileInfo.get("partsCount");
        long totalBytes = (long) fileInfo.get("totalBytes");

        // Если частей много - сообщим в log.info
        if (filePartsCount > 1) {
            log.info("mailer.receive, filePartsCount: " + filePartsCount);
        }

        // Замечание: файл replicaFile может уже существует - от предыдущих попыток скачать
        String localFileName = "~" + getFileName(no);
        File replicaFile = new File(localDirTmp + localFileName);

        // Большие письма получаем с докачкой, поэтому сначала выясняем, что уже успели скачать
        long filePart = 0;
        long receivedBytes = 0;
        if (filePartsCount > 1) {
            receivedBytes = replicaFile.length();
            filePart = (receivedBytes + HTTP_FILE_MAX_SIZE - 1) / HTTP_FILE_MAX_SIZE;
            //
            if (receivedBytes > 0) {
                log.info("mailer.receive, already received part: " + filePart + ", receive bytes: " + receivedBytes + "/" + totalBytes);
            }
        }

        // Закачиваем (по частям)
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        while (receivedBytes < totalBytes) {
            //
            HttpGet httpGet = new HttpGet(getUrl("repl_receive_part") + "&guid=" + guid + "&box=" + box + "&no=" + no + "&file_part=" + filePart);

            //
            HttpResponse response = httpclient.execute(httpGet);
            //
            handleErrors(response);

            //
            HttpEntity entity = response.getEntity();
            byte[] buff = EntityUtils.toByteArray(entity);
            //
            FileOutputStream outputStream = new FileOutputStream(replicaFile, true);
            outputStream.write(buff);
            outputStream.close();

            //
            filePart = filePart + 1;
            receivedBytes = receivedBytes + buff.length;

            //
            log.info("mailer.receive, part: " + filePart + "/" + filePartsCount + ", receivedBytes: " + receivedBytes);
        }

        //
        IReplica replica = new ReplicaFile();
        replica.setFile(replicaFile);

        //
        return replica;
    }

    @Override
    public void delete(long no, String box) throws Exception {
        log.info("mailer.delete, no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_delete") + "&guid=" + guid + "&box=" + box + "&no=" + no);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);

        //
        JSONObject res = parseResult(response);
    }


    @Override
    public void ping(String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_ping") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);

        //
        JSONObject res = parseResult(response);
    }


    @Override
    public DateTime getPingDt(String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_ping_dt") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);

        //
        JSONObject res = parseResult(response);

        //
        String state_dt = (String) res.get("state_dt");
        if (state_dt == null || state_dt.length() == 0) {
            return null;
        }
        //
        return new DateTime(state_dt);
    }


    @Override
    public ReplicaInfo getInfo(long no, String box) throws Exception {
        JSONObject res = getInfo_internal(no, box);

        //
        ReplicaInfo info = ReplicaInfo.fromJSONObject(res);

        //
        return info;
    }


    /**
     * Утилиты
     */

    JSONObject getInfo_internal(long no, String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_info") + "&guid=" + guid + "&box=" + box + "&no=" + no);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);
        //
        JSONObject res = parseResult(response);

        //
        return res;
    }

    JSONObject getPartState_internal(long no, String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_part_state") + "&guid=" + guid + "&box=" + box + "&no=" + no);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);
        //
        JSONObject res = parseResult(response);
        JSONObject result = (JSONObject) res.get("result");

        //
        return result;
    }

    void sendCommit_internal(long no, String box, ReplicaInfo info, long partsCount, long totalBytes) throws Exception {
        HttpClient client = HttpClientBuilder.create().build();

        HttpPost post = new HttpPost(remoteUrl + "repl_send_commit.php");

        //
        JSONObject infoJson = info.toJSONObject();
        infoJson.put("protocolVersion", REPL_PROTOCOL_VERSION);
        infoJson.put("partsCount", partsCount);
        infoJson.put("totalBytes", totalBytes);

        //
        StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_no = new StringBody(String.valueOf(no), ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_info = new StringBody(infoJson.toString(), ContentType.MULTIPART_FORM_DATA);

        //
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addPart("guid", stringBody_guid);
        builder.addPart("box", stringBody_box);
        builder.addPart("no", stringBody_no);
        builder.addPart("info", stringBody_info);
        HttpEntity entity = builder.build();
        //
        post.setEntity(entity);

        //
        HttpResponse response = client.execute(post);

        //
        handleErrors(response);
        //
        parseResult(response);
    }

    byte[] readFilePart(File file, long pos, int file_max_size) throws IOException {
        long lenMax = file.length();
        int len = (int) Math.min(lenMax - pos, file_max_size);
        byte[] buff = new byte[len];
        FileInputStream fis = new FileInputStream(file);
        fis.skip(pos);
        fis.read(buff);
        return buff;
    }

    void handleErrors(HttpResponse response) throws Exception {
        if (response.getStatusLine().getStatusCode() != 200) {
            String resStr = EntityUtils.toString(response.getEntity());
            JSONObject res = null;
            try {
                res = parseJson(resStr);
            } catch (Exception e) {
                throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
            }
            if (res.get("error") != null) {
                throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode() + ", error: " + String.valueOf(res.get("error")));
            } else {
                throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode() + ", " + resStr);
            }
        }

    }

    JSONObject parseResult(HttpResponse response) throws Exception {
        String resStr = EntityUtils.toString(response.getEntity());
        JSONObject res = parseJson(resStr);
        if (res.get("error") != null) {
            throw new XError(String.valueOf(res.get("error")));
        }
        return res;
    }

    JSONObject parseJson(String jsonStr) throws Exception {
        JSONObject jsonObject;
        try {
            Reader reader = new StringReader(jsonStr);
            try {
                JSONParser p = new JSONParser();
                jsonObject = (JSONObject) p.parse(reader);
            } finally {
                reader.close();
            }
        } catch (Exception e) {
            log.error("parseJson.error: " + e.getMessage());
            log.error("parseJson.jsonStr: " + jsonStr);
            throw e;
        }

        //
        return jsonObject;
    }

    String seed() {
        return String.valueOf(Math.abs(rnd.nextLong()));
    }

    String getUrl(String url) {
        return remoteUrl + url + ".php?seed=" + seed();
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }

    public void createMailBox(String box) throws Exception {
        log.info("createMailBox, url: " + remoteUrl);
        log.info("createMailBox, guid: " + guid + ", box: " + box);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_create_box") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);
        //
        parseResult(response);
    }


    public void checkMailBox(String box) throws Exception {
        log.info("checkMailBox, url: " + remoteUrl);
        log.info("checkMailBox, guid: " + guid + ", box: " + box);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_state") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleErrors(response);
        //
        parseResult(response);
    }

}
