package jdtx.repl.main.api.mailer;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
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


    public String guid;
    String remoteUrl;
    String localDirTmp;
    Random rnd;

    protected static Log log = LogFactory.getLog("jdtx");

    // 32 Mb
    public static final int HTTP_FILE_MAX_SIZE = 1024 * 1024 * 32;
    //int HTTP_FILE_MAX_SIZE = 512;

    public static final String REPL_PROTOCOL_VERSION = "03";


    @Override
    public void init(JSONObject cfg) {
        remoteUrl = (String) cfg.get("url");
        guid = (String) cfg.get("guid");
        localDirTmp = (String) cfg.get("localDirTmp");
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
        remoteUrl = remoteUrl + "api." + REPL_PROTOCOL_VERSION + "/";
        //
        localDirTmp = UtFile.unnormPath(localDirTmp) + "/";
        //
        UtFile.mkdirs(localDirTmp);
        //
        rnd = new Random();
        rnd.setSeed(new DateTime().getMillis());
    }


    @Override
    public long getBoxState(String box) throws Exception {
        JSONObject res = getState_internal(box);
        JSONObject files = (JSONObject) res.get("files");
        return Long.valueOf(String.valueOf(files.get("max")));
    }

    @Override
    public long getSendRequired(String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        HttpGet httpGet = new HttpGet(getUrl("repl_get_send_required", info));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);

        //
        JSONObject required = (JSONObject) res.get("required");
        return Long.valueOf(String.valueOf(required.getOrDefault("required", -1)));
    }

    @Override
    public void setSendRequired(String box, long required) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("required", required);
        HttpGet httpGet = new HttpGet(getUrl("repl_set_send_required", info));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }

    @Override
    public void send(IReplica replica, String box, long no) throws Exception {
        log.info("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);


        // Проверки: правильность типа реплики
        JdxUtils.validateReplica(replica);

        // Проверки: не отправляли ли ранее такую реплику?
        // Защита от ситуации "восстановление на клиенте БД из бэкапа"
        JSONObject resState = getState_internal(box);
        JSONObject last_info = (JSONObject) resState.get("last_info");
        long srv_age = Long.valueOf(String.valueOf(last_info.getOrDefault("age", 0)));
        JSONObject required = (JSONObject) resState.get("required");
        long srv_required_send = Long.valueOf(String.valueOf(required.getOrDefault("required", -1)));
        if (no <= srv_age && srv_required_send == -1) {
            throw new XError("invalid replica.no, send.no: " + no + ", srv.no: " + srv_age);
        }


        // Закачиваем
        HttpClient client = HttpClientBuilder.create().build();

        // Закачиваем по частям
        long filePart = 0;
        long sentBytes = 0;
        long totalBytes = replica.getFile().length();


        // Большие письма отправляем с докачкой, для чего сначала выясняем, что уже успели закачать
        if (totalBytes > HTTP_FILE_MAX_SIZE) {
            JSONObject resInfo = getInfo_internal(box, no, false);
            JSONObject part_info = (JSONObject) resInfo.get("part_info");
            sentBytes = (long) part_info.get("total_bytes");
            filePart = (long) part_info.get("part_max_no");
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
            HttpPost post = new HttpPost(getUrlPost("repl_send_part"));

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
            builder.addPart("part", stringBody_part);
            builder.addPart("file", byteBody);
            HttpEntity entity = builder.build();
            //
            post.setEntity(entity);

            //
            HttpResponse response = client.execute(post);

            //
            handleHttpErrors(response);
            //
            parseHttpResponse(response);

            //
            filePart = filePart + 1;
            sentBytes = sentBytes + buff.length;

            //
            if (sentBytes != totalBytes) {
                log.info("mailer.send, part: " + filePart + ", sentBytes: " + sentBytes + "/" + totalBytes);
            } else {
                log.info("mailer.send, part: " + filePart + ", sentBytes: " + sentBytes);
            }
        }


        // Завершение закачки
        ReplicaInfo info = new ReplicaInfo();
        info.setDbStructCrc(replica.getInfo().getDbStructCrc());
        info.setWsId(replica.getInfo().getWsId());
        info.setAge(replica.getInfo().getAge());
        info.setDtFrom(replica.getInfo().getDtFrom());
        info.setDtTo(replica.getInfo().getDtTo());
        info.setReplicaType(replica.getInfo().getReplicaType());
        info.setCrc(JdxUtils.getMd5File(replica.getFile()));

        //
        sendCommit_internal(box, no, info, filePart, totalBytes);
    }


    @Override
    public ReplicaInfo getReplicaInfo(String box, long no) throws Exception {
        JSONObject resInfo = getInfo_internal(box, no);

        //
        JSONObject file_info = (JSONObject) resInfo.get("file_info");

        //
        return ReplicaInfo.fromJSONObject(file_info);
    }


    @Override
    public IReplica receive(String box, long no) throws Exception {
        log.info("mailer.receive, no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);

        // Читаем данные об очередном письме
        JSONObject resInfo = getInfo_internal(box, no);
        JSONObject file_info = (JSONObject) resInfo.get("file_info");

        // Проверим протокол репликатора, с помощью которого было отправлено письмо
        String protocolVersion = (String) file_info.get("protocolVersion");
        if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
            throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
        }

        // Сколько частей надо скачивать
        long filePartsCount = (long) file_info.get("partsCount");
        long totalBytes = (long) file_info.get("totalBytes");

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
            Map info = new HashMap<>();
            info.put("box", box);
            info.put("no", no);
            info.put("part", filePart);
            HttpGet httpGet = new HttpGet(getUrl("repl_receive_part", info));

            //
            HttpResponse response = httpclient.execute(httpGet);
            //
            handleHttpErrors(response);

            // Физическая скачка происходит в методе response.getEntity.
            // Поэтому buff получается сразу готовым, его можно безопасно дописывать в конец частично скачанному.
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
            if (receivedBytes != totalBytes) {
                log.info("mailer.receive, part: " + filePart + "/" + filePartsCount + ", receivedBytes: " + receivedBytes + "/" + totalBytes);
            } else {
                log.info("mailer.receive, part: " + filePart + "/" + filePartsCount + ", receivedBytes: " + receivedBytes);
            }
        }

        //
        IReplica replica = new ReplicaFile();
        replica.setFile(replicaFile);

        //
        return replica;
    }

    @Override
    public void delete(String box, long no) throws Exception {
        log.info("mailer.delete, no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("no", no);
        HttpGet httpGet = new HttpGet(getUrl("repl_delete", info));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }


    /**
     * Утилиты
     */

    JSONObject getState_internal(String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        HttpGet httpGet = new HttpGet(getUrl("repl_get_state", info));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);

        //
        return res;
    }


    JSONObject getInfo_internal(String box, long no) throws Exception {
        return getInfo_internal(box, no, true);
    }

    JSONObject getInfo_internal(String box, long no, boolean doRaiseErrors) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("no", no);
        HttpGet httpGet = new HttpGet(getUrl("repl_get_info", info));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);

        //
        if (doRaiseErrors && res.get("error") != null) {
            throw new XError(String.valueOf(res.get("error")));
        }

        //
        return res;
    }


    void sendCommit_internal(String box, long no, ReplicaInfo info, long partsCount, long totalBytes) throws Exception {
        HttpClient client = HttpClientBuilder.create().build();

        HttpPost post = new HttpPost(getUrlPost("repl_send_commit"));

        //
        JSONObject infoJson = info.toJSONObject();
        infoJson.put("protocolVersion", REPL_PROTOCOL_VERSION);
        infoJson.put("partsCount", partsCount);
        infoJson.put("totalBytes", totalBytes);
        infoJson.put("no", no);

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
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
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

    void handleHttpErrors(HttpResponse response) throws Exception {
        if (response.getStatusLine().getStatusCode() != 200) {
            String resStr = EntityUtils.toString(response.getEntity());
            JSONObject res;
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

    JSONObject parseHttpResponse(HttpResponse response) throws Exception {
        String resStr = EntityUtils.toString(response.getEntity());
        JSONObject res = parseJson(resStr);
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

    String getUrl(String url, Map info) {
        UrlBuilder b = new UrlBuilder();
        b.append("seed", seed());
        b.append("protocolVersion", REPL_PROTOCOL_VERSION);
        b.append("appVersion", UtRepl.getVersion());
        b.append("guid", guid);
        if (info != null) {
            b.append(info);
        }
        return remoteUrl + url + ".php" + b.toString();
    }

    String getUrlPost(String url) {
        return remoteUrl + url + ".php?" + "protocolVersion=" + REPL_PROTOCOL_VERSION + "&appVersion=" + UtRepl.getVersion();
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
        Map info = new HashMap<>();
        info.put("box", box);
        HttpGet httpGet = new HttpGet(getUrl("repl_create_box", info));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);
        //
        parseHttpResponse(response);
    }


    public void checkMailBox(String box) throws Exception {
        log.info("checkMailBox, url: " + remoteUrl);
        log.info("checkMailBox, guid: " + guid + ", box: " + box);

        // Обращение getState_internal ящика доказывает его нормальную работу
        getState_internal(box);
    }


    public JSONObject getData(String name, String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map params = new HashMap<>();
        params.put("name", name);
        params.put("box", box);
        HttpGet httpGet = new HttpGet(getUrl("repl_get_data", params));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        return parseHttpResponse(response);
    }

    public void setData(Map data, String name, String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        JSONObject data_json = null;
        if (data != null) {
            data_json = new JSONObject(data);
        }

        //
        Map params = new HashMap<>();
        params.put("data", data_json);
        params.put("name", name);
        params.put("box", box);
        HttpGet httpGet = new HttpGet(getUrl("repl_set_data", params));

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }

}
