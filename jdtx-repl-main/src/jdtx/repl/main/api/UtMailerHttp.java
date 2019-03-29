package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.logging.*;
import org.apache.http.*;
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
public class UtMailerHttp implements IJdxMailer {


    String remoteUrl;
    String guid;
    String localDirTmp;
    Random rnd;

    protected static Log log = LogFactory.getLog("jdtx");

    // 32 Mb
    int HTTP_FILE_MAX_SIZE = 1024 * 1024 * 32;


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
        DefaultHttpClient httpclient = new DefaultHttpClient();

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
    public void send(IReplica repl, long no, String box) throws Exception {
        log.info("mailer.send, repl.wsId: " + repl.getWsId() + ", repl.age: " + repl.getAge() + ", no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);


        // Проверки: правильность типа реплики
        if (repl.getReplicaType() <= 0) {
            throw new XError("invalid replica.replicaType");
        }
        // Проверки: правильность возраста реплики
        if (repl.getAge() == -1) {
            throw new XError("invalid replica.age");
        }
        // Проверки: правильность кода рабочей станции
        if (repl.getWsId() <= 0) {
            throw new XError("invalid replica.wsId");
        }


        // Закачиваем
        DefaultHttpClient client = new DefaultHttpClient();

        // Закачиваем по частям
        int part = 0;
        long sentBytes = 0;
        long totalBytes = repl.getFile().length();

        //
        while (sentBytes < totalBytes) {
            //
            HttpPost post = new HttpPost(remoteUrl + "repl_part_send.php");

            //
            StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_no = new StringBody(String.valueOf(no), ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_part = new StringBody(String.valueOf(part), ContentType.MULTIPART_FORM_DATA);
            byte[] buff = readFilePart(repl.getFile(), sentBytes, HTTP_FILE_MAX_SIZE);
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
            part = part + 1;
            sentBytes = sentBytes + buff.length;

            //
            log.info("mailer.send, part: " + part + ", sentBytes: " + sentBytes + "/" + totalBytes);
        }


        // Завершение закачки
        JdxReplInfo info = new JdxReplInfo();
        info.wsId = repl.getWsId();
        info.age = repl.getAge();
        info.replicaType = repl.getReplicaType();
        info.crc = JdxUtils.getMd5File(repl.getFile());

        //
        sendCommit_internal(no, box, info, part);
    }


    @Override
    public IReplica receive(long no, String box) throws Exception {
        log.info("mailer.receive, no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);

        //
        String localFileName = "~" + getFileName(no);
        File replicaFile = new File(localDirTmp + localFileName);
        replicaFile.delete();

        //
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        JSONObject fileInfo = getInfo_internal(no, box);
        int filePartsCount = Integer.valueOf(String.valueOf(fileInfo.get("partsCount")));  // так сложно - потому что в res.get("partsCount") оказывается Long
        if (filePartsCount > 1) {
            log.info("mailer.receive, filePartsCount: " + filePartsCount);
        }

        // Закачиваем по частям
        int filePart = 0;
        int receivedBytes = 0;

        //
        while (filePart < filePartsCount) {
            //
            HttpGet httpGet = new HttpGet(getUrl("repl_part_receive") + "&guid=" + guid + "&box=" + box + "&no=" + no + "&file_part=" + filePart);

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
        DefaultHttpClient httpclient = new DefaultHttpClient();

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
        DefaultHttpClient httpclient = new DefaultHttpClient();

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
        DefaultHttpClient httpclient = new DefaultHttpClient();

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
    public JdxReplInfo getInfo(long no, String box) throws Exception {
        JSONObject res = getInfo_internal(no, box);

        //
        JdxReplInfo info = JdxReplInfo.fromJSONObject(res);

        //
        return info;
    }


    /**
     * Утилиты
     */

    JSONObject getInfo_internal(long no, String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

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

    void sendCommit_internal(long no, String box, JdxReplInfo info, int partsCount) throws Exception {
        DefaultHttpClient client = new DefaultHttpClient();

        HttpPost post = new HttpPost(remoteUrl + "repl_part_commit.php");

        //
        JSONObject infoJson = info.toJSONObject();
        infoJson.put("partsCount", partsCount);

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
            JSONObject res = parseJson(resStr);
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
        return String.valueOf(rnd.nextLong());
    }

    String getUrl(String url) {
        return remoteUrl + url + ".php?seed=" + seed();
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }

    void createMailBox(String box) throws Exception {
        log.info("createMailBox, url: " + remoteUrl);
        log.info("createMailBox, guid: " + guid + ", box: " + box);

        //
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_create_box") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
        }

        //
        String resStr = EntityUtils.toString(response.getEntity());
        JSONObject res = parseJson(resStr);
        if (res.get("error") != null) {
            throw new XError(String.valueOf(res.get("error")));
        }
    }


    void checkMailBox(String box) throws Exception {
        log.info("checkMailBox, url: " + remoteUrl);
        log.info("checkMailBox, guid: " + guid + ", box: " + box);

        //
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_state") + "&guid=" + guid + "&box=" + box);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
        }

        //
        String resStr = EntityUtils.toString(response.getEntity());
        JSONObject res = parseJson(resStr);
        if (res.get("error") != null) {
            throw new XError(String.valueOf(res.get("error")));
        }
    }

}
