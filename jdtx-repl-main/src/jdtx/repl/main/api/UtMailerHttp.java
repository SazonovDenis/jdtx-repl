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
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
        }

        //
        String resStr = EntityUtils.toString(response.getEntity());
        JSONObject res = parseJson(resStr);
        if (res.get("error") != null) {
            throw new XError(String.valueOf(res.get("error")));
        }

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

        //
        DefaultHttpClient client = new DefaultHttpClient();

        //
        HttpPost post = new HttpPost(remoteUrl + "repl_send.php");

        //
        JdxReplInfo info = new JdxReplInfo();
        info.wsId = repl.getWsId();
        info.age = repl.getAge();
        info.replicaType = repl.getReplicaType();
        info.crc = JdxUtils.getMd5File(repl.getFile());

        //
        StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_no = new StringBody(String.valueOf(no), ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_info = new StringBody(info.toString(), ContentType.MULTIPART_FORM_DATA);
        FileBody fileBody = new FileBody(repl.getFile(), ContentType.DEFAULT_BINARY);

        //
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addPart("guid", stringBody_guid);
        builder.addPart("box", stringBody_box);
        builder.addPart("no", stringBody_no);
        builder.addPart("info", stringBody_info);
        builder.addPart("file", fileBody);
        HttpEntity entity = builder.build();
        //
        post.setEntity(entity);

        //
        HttpResponse response = client.execute(post);

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

    @Override
    public IReplica receive(long no, String box) throws Exception {
        log.info("mailer.receive, no: " + no + ", remoteUrl: " + remoteUrl + ", box: " + box);

        //
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_receive") + "&guid=" + guid + "&box=" + box + "&no=" + no);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
        }

        //
        byte[] res = EntityUtils.toByteArray(response.getEntity());
        //
        String localFileName = "~" + getFileName(no);
        File replicaFile = new File(localDirTmp + localFileName);
        FileOutputStream outputStream = new FileOutputStream(replicaFile);
        outputStream.write(res);
        outputStream.close();

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

    @Override
    public void ping(String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_ping") + "&guid=" + guid + "&box=" + box);

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

    @Override
    public DateTime getPingDt(String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_ping_dt") + "&guid=" + guid + "&box=" + box);

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
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(getUrl("repl_get_info") + "&guid=" + guid + "&box=" + box + "&no=" + no);

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

        //
        JdxReplInfo info = JdxReplInfo.fromJSONObject(res);
        return info;
    }


    /**
     * Утилиты
     */

    JSONObject parseJson(String jsonStr) throws Exception {
        JSONObject cfgData;
        try {
            Reader r = new StringReader(jsonStr);
            try {
                JSONParser p = new JSONParser();
                cfgData = (JSONObject) p.parse(r);
            } finally {
                r.close();
            }
        } catch (Exception e) {
            log.error("parseJson.error: " + e.getMessage());
            log.error("parseJson.jsonStr: " + jsonStr);
            throw e;
        }

        //
        return cfgData;
    }

    String seed() {
        return String.valueOf(rnd.nextLong());
    }

    private String getUrl(String url) {
        return remoteUrl + url + ".php?seed=" + seed();
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }


}
