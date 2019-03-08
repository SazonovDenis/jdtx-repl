package jdtx.repl.main.api;

import jandcode.utils.UtFile;
import jandcode.utils.UtString;
import jandcode.utils.error.XError;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;

/**
 */
public class UtMailerHttp implements IJdxMailer {


    String remoteUrl;
    String guid;
    String localDirTmp;

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
    }


    @Override
    public long getSrvSate(String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_get_state.php?" + "guid=" + guid + "&box=" + box);

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
        return Long.valueOf(String.valueOf(res.get("result")));
    }

    @Override
    public void send(IReplica repl, long no, String box) throws Exception {
        DefaultHttpClient client = new DefaultHttpClient();

        //
        HttpPost post = new HttpPost(remoteUrl + "/repl_send.php");

        //
        StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_no = new StringBody(String.valueOf(no), ContentType.MULTIPART_FORM_DATA);
        FileBody fileBody = new FileBody(repl.getFile(), ContentType.DEFAULT_BINARY);
        //
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addPart("guid", stringBody_guid);
        builder.addPart("box", stringBody_box);
        builder.addPart("no", stringBody_no);
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
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_receive.php?" + "guid=" + guid + "&box=" + box + "&no=" + no);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
        }

        //
        String localFileName = getFileName(no);
        File file = new File(localDirTmp + localFileName);
        String resStr = EntityUtils.toString(response.getEntity());
        UtFile.saveString(resStr, file);

        //
        IReplica replica = new ReplicaFile();
        replica.setFile(file);

        //
        return replica;
    }

    @Override
    public void delete(long n, String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_delete.php?" + "guid=" + guid + "&box=" + box + "&no=" + n);

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
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_ping.php?" + "guid=" + guid + "&box=" + box);

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
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_get_ping_dt.php?" + "guid=" + guid + "&box=" + box);

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

    JSONObject parseJson(String jsonStr) throws Exception {
        JSONObject cfgData;
        Reader r = new StringReader(jsonStr);
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }

        return cfgData;
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

}
