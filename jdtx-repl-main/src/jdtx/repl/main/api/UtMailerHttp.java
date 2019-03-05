package jdtx.repl.main.api;

import jandcode.utils.UtFile;
import jandcode.utils.UtString;
import jandcode.utils.error.XError;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class UtMailerHttp implements IJdxMailer {


    String rootUrl;
    String guid;
    String localDirTmp;

    protected static Log log = LogFactory.getLog("jdtx");


    @Override
    public void init(JSONObject cfg) {
        rootUrl = (String) cfg.get("url");
        guid = (String) cfg.get("guid");
        localDirTmp = (String) cfg.get("mailLocalDirTmp");
        //
        if (rootUrl == null || rootUrl.length() == 0) {
            throw new XError("Invalid rootUrl");
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
    public long getSrvSend(String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(rootUrl + "/repl_get_send.php?" + "guid=" + guid + "&box=" + box);

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
        HttpPost post = new HttpPost(rootUrl + "/repl_send.php");

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
    public long getSrvReceive(String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(rootUrl + "/repl_get_receive.php?" + "guid=" + guid + "&box=" + box);

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
    public IReplica receive(long no, String box) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(rootUrl + "/repl_receive.php?" + "guid=" + guid + "&box=" + box + "&no=" + no);

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
