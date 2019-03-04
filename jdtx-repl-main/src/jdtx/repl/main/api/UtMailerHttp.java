package jdtx.repl.main.api;

import jandcode.utils.UtFile;
import jandcode.utils.UtString;
import jandcode.utils.error.XError;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
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
/*
    String remoteDirSend;
    String remoteDirReceive;
*/
    String localDir;

    protected static Log log = LogFactory.getLog("jdtx");


    @Override
    public void init(JSONObject cfg) {
        rootUrl = (String) cfg.get("url");
        guid = (String) cfg.get("guid");
/*
        remoteDirSend = (String) cfg.get("mailSend");
        remoteDirReceive = (String) cfg.get("mailReceive");
*/
        localDir = (String) cfg.get("mailLocalDir");
        //
        if (rootUrl == null || rootUrl.length() == 0) {
            throw new XError("Invalid rootUrl");
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Invalid guid");
        }
/*
        if (remoteDirSend == null || remoteDirSend.length() == 0) {
            throw new XError("Invalid remoteDirSend");
        }
        if (remoteDirReceive == null || remoteDirReceive.length() == 0) {
            throw new XError("Invalid remoteDirReceive");
        }
*/
        if (localDir == null || localDir.length() == 0) {
            throw new XError("Invalid localDir");
        }
/*
        remoteDirSend = UtFile.unnormPath(remoteDirSend) + "/";
        remoteDirReceive = UtFile.unnormPath(remoteDirReceive) + "/";
*/
        localDir = UtFile.unnormPath(localDir) + "/";
    }

    @Override
    public long getSrvSend() throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(rootUrl + "/repl_get_send.php?" + "guid=" + guid);

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
    public void send(IReplica repl, long no) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        // Отправка запроса
        HttpPost httpPost = new HttpPost(rootUrl + "/repl_send.php");

        //
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("guid", guid));
        params.add(new BasicNameValuePair("no", String.valueOf(no)));
        params.add(new BasicNameValuePair("file", UtFile.loadString(repl.getFile())));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        //
        HttpResponse response = httpclient.execute(httpPost);

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
        //return Long.valueOf((String) resStr.get("result"));
    }

    @Override
    public long getSrvReceive() throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(rootUrl + "/repl_get_receive.php?" + "guid=" + guid);

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
    public IReplica receive(long no) throws Exception {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(rootUrl + "/repl_receive.php?" + "guid=" + guid + "&no=" + no);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
        }

        //
        String localFileName = getFileName(no);
        File file = new File(localDir + localFileName);
        //File file = File.createTempFile("~jdx-" + UtString.padLeft(String.valueOf(no), 9, '0') + "-", ".xml");
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
