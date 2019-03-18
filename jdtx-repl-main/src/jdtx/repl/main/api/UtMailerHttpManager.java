package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import org.apache.commons.logging.*;
import org.apache.http.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.*;
import org.joda.time.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 */
public class UtMailerHttpManager {


    String remoteUrl;
    String guid;
    Random rnd;

    protected static Log log = LogFactory.getLog("jdtx");


    public void init(String cfgFileName, long wsId) throws Exception {
        JSONObject cfg = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));
        JSONObject cfgWs = (JSONObject) cfg.get(String.valueOf(wsId));

        //
        remoteUrl = (String) cfgWs.get("url");
        guid = (String) cfgWs.get("guid");
        //
        if (remoteUrl == null || remoteUrl.length() == 0) {
            throw new XError("Invalid remoteUrl");
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Invalid guid");
        }

        //
        rnd = new Random();
        rnd.setSeed(new DateTime().getMillis());
    }


    /**
     * IJdxMailerManager
     */

    public void createMail() throws Exception {
        createMailBox(guid, "from");
        createMailBox(guid, "to");
    }

    public void checkMail() throws Exception {
        checkMailBox(guid, "from");
        checkMailBox(guid, "to");
    }

    void checkMailBox(String guid, String box) throws Exception {
        log.info("checkMailBox, url: " + remoteUrl);
        log.info("checkMailBox, guid: " + guid + ", box: " + box);

        //
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_get_state.php?nonce=" + nonce() + "&guid=" + guid + "&box=" + box);

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

    void createMailBox(String guid, String box) throws Exception {
        log.info("createMailBox, url: " + remoteUrl);
        log.info("createMailBox, guid: " + guid + ", box: " + box);

        //
        DefaultHttpClient httpclient = new DefaultHttpClient();

        //
        HttpGet httpGet = new HttpGet(remoteUrl + "/repl_create_box.php?nonce=" + nonce() + "&guid=" + guid + "&box=" + box);

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

    /**
     * Утилиты
     */

    String nonce() {
        return String.valueOf(rnd.nextLong());
    }

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

}
