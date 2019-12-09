package jdtx.repl.main.api.mailer;


import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.*;

import java.io.*;

public class HttpFileDownload_Test {

    public static void main(final String[] args) throws Exception {
        String url = "http://jadatex.ru/repl/api.03/repl_receive_part.php";
        url = url + "?protocolVersion=03&appVersion=424DEV&guid=B8DBF471BA263AFA.lombardsk-015FBC308EC5180C&no=783&part=0&box=to";

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpGet httpGet = new HttpGet(url);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        System.out.println("response: " + response.getStatusLine().toString());

        //
        //String resStr = EntityUtils.toString(response.getEntity());

        //
        //System.out.println("resStr: "+resStr);

        //
        HttpEntity entity = response.getEntity();
        byte[] buff = EntityUtils.toByteArray(entity);
        //
        File replicaFile = new File("temp/file.zip");
        FileOutputStream outputStream = new FileOutputStream(replicaFile, true);
        outputStream.write(buff);
        outputStream.close();

        //
        System.out.println("replicaFile.length: " + replicaFile.length());
    }

}