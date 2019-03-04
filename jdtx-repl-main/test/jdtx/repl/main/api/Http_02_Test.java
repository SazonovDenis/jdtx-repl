package jdtx.repl.main.api;


import org.apache.http.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.util.*;

public class Http_02_Test {

    public static void main(final String[] args) throws Exception {
        String authUrl = "http://google.com";

        // Отправка запроса
        DefaultHttpClient httpclient_0 = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(authUrl);

        // Получение ответа
        HttpResponse response = httpclient_0.execute(httpget);
        String resStr = EntityUtils.toString(response.getEntity());

        //
        System.out.println(resStr);

        // Отправка запроса
        DefaultHttpClient httpclient_1 = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(authUrl);

        // Получение ответа
        response = httpclient_1.execute(httpPost);
        resStr = EntityUtils.toString(response.getEntity());

        //
        System.out.println(resStr);
    }
}