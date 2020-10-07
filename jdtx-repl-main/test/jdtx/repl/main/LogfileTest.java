package jdtx.repl.main;

import jandcode.app.test.*;
import org.junit.*;

import java.io.*;
import java.util.*;

public class LogfileTest extends AppTestCase {

    {
        logSetUp = false;
    }

    @Test
    public void testLogJadatexRu() throws Exception {
        Map<String, Long> res = new HashMap();
        Map<String, String> ipTable = new HashMap();

        //
        BufferedReader f = new BufferedReader(new FileReader("D:/t/jadatex.ru.access.log"));
        String s = f.readLine();
        while (s != null) {
            String ss[] = s.split(" ");
            String ip = ss[0];
            String url = ss[6];
            String urlArr[] = url.split("&");
            if (url.equals("/robots.txt") || url.equals("/humans.txt") || url.equals("/ads.txt") || url.equals("/")) {
                String clientGuid = "/robots";
                ipTable.put(ip, clientGuid);
                res.put(ip, res.getOrDefault(ip, 0L) + 1);
            } else if (url.startsWith("/repl/web_status_json.php") || url.startsWith("/repl/web_status.html") || url.startsWith("/repl/js/") || url.startsWith("/repl/css/") || url.startsWith("/repl/images/") || url.contains(".png")) {
                String clientGuid = "/web_status_json.php";
                ipTable.put(ip, clientGuid);
                res.put(ip, res.getOrDefault(ip, 0L) + 1);
            } else if (ss[5].contains("POST")) {
                res.put(ip, res.getOrDefault(ip, 0L) + 1);
            } else if (urlArr.length < 4) {
                System.out.println("*** " + url);
            } else {
                String clientGuid = urlArr[3];
                ipTable.put(ip, clientGuid);
                res.put(ip, res.getOrDefault(ip, 0L) + 1);
            }

            //
            s = f.readLine();
        }

        //
        List<String> ips = new ArrayList<>(res.keySet());
        Collections.sort(ips);
        for (int i = 0; i < ips.size(); i++) {
            String ip = ips.get(i);
            Long count = res.get(ip);
            System.out.println(ipTable.get(ip).replace("-", "\t") + "\t" + ip + "\t" + count);
        }
    }


}
