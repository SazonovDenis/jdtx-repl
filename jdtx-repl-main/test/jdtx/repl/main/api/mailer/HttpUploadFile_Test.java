package jdtx.repl.main.api.mailer;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.File;

public class HttpUploadFile_Test {

    public static void main(final String[] args) throws Exception {
        String url = "http://localhost/lombard.systems/repl/repl_part_send.php";
        DefaultHttpClient client = new DefaultHttpClient();

        File file = new File("D:/Install/apache-tomcat-6.0.44.zip");
        //File file = new File("D:/Install/tortoisehg-4.1.0-x64.msi");
        HttpPost post = new HttpPost(url);

        FileBody fileBody = new FileBody(file, ContentType.DEFAULT_BINARY);
        StringBody stringBody_box = new StringBody("from", ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_guid = new StringBody("b5781df573ca6ee6-17845f2f56f4d401", ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_no = new StringBody("1234", ContentType.MULTIPART_FORM_DATA);
        //
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addPart("file", fileBody);
        builder.addPart("box", stringBody_box);
        builder.addPart("guid", stringBody_guid);
        builder.addPart("no", stringBody_no);
        //builder.addBinaryBody("upfile", file, ContentType.DEFAULT_BINARY, "xxxxx");
        HttpEntity entity = builder.build();
        //
        post.setEntity(entity);

        //
        HttpResponse response = client.execute(post);

        //
        System.out.println(response);
        String resStr = EntityUtils.toString(response.getEntity());
        System.out.println(resStr);
    }

}