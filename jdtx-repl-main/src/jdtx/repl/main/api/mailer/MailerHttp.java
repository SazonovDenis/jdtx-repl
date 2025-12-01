package jdtx.repl.main.api.mailer;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.*;
import org.apache.http.entity.*;
import org.apache.http.entity.mime.*;
import org.apache.http.entity.mime.content.*;
import org.apache.http.impl.client.*;
import org.apache.http.impl.cookie.*;
import org.apache.http.message.*;
import org.apache.http.protocol.*;
import org.apache.http.util.*;
import org.joda.time.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 *
 */
public class MailerHttp implements IMailer {


    public String guid;
    String remoteUrl;
    String localDirTmp;
    Random rnd;

    protected static Log log = LogFactory.getLog("jdtx.MailerHttp");

    public static final int HTTP_FILE_MAX_SIZE = 1024 * 1024 * 32; // 32 Mb
    //int HTTP_FILE_MAX_SIZE = 512;

    public static final String REPL_PROTOCOL_VERSION = "04";


    @Override
    public void init(JSONObject cfg) {
        remoteUrl = (String) cfg.get("url");
        guid = (String) cfg.get("guid");
        localDirTmp = (String) cfg.get("localDirTmp");
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
        if (!remoteUrl.endsWith("/")) {
            remoteUrl = remoteUrl + "/";
        }
        remoteUrl = remoteUrl + "api." + REPL_PROTOCOL_VERSION + "/";
        //
        localDirTmp = UtFile.unnormPath(localDirTmp) + "/";
        //
        // todo нехорошая зависимость мейлера от других модулей, которые определяют перечень ящиков
        UtFile.mkdirs(localDirTmp + "/from");
        UtFile.mkdirs(localDirTmp + "/to");
        UtFile.mkdirs(localDirTmp + "/to001");
        //
        rnd = new Random();
        rnd.setSeed(new DateTime().getMillis());
    }


    @Override
    public long getBoxState(String box) throws Exception {
        JSONObject res = getData("files", box);
        JSONObject files = (JSONObject) res.get("files");
        return UtJdxData.longValueOf(files.get("max"));
    }

    @Override
    public long getSendDone(String box) throws Exception {
        JSONObject res = getData("last.write", box);
        JSONObject data = (JSONObject) res.get("data");
        boolean completelySend = UtJdxData.booleanValueOf(data.get("all"), false);
        long no = UtJdxData.longValueOf(data.get("no"), 0L);
        if (completelySend) {
            return no;
        } else {
            if (no == 0) {
                return no;
            } else {
                return no - 1;
            }
        }
    }

    @Override
    public long getReceiveDone(String box) throws Exception {
        JSONObject res = getData("last.read", box);
        JSONObject data = (JSONObject) res.get("data");
        long no = UtJdxData.longValueOf(data.getOrDefault("no", 0));
        return no;
    }

    @Override
    public RequiredInfo getSendRequired(String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        HttpGet httpGet = createHttpGet("repl_get_send_required", info);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);

        //
        JSONObject required = (JSONObject) res.get("required");
        RequiredInfo requiredInfo = new RequiredInfo(required);

        //
        return requiredInfo;
    }

    @Override
    public void setSendRequired(String box, RequiredInfo requiredInfo) throws Exception {
        log.info("mailer.setSendRequired, box: " + box + ", " + requiredInfo.toString() + ", remoteUrl: " + remoteUrl);

        //
        validateRequiredInfo(requiredInfo);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("requiredFrom", requiredInfo.requiredFrom);
        info.put("requiredTo", requiredInfo.requiredTo);
        info.put("recreate", requiredInfo.recreate);
        info.put("executor", requiredInfo.executor);
        HttpGet httpGet = createHttpGet("repl_set_send_required", info);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }

    private void validateRequiredInfo(RequiredInfo requiredInfo) {
        if (requiredInfo.requiredFrom == -1) {
            // Запрос пустой - не проверяеем больше ничего
            return;
        }

        // executor
        if (requiredInfo.executor == null) {
            throw new XError("validateRequiredInfo: requiredInfo.executor == null");
        }
        if (requiredInfo.executor.compareToIgnoreCase(RequiredInfo.EXECUTOR_SRV) != 0 && requiredInfo.executor.compareToIgnoreCase(RequiredInfo.EXECUTOR_WS) != 0) {
            throw new XError("validateRequiredInfo: requiredInfo.executor not valid");
        }

        // Правильный интервал requiredFrom .. requiredTo
        if (requiredInfo.requiredTo != -1 && requiredInfo.requiredFrom > requiredInfo.requiredTo) {
            throw new XError("validateRequiredInfo: requiredInfo.requiredTo > requiredInfo.requiredFrom");
        }
    }

    @Override
    public void send(IReplica replica, String box, long no) throws Exception {
        log.debug("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", box: " + box + ", remoteUrl: " + remoteUrl);

        // Проверки: правильность полей реплики
        UtJdx.validateReplicaFields(replica);

        // Если почему-то не указан crc файла данных - вычисляем
        if (replica.getData() != null && (replica.getInfo().getCrc() == null || replica.getInfo().getCrc().length() == 0)) {
            log.warn("mailer.send, replica.crc is no set");
            String crcFile = UtJdx.getMd5File(replica.getData());
            replica.getInfo().setCrc(crcFile);
        }

        // Закачиваем
        HttpClient client = HttpClientBuilder.create().build();

        // Закачиваем по частям
        long filePart = 0;
        long sentBytes = 0;
        long totalBytes = replica.getData().length();


        // Большие письма отправляем с докачкой, для чего сначала выясняем, что уже успели закачать
        if (totalBytes > HTTP_FILE_MAX_SIZE) {
            JSONObject resInfo = getInfo_internal(box, no, false);
            JSONObject part_info = (JSONObject) resInfo.get("part_info");
            sentBytes = (long) part_info.get("total_bytes");
            filePart = (long) part_info.get("part_max_no");
            //
            if (sentBytes > 0) {
                log.info("mailer.send, already sent part: " + filePart + ", sent bytes: " + sentBytes + "/" + totalBytes);
            }
            //
            filePart = filePart + 1;
        }


        // Часть за частью
        while (sentBytes < totalBytes) {
            //
            HttpPost post = createHttpPost("repl_send_part");

            //
            byte[] buff = readFilePart(replica.getData(), sentBytes, HTTP_FILE_MAX_SIZE);
            ByteArrayBody byteBody = new ByteArrayBody(buff, "file");
            //
            String partCrc = UtJdx.getMd5Buffer(buff);
            //
            StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_no = new StringBody(String.valueOf(no), ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_part = new StringBody(String.valueOf(filePart), ContentType.MULTIPART_FORM_DATA);
            StringBody stringBody_partCrc = new StringBody(partCrc, ContentType.MULTIPART_FORM_DATA);

            //
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            builder.addPart("guid", stringBody_guid);
            builder.addPart("box", stringBody_box);
            builder.addPart("no", stringBody_no);
            builder.addPart("part", stringBody_part);
            builder.addPart("partCrc", stringBody_partCrc);
            builder.addPart("file", byteBody);
            HttpEntity entity = builder.build();
            //
            post.setEntity(entity);

            //
            HttpResponse response = client.execute(post);

            //
            handleHttpErrors(response);

            //
            parseHttpResponse(response);

            //
            filePart = filePart + 1;
            sentBytes = sentBytes + buff.length;

            //
            if (sentBytes != totalBytes) {
                log.info("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", box: " + box + ", part: " + filePart + ", sentBytes: " + sentBytes + "/" + totalBytes);
            } else {
                log.info("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", box: " + box + ", part: " + filePart + ", sentBytes: " + sentBytes);
            }
        }


        // Завершение закачки
        IReplicaInfo info = new ReplicaInfo();
        info.setReplicaType(replica.getInfo().getReplicaType());
        info.setDbStructCrc(replica.getInfo().getDbStructCrc());
        info.setWsId(replica.getInfo().getWsId());
        info.setNo(replica.getInfo().getNo());
        info.setAge(replica.getInfo().getAge());
        info.setDtFrom(replica.getInfo().getDtFrom());
        info.setDtTo(replica.getInfo().getDtTo());
        info.setCrc(replica.getInfo().getCrc());

        //
        sendCommit_internal(box, no, info, filePart, totalBytes);
    }


    @Override
    public IReplicaInfo getReplicaInfo(String box, long no) throws Exception {
        JSONObject resInfo = getInfo_internal(box, no);

        //
        JSONObject file_info = (JSONObject) resInfo.get("file_info");

        //
        IReplicaInfo info = new ReplicaInfo();
        info.fromJSONObject(file_info);
        return info;
    }

    String getPartInfoStr(long filePartNo, long filePartsCount) {
        String filePartInfo = "";
        if (filePartsCount > 1) {
            filePartInfo = ", part: " + (filePartNo + 1) + "/" + filePartsCount;
        }
        return filePartInfo;
    }

    @Override
    public IReplica receive(String box, long no) throws Exception {
        log.debug("mailer.receive, no: " + no + ", box: " + box + ", remoteUrl: " + remoteUrl);

        // Читаем данные об очередном письме
        JSONObject resInfo = getInfo_internal(box, no);
        JSONObject file_info = (JSONObject) resInfo.get("file_info");

        // Проверим протокол репликатора, с помощью которого было отправлено письмо
        String protocolVersion = (String) file_info.get("protocolVersion");
        if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
            throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
        }


        // ---
        // Есть ранее скачанный файл реплики - удаляем
        File replicaFileTmp = new File(localDirTmp + getFileName(box, no));

        //
        if (replicaFileTmp.exists()) {
            log.info("mailer.receive, delete previously received file");
            if (!replicaFileTmp.delete()) {
                throw new XError("Unable to delete previously received file: " + replicaFileTmp.getAbsolutePath());
            }
        }


        // ---
        // Скачиваем по частям

        // Сколько частей надо скачивать
        long filePartsCount = (long) file_info.get("partsCount");

        // Если частей много - сообщим в log.info
        if (filePartsCount > 1) {
            log.info("mailer.receive, filePartsCount: " + filePartsCount);
        }

        // Докачиваем части, которых нет
        long filePartNo = 0;
        while (filePartNo < filePartsCount) {
            File partFileTmp = new File(localDirTmp + getPartName(box, no, filePartNo));
            if (partFileTmp.exists()) {
                log.info("mailer.receive, already received no: " + no + ", box: " + box + getPartInfoStr(filePartNo, filePartsCount));
            } else {
                receiveFilePart(box, no, filePartNo);

                //
                log.info("mailer.receive, received no: " + no + ", box: " + box + getPartInfoStr(filePartNo, filePartsCount));
            }

            //
            filePartNo = filePartNo + 1;
        }


        // ---
        // Все порции скачаны - собираем порции в один файл
        FileOutputStream outputStream = new FileOutputStream(replicaFileTmp);
        try {
            //
            filePartNo = 0;
            while (filePartNo < filePartsCount) {
                File partFileTmp = new File(localDirTmp + getPartName(box, no, filePartNo));

                // Читаем и копируем файл порции
                FileInputStream inputStream = new FileInputStream(partFileTmp);
                try {
                    byte[] buff = new byte[HTTP_FILE_MAX_SIZE];
                    while (inputStream.available() != 0) {
                        // Читаем файл порции
                        int partSize = inputStream.read(buff);

                        // Копируем порцию
                        outputStream.write(buff, 0, partSize);
                    }
                } finally {
                    inputStream.close();
                }


                // Удаляем файл порции
                if (!partFileTmp.delete()) {
                    throw new XError("Unable to delete part: " + filePartNo + ", file: " + partFileTmp.getAbsolutePath());
                }

                //
                if (filePartsCount > 1) {
                    log.info("mailer.receive, merge parts" + getPartInfoStr(filePartNo, filePartsCount));
                }

                //
                filePartNo = filePartNo + 1;
            }
        } finally {
            outputStream.close();
        }


        // ---
        // Файл скачан, формируем и проверяем реплику

        // Реплика
        IReplica replica = new ReplicaFile();
        replica.setData(replicaFileTmp);

        //
        replica.getInfo().fromJSONObject(file_info);

        // Проверяем целостность скачанного
        String crcInfo = (String) file_info.get("crc");
        UtJdx.checkReplicaCrc(replica, crcInfo);


        //
        return replica;
    }

    @Override
    public long delete(String box, long no) throws Exception {
        return deleteInternal(box, no, false);
    }


    @Override
    public long deleteAll(String box, long no) throws Exception {
        return deleteInternal(box, no, true);
    }


    public long deleteInternal(String box, long no, boolean deleteAllBelowNo) throws Exception {
        log.debug("mailer.delete, no: " + no + ", box: " + box + ", delete all below: " + deleteAllBelowNo + ", remoteUrl: " + remoteUrl);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("no", no);
        if (deleteAllBelowNo) {
            info.put("all", deleteAllBelowNo);
        }
        HttpGet httpGet = createHttpGet("repl_delete", info);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);

        //
        return UtJdxData.longValueOf(res.get("deleted"));
    }


    /**
     * Утилиты
     */

    JSONObject getInfo_internal(String box, long no) throws Exception {
        return getInfo_internal(box, no, true);
    }

    JSONObject getInfo_internal(String box, long no, boolean doRaiseErrors) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("no", no);
        HttpGet httpGet = createHttpGet("repl_get_info", info);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);

        //
        if (doRaiseErrors && res.get("error") != null) {
            throw new XError(String.valueOf(res.get("error")));
        }

        //
        return res;
    }

    public IReplicaInfo getLastReplicaInfo(String box) throws Exception {
        JSONObject res = getData("last.dat.info", box);
        JSONObject data = (JSONObject) res.get("data");
        IReplicaInfo info = new ReplicaInfo();
        info.fromJSONObject(data);
        return info;
    }

    void sendCommit_internal(String box, long no, IReplicaInfo info, long partsCount, long totalBytes) throws Exception {
        HttpClient client = HttpClientBuilder.create().build();

        HttpPost post = createHttpPost("repl_send_commit");

        // Дополним replica.info техническими данными
        // Значения полей crc и no ЗАПИСЫВАЕМ, т.к. на момент отправки реплики эти значения должны быть уже ИЗВЕСТНЫ
        JSONObject infoJson = info.toJSONObject_withFileInfo();
        infoJson.put("protocolVersion", REPL_PROTOCOL_VERSION);
        infoJson.put("partsCount", partsCount);
        infoJson.put("totalBytes", totalBytes);

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
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }


    private void receiveFilePart(String box, long no, long filePartNo) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map info = new HashMap<>();
        info.put("box", box);
        info.put("no", no);
        info.put("part", filePartNo);
        HttpGet httpGet = createHttpGet("repl_receive_part", info);
        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        // Физическая скачка происходит в методе response.getEntity.
        // Поэтому buff получается сразу готовым, его можно безопасно дописывать в конец частично скачанному.
        HttpEntity entity = response.getEntity();
        byte[] buff = EntityUtils.toByteArray(entity);
        //
        File partFileTmp = new File(localDirTmp + getPartName(box, no, filePartNo));
        FileOutputStream outputStream = new FileOutputStream(partFileTmp);
        outputStream.write(buff);
        outputStream.close();
    }


    byte[] readFilePart(File file, long pos, int fileMaxSize) throws IOException {
        long lenMax = file.length();
        int len = (int) Math.min(lenMax - pos, fileMaxSize);
        byte[] buff = new byte[len];
        FileInputStream fis = new FileInputStream(file);
        fis.skip(pos);
        fis.read(buff);
        return buff;
    }

    void handleHttpErrors(HttpResponse response) throws Exception {
        //log.debug("response: " + response.getStatusLine().toString());
        //
        if (response.getStatusLine().getStatusCode() != 200) {
            String resStr = EntityUtils.toString(response.getEntity());
            JSONObject res;
            try {
                res = parseJson(resStr);
            } catch (Exception e) {
                throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode());
            }
            if (res.get("error") != null) {
                throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode() + ", error: " + String.valueOf(res.get("error")));
            } else {
                throw new XError("HttpResponse.StatusCode: " + response.getStatusLine().getStatusCode() + ", " + resStr);
            }
        }
    }

    JSONObject parseHttpResponse(HttpResponse response) throws Exception {
        HttpEntity entity = response.getEntity();

        //
        String resStr = EntityUtils.toString(entity);

        //
        JSONObject res = parseJson(resStr);
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
            log.error("parseJson.error: " + UtJdxErrors.collectExceptionText(e));
            log.debug("parseJson.jsonStr: " + jsonStr);
            throw e;
        }

        //
        return jsonObject;
    }

    private String getUrlDomain(String url) throws URISyntaxException {
        URI uri = new URI(url);
        String domain = uri.getHost();
        return domain;
    }

    public void createGuid(String guid, String pass) throws Exception {
        log.info("createGuid, url: " + remoteUrl + ", guid: " + guid);

        // Если указан пароль - то залогинимся
        HttpContext context = null;
        if (pass != null) {
            String token = login(pass);
            context = createCookieContext(UtCnv.toMap("token", token));
        }

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpPost httpPost = createHttpPost("repl_create_guid");

        //
        StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
        //
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addPart("guid", stringBody_guid);
        HttpEntity entity = builder.build();
        //
        httpPost.setEntity(entity);

        //
        HttpResponse response = httpclient.execute(httpPost, context);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }

    private HttpContext createCookieContext(Map<String, Object> values) throws URISyntaxException {
        BasicCookieStore cookieStore = new BasicCookieStore();

        for (String key : values.keySet()) {
            BasicClientCookie cookie = new BasicClientCookie(key, String.valueOf(values.get(key)));
            cookie.setDomain(getUrlDomain(remoteUrl));
            cookieStore.addCookie(cookie);
        }

        HttpContext context = new BasicHttpContext();
        context.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

        return context;
    }


    public void createMailBox(String box) throws Exception {
        log.info("createMailBox, url: " + remoteUrl + ", guid: " + guid + ", box: " + box);

        //
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        HttpPost httpPost = createHttpPost("repl_create_box");

        //
        StringBody stringBody_guid = new StringBody(guid, ContentType.MULTIPART_FORM_DATA);
        StringBody stringBody_box = new StringBody(box, ContentType.MULTIPART_FORM_DATA);
        //
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addPart("guid", stringBody_guid);
        builder.addPart("box", stringBody_box);
        HttpEntity entity = builder.build();
        //
        httpPost.setEntity(entity);

        //
        HttpResponse response = httpclient.execute(httpPost);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }


    public void checkMailBox(String box) throws Exception {
        log.info("checkMailBox, url: " + remoteUrl);
        log.info("checkMailBox, guid: " + guid + ", box: " + box);

        // Успешное обращение к getData ящика доказывает его нормальную работу
        getData("files", box);
        getData("ping.read", box);
        getData("ping.write", box);
        getData("last.dat.info", box);
        getData("last.read", box);
        getData("last.write", box);
        getData("required.info", box);
    }

    public String login(String password) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map params = new HashMap<>();
        params.put("pass", password);
        HttpGet httpGet = createHttpGet("login", params);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        JSONObject res = parseHttpResponse(response);
        return String.valueOf(res.get("token"));
    }


    public JSONObject getData(String name, String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map params = new HashMap<>();
        params.put("name", name);
        params.put("box", box);
        HttpGet httpGet = createHttpGet("repl_get_data", params);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        return parseHttpResponse(response);
    }

    public void setData(Map data, String name, String box) throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        JSONObject data_json = null;
        if (data != null) {
            data_json = new JSONObject(data);
        }

        //
        Map params = new HashMap<>();
        params.put("data", data_json);
        params.put("name", name);
        params.put("box", box);
        HttpGet httpGet = createHttpGet("repl_set_data", params);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        parseHttpResponse(response);
    }

    public JSONObject ping() throws Exception {
        HttpClient httpclient = HttpClientBuilder.create().build();

        //
        Map params = new HashMap<>();
        HttpGet httpGet = createHttpGet("ping", params);

        //
        HttpResponse response = httpclient.execute(httpGet);

        //
        handleHttpErrors(response);

        //
        return parseHttpResponse(response);
    }

    private String seed() {
        return String.valueOf(Math.abs(rnd.nextLong()));
    }

    private String getUrl(String url, Map info) {
        UrlBuilder b = new UrlBuilder();
        b.append("seed", seed());
        b.append("protocolVersion", REPL_PROTOCOL_VERSION);
        b.append("appVersion", UtRepl.getVersion());
        b.append("guid", guid);
        if (info != null) {
            b.append(info);
        }
        String urlRes = remoteUrl + url + ".php" + b.toString();
        //
        //log.debug("getUrl: " + urlRes);
        //
        return urlRes;
    }

    private String getUrlPost(String url) {
        String urlRes = remoteUrl + url + ".php?" + "seed=" + seed() + "&protocolVersion=" + REPL_PROTOCOL_VERSION + "&appVersion=" + UtRepl.getVersion();
        //
        //log.debug("getUrlPost: " + urlRes);
        //
        return urlRes;
    }

    public static String getFileName(String box, long no) {
        return "/" + box + "/" + UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }

    public static String getPartName(String box, long no, long part) {
        return "/" + box + "/" + UtString.padLeft(String.valueOf(no), 9, '0') + "." + UtString.padLeft(String.valueOf(part), 3, '0');
    }

    private HttpGet createHttpGet(String funcName, Map params) {
        HttpGet httpGet = new HttpGet(getUrl(funcName, params));
        httpGet.setHeader(new BasicHeader("Pragma", "no-cache"));
        httpGet.setHeader(new BasicHeader("Cache-Control", "no-cache"));
        log.debug(httpGet.getMethod() + " " + httpGet.getURI());
        log.debug(httpGet.toString());
        return httpGet;
    }

    private HttpPost createHttpPost(String funcName) {
        HttpPost httpPost = new HttpPost(getUrlPost(funcName));
        httpPost.setHeader(new BasicHeader("Pragma", "no-cache"));
        httpPost.setHeader(new BasicHeader("Cache-Control", "no-cache"));
        log.debug(httpPost.getMethod() + " " + httpPost.getURI());
        log.debug(httpPost.toString());
        return httpPost;
    }

}
