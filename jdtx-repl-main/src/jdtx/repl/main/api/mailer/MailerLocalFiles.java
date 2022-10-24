package jdtx.repl.main.api.mailer;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.joda.time.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

/**
 * Отправляет и забирает реплики через локальные каталоги.
 */
public class MailerLocalFiles implements IMailer {

    String remoteDir;
    String localDirTmp;


    protected static Log log = LogFactory.getLog("jdtx.MailerLocalFiles");

    private String inFileMask = "*.zip";


    public void init(JSONObject cfg) {
        remoteDir = (String) cfg.get("remoteDir");
        localDirTmp = (String) cfg.get("localDirTmp");
        //
        if (remoteDir == null || remoteDir.length() == 0) {
            throw new XError("Invalid remoteDir");
        }
        if (localDirTmp == null || localDirTmp.length() == 0) {
            throw new XError("Invalid localDirTmp");
        }
        //
        remoteDir = UtFile.unnormPath(remoteDir) + "/";
        localDirTmp = UtFile.unnormPath(localDirTmp) + "/";
        //
        UtFile.mkdirs(remoteDir);
        UtFile.mkdirs(remoteDir + "/from");
        UtFile.mkdirs(remoteDir + "/to");
        UtFile.mkdirs(remoteDir + "/to001");
        UtFile.mkdirs(localDirTmp);
    }

    public long getBoxState(String box) {
        UtFile.mkdirs(remoteDir + box);
        //
        File dir = new File(remoteDir + box);
        File[] files = dir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));

        //
        long idx = 0;
        for (File file : files) {
            if (idx < getNo(file.getName())) {
                idx = getNo(file.getName());
            }
        }

        //
        return idx;
    }

    @Override
    public long getSendDone(String box) throws Exception {
        return 0;
    }

    @Override
    public long getReceiveDone(String box) throws Exception {
        return 0;
    }

    @Override
    public RequiredInfo getSendRequired(String box) throws Exception {
        return null;
    }

    @Override
    public void setSendRequired(String box, RequiredInfo requiredInfo) throws Exception {

    }

    public void send(IReplica replica, String box, long no) throws Exception {
        log.debug("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        // Проверки: правильность полей реплики
        UtJdx.validateReplicaFields(replica);

        // Если почему-то не указан crc файла данных - вычисляем
        if (replica.getData() != null && (replica.getInfo().getCrc() == null || replica.getInfo().getCrc().length() == 0)) {
            log.warn("mailer.send, replica.crc is no set");
            String crcFile = UtJdx.getMd5File(replica.getData());
            replica.getInfo().setCrc(crcFile);
        }


        // Закачиваем
        UtFile.mkdirs(remoteDir + box);
        //
        File localFile = replica.getData();
        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);
        //
        FileUtils.copyFile(localFile, remoteFile);


        // Завершение закачки
        Map data = new HashMap();
        data.put("no", no);
        data.put("crc", replica.getInfo().getCrc());
        data.put("totalBytes", replica.getData().length());
        String dataFileName = remoteDir + box + "/" + getFileNameInfo(no);
        UtFile.saveString(UtJson.toString(data), new File(dataFileName));
    }

    @Override
    public IReplicaInfo getReplicaInfo(String box, long no) throws Exception {
        ReplicaInfo info = new ReplicaInfo();

        String dataFileName = remoteDir + box + "/" + getFileNameInfo(no);
        JSONObject data = (JSONObject) UtJson.toObject(UtFile.loadString(new File(dataFileName)));
        info.setCrc(UtJdxData.stringValueOf(data.get("crc")));
        info.setNo(UtJdxData.longValueOf(data.get("no")));

        return info;
    }

    @Override
    public IReplica receive(String box, long no) throws Exception {
        log.info("mailer.receive, no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        //
        UtFile.mkdirs(remoteDir + box);
        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);
        String localFileName = getFileName(no);
        File localFile = new File(localDirTmp + localFileName);

        //
        if (localFile.exists()) {
            log.debug("localFile.exists: " + localFile.getAbsolutePath());
            localFile.delete();
        }
        FileUtils.copyFile(remoteFile, localFile);

        //
        IReplica replica = new ReplicaFile();
        replica.setData(localFile);

        //
        String dataFileName = remoteDir + box + "/" + getFileNameInfo(no);
        JSONObject data = (JSONObject) UtJson.toObject(UtFile.loadString(new File(dataFileName)));
        replica.getInfo().setCrc(UtJdxData.stringValueOf(data.get("crc")));
        replica.getInfo().setNo(UtJdxData.longValueOf(data.get("no")));

        //
        return replica;
    }

    @Override
    public long delete(String box, long no) throws Exception {
        log.debug("mailer.delete, no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);

        //
        FileUtils.forceDelete(remoteFile);

        //
        return 0;
    }

    @Override
    public long deleteAll(String box, long no) throws Exception {
        //throw new XError("Not implemented");
        return 0;
    }

    @Override
    public void setData(Map data, String name, String box) throws Exception {
        if (data == null) {
            data = new HashMap();
        }
        data.put("dt", new DateTime());

        //
        String dataFileName = remoteDir + box + "/" + name;
        UtFile.saveString(UtJson.toString(data), new File(dataFileName));
    }

    @Override
    public JSONObject getData(String name, String box) throws Exception {
        String dataFileName = remoteDir + box + "/" + name;
        JSONObject data_json = (JSONObject) UtJson.toObject(UtFile.loadString(new File(dataFileName)));
        return data_json;
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }

    String getFileNameInfo(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".info";
    }

    long getNo(String fileName) {
        return Long.parseLong(fileName.substring(0, 9));
    }


}
