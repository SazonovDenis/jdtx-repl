package jdtx.repl.main.api.mailer;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

/**
 * Отправляет и забирает реплики через локальные каталоги.
 */
public class MailerLocalFiles implements IMailer {

    String remoteDir;
    String localDirTmp;


    protected static Log log = LogFactory.getLog("jdtx.MailerFiles");

    private String inFileMask = "*.zip";


    public void init(JSONObject cfg) {
        remoteDir = (String) cfg.get("mailRemoteDir");
        localDirTmp = (String) cfg.get("mailLocalDirTmp");
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
    public SendRequiredInfo getSendRequired(String box) throws Exception {
        return null;
    }

    @Override
    public void setSendRequired(String box, SendRequiredInfo requiredInfo) throws Exception {

    }

    public void send(IReplica replica, String box, long no) throws Exception {
        log.info("mailer.send, replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        // Проверки: правильность полей реплики
        UtJdx.validateReplicaFields(replica);

        //
        UtFile.mkdirs(remoteDir + box);
        //
        File localFile = replica.getFile();
        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);

        //
        FileUtils.copyFile(localFile, remoteFile);
    }

    @Override
    public IReplicaFileInfo getReplicaInfo(String box, long no) throws Exception {
        ReplicaFileInfo info = new ReplicaFileInfo();

        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);
        info.setCrc(UtJdx.getMd5File(remoteFile));

        return info;
    }

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
        replica.setFile(localFile);

        //
        return replica;
    }

    @Override
    public void delete(String box, long no) throws Exception {
        log.info("mailer.delete, no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);

        //
        FileUtils.forceDelete(remoteFile);
    }


    @Override
    public void setData(Map data, String name, String box) throws Exception {
        //throw new XError("Not implemented");
    }

    @Override
    public JSONObject getData(String name, String box) throws Exception {
        //throw new XError("Not implemented");
        return null;
    }

    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".zip";
    }

    long getNo(String fileName) {
        return Long.valueOf(fileName.substring(0, 9));
    }


}
