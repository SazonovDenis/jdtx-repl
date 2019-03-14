package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.joda.time.*;
import org.json.simple.*;

import java.io.*;

/**
 * Отправляет и забирает реплики через локальные каталоги.
 */
public class UtMailerLocalFiles implements IJdxMailer {

    String remoteDir;
    String localDirTmp;


    protected static Log log = LogFactory.getLog("jdtx");

    private String inFileMask = "*.xml";


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

    public long getSrvSate(String box) {
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

    public void send(IReplica repl, long no, String box) throws Exception {
        log.info("mailer.send, repl.wsId: " + repl.getWsId() + ", repl.age: " + repl.getAge() + ", no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        //
        UtFile.mkdirs(remoteDir + box);
        //
        File localFile = repl.getFile();
        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);

        //
        FileUtils.copyFile(localFile, remoteFile);
    }

    public IReplica receive(long no, String box) throws Exception {
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
            log.debug("localFile.exists: "+localFile.getAbsolutePath());
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
    public void delete(long no, String box) throws Exception {
        log.info("mailer.delete, no: " + no + ", remoteDir: " + remoteDir + "/" + box);

        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDir + box + "/" + remoteFileName);

        //
        FileUtils.forceDelete(remoteFile);
    }

    @Override
    public void ping(String box) throws Exception {
        //throw new XError("Not implemented");
    }

    @Override
    public DateTime getPingDt(String box) throws Exception {
        throw new XError("Not implemented");
    }

    @Override
    public JdxReplInfo getInfo(long n, String box) throws Exception {
        JdxReplInfo info = new JdxReplInfo();
        return info;
    }


    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

    long getNo(String fileName) {
        return Long.valueOf(fileName.substring(0, 9));
    }


}
