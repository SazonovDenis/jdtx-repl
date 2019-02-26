package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;

/**
 * Отправляет и забирает реплики через локальные каталоги.
 */
public class UtMailerLocalFiles implements IJdxMailer {

    String remoteDirSend;
    String remoteDirReceive;
    String localDir;


    protected static Log log = LogFactory.getLog("jdtx");

    private String inFileMask = "*.xml";


    public void init(JSONObject cfg) {
        remoteDirSend = (String) cfg.get("mailSend");
        remoteDirReceive = (String) cfg.get("mailReceive");
        localDir = (String) cfg.get("mailLocalDir");
        //
        if (remoteDirSend == null || remoteDirSend.length() == 0) {
            throw new XError("Invalid remoteDirSend");
        }
        if (remoteDirReceive == null || remoteDirReceive.length() == 0) {
            throw new XError("Invalid remoteDirReceive");
        }
        if (localDir == null || localDir.length() == 0) {
            throw new XError("Invalid localDir");
        }
        //
        remoteDirSend = UtFile.unnormPath(remoteDirSend) + "/";
        remoteDirReceive = UtFile.unnormPath(remoteDirReceive) + "/";
        localDir = UtFile.unnormPath(localDir) + "/";
        //
        UtFile.mkdirs(remoteDirSend);
        UtFile.mkdirs(remoteDirReceive);
        UtFile.mkdirs(localDir);
    }

    public void send(IReplica repl, long n) throws Exception {
        log.info("UtMailer, send n: " + n);

        //
        File localFile = repl.getFile();
        String remoteFileName = getFileName(n);
        File remoteFile = new File(remoteDirSend + remoteFileName);
        //
        FileUtils.copyFile(localFile, remoteFile);
    }

    public IReplica receive(long no) throws Exception {
        log.info("UtMailer, receive no: " + no);

        //
        String remoteFileName = getFileName(no);
        File remoteFile = new File(remoteDirReceive + remoteFileName);
        String localFileName = getFileName(no);
        File localFile = new File(localDir + localFileName);

        //
        FileUtils.copyFile(remoteFile, localFile);

        //
        IReplica replica = new ReplicaFile();
        replica.setFile(localFile);

        //
        ReplicaFile.readReplicaInfo(replica);

        //
        return replica;
    }

    public long getSrvSend() {
        File dir = new File(remoteDirSend);
        File[] files = dir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));

        //
        long age = -1;
        for (File file : files) {
            if (age < getNo(file.getName())) {
                age = getNo(file.getName());
            }
        }

        //
        return age;
    }

    public long getSrvReceive() {
        File dir = new File(remoteDirReceive);
        File[] files = dir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));

        //
        long idx = -1;
        for (File file : files) {
            if (idx < getNo(file.getName())) {
                idx = getNo(file.getName());
            }
        }

        //
        return idx;
    }


    String getFileName(long no) {
        return UtString.padLeft(String.valueOf(no), 9, '0') + ".xml";
    }

    long getNo(String fileName) {
        return Long.valueOf(fileName.substring(0, 9));
    }


}
