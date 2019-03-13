package jdtx.repl.main.api;

import jandcode.utils.UtFile;
import jandcode.utils.UtString;
import jandcode.utils.error.XError;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileFilter;

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
        log.info("mailer.send, wsId: " + repl.getWsId() + ", repl.age: " + repl.getAge() + ", no: " + no + ", remoteDir: " + remoteDir + "/" + box);

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
