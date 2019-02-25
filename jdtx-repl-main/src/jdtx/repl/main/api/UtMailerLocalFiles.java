package jdtx.repl.main.api;

import jandcode.utils.*;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;

/**
 * Отправляет и забирает реплики через локальные каталоги.
 */
public class UtMailerLocalFiles implements IJdxMailer {

    private String localDirQueOut;
    private String remoteDirQueOut;

    private String localDirQueIn;
    private String remoteDirQueIn;

    private IJdxQuePersonal queOut;
    private IJdxQueCommon queIn;

    protected static Log log = LogFactory.getLog("jdtx");

    private String inFileMask = "*.xml";

    public UtMailerLocalFiles(IJdxQueCommon queIn, IJdxQuePersonal queOut) {
        this.queIn = queIn;
        this.queOut = queOut;
    }

    public void init(JSONObject cfg) {
        localDirQueOut = queOut.getBaseDir();
        JSONObject cfgOut = (JSONObject) cfg.get("queOut_DirRemote");
        remoteDirQueOut = UtFile.unnormPath((String) cfgOut.get("directory")) + "/";
        //
        localDirQueIn = queIn.getBaseDir();
        JSONObject cfgIn = (JSONObject) cfg.get("queIn_DirRemote");
        remoteDirQueIn = UtFile.unnormPath((String) cfgIn.get("directory")) + "/";
        //
        UtFile.mkdirs(localDirQueIn);
        UtFile.mkdirs(localDirQueOut);
        UtFile.mkdirs(remoteDirQueIn);
        UtFile.mkdirs(remoteDirQueOut);
    }

    public void send() throws Exception {
        // Узнаем сколько уже отправлено на сервер
        long srvSendAge = getSrvSendAge();

        // Узнаем сколько есть у нас в очереди на отправку
        long selfOutAge = queOut.getMaxAge();

        //
        log.info("UtMailer, send.age: " + srvSendAge + ", que.age: " + selfOutAge);

        //
        long n = 0;
        for (long age = srvSendAge + 1; age <= selfOutAge; age++) {
            log.info("UtMailer, send age: " + age);

            //
            String localFileName = getFileName(age);
            File localFile = new File(localDirQueOut + localFileName);
            String remoteFileName = getFileName(age);
            File remoteFile = new File(remoteDirQueOut + remoteFileName);
            //
            FileUtils.copyFile(localFile, remoteFile);
            //
            age = age + 1;

            //
            n++;
        }

        //
        log.info("UtMailer, send done: " + n + ", age: " + selfOutAge);
    }

    public void receive() throws Exception {
        // Узнаем сколько есть на сервере
        long srvAvailableNo = getSrvMaxIdx();

        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        //
        log.info("UtMailer, srv.available: " + srvAvailableNo + ", self.received: " + selfReceivedNo);

        //
        long n = 0;
        for (long no = selfReceivedNo + 1; no <= srvAvailableNo; no++) {
            log.info("UtMailer, receive no: " + no);

            //
            String remoteFileName = getFileName(no);
            File remoteFile = new File(remoteDirQueIn + remoteFileName);
            String localFileName = getFileName(no);
            File localFile = new File(localDirQueIn + localFileName);

            //
            FileUtils.copyFile(remoteFile, localFile);

            //
            n++;
        }

        //
        log.info("UtMailer, receive done: " + n + ", no: " + srvAvailableNo);
    }

    protected long getSrvSendAge() {
        File dir = new File(remoteDirQueOut);
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

    protected long getSrvMaxIdx() {
        File dir = new File(remoteDirQueIn);
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
