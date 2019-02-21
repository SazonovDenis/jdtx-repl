package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import org.apache.commons.io.*;

import java.io.*;

/**
 *
 */
public class UtMailer {

    String localDir;
    String remoteDir;

    Db db;


    public UtMailer(Db db) {
        this.db = db;
    }

    public void send() throws Exception {
        // Узнаем сколько уже получено на сервере
        long sendAge = getSrvSendAge();

        // Узнаем сколько есть у нас
        long selfAge = getSelfAuditAge();

        long age = sendAge;
        while (age <= selfAge) {
            String localFileName = genFileName(age);
            File localFile = new File(localDir + localFileName);

            String remoteFileName = genFileName(age);
            File remoteFile = new File(remoteDir + remoteFileName);
            FileUtils.copyFile(localFile, remoteFile);
            age = age + 1;
        }

    }

    public long getSrvSendAge() {
        return 0;
    }

    public void receive() throws IOException {
        FileUtils.copyFile(new File(remoteDir), new File(localDir));
    }

    public long getSelfAuditAge() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, null);
        return ut.getAuditAge();
    }

    String genFileName(long age) {
        return UtString.padLeft(String.valueOf(age), 9, '0') + ".xml";
    }


}
