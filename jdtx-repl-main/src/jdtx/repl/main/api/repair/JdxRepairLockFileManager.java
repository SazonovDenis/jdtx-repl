package jdtx.repl.main.api.repair;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.ut.*;
import org.joda.time.*;

import java.io.*;
import java.util.*;

public class JdxRepairLockFileManager {

    // todo сделать автосоздание и автоудаление lock на сервере

    String dataRoot;

    public JdxRepairLockFileManager(String dataRoot){
        this.dataRoot = dataRoot;
    }

    public File getRepairLockFile() {
        File lockFile = new File(dataRoot + "temp/repairBackup.lock");
        return lockFile;
    }

    public void repairLockFileCreate() throws Exception {
        File lockFile = getRepairLockFile();

        if (lockFile.exists()) {
            throw new XError("lockFile already exists: " + repairLockFileRead());
        }

        RandomString rnd = new RandomString();
        String guid = rnd.nextHexStr(16);
        String dt = String.valueOf(new DateTime());
        Map lockFileMap = UtCnv.toMap("dt", dt, "guid", guid);
        UtFile.saveString(UtCnv.toString(lockFileMap), lockFile);
    }

    public void repairLockFileDelete() {
        File lockFile = getRepairLockFile();

        if (lockFile.exists() && !lockFile.delete()) {
            throw new XError("Can`t delete lockFile: " + lockFile);
        }
    }

    public String repairLockFileRead() throws Exception {
        File lockFile = getRepairLockFile();
        if (lockFile.exists()) {
            return UtFile.loadString(getRepairLockFile());
        } else {
            return null;
        }
    }

    public String repairLockFileGuid() throws Exception {
        File lockFile = getRepairLockFile();
        if (lockFile.exists()) {
            Map lockFileMap = new HashMap();
            String lockFileStr = UtFile.loadString(getRepairLockFile());
            lockFileStr = lockFileStr.substring(1, lockFileStr.length() - 1);
            UtCnv.toMap(lockFileMap, lockFileStr, ",", "=");
            return (String) lockFileMap.get("guid");
        } else {
            return null;
        }
    }

}
