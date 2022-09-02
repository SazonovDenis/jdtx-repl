package jdtx.repl.main.api.repair;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.ut.*;
import org.joda.time.*;

import java.io.*;
import java.util.*;

public class JdxRepairLockFileManager {

    String dataRoot;

    public JdxRepairLockFileManager(String dataRoot) {
        this.dataRoot = dataRoot;
    }

    public File getRepairLockFile() {
        File lockFile = new File(dataRoot + "/repairBackup.lock");
        return lockFile;
    }

    public void repairLockFileCreate(Map<String, Object> params) throws Exception {
        File lockFile = getRepairLockFile();

        if (lockFile.exists()) {
            throw new XError("lockFile already exists: " + repairLockFileStr());
        }

        //
        Map<String, String> lockFileMap = new HashMap<>();
        //
        RandomString rnd = new RandomString();
        String guid = rnd.nextHexStr(16);
        String dt = String.valueOf(new DateTime());

        lockFileMap.put("dt", dt);
        lockFileMap.put("guid", guid);

        if (params != null) {
            for (String key : params.keySet()) {
                lockFileMap.put(key, String.valueOf(params.get(key)));
            }
        }

        String lockFileStr = lockFileMap.toString();
        lockFileStr = lockFileStr.substring(1, lockFileStr.length() - 1);
        UtFile.saveString(lockFileStr, lockFile);
    }

    public void repairLockFileDelete() {
        File lockFile = getRepairLockFile();

        if (lockFile.exists() && !lockFile.delete()) {
            throw new XError("Can`t delete lockFile: " + lockFile);
        }
    }

    public String repairLockFileStr() throws Exception {
        File lockFile = getRepairLockFile();
        if (lockFile.exists()) {
            return UtFile.loadString(getRepairLockFile());
        } else {
            return null;
        }
    }

    public Map repairLockFileMap() throws Exception {
        File lockFile = getRepairLockFile();
        if (lockFile.exists()) {
            Map res = new HashMap();
            UtCnv.toMap(res, UtFile.loadString(getRepairLockFile()), ",", "=");
            return res;
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
