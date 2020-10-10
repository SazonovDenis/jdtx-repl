package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

public class UtAppUpdate {

    private Db db;
    private String dataRoot;

    protected static Log log = LogFactory.getLog("jdtx.UtAppUpdate");

    public UtAppUpdate(Db db, String dataRoot) {
        this.db = db;
        this.dataRoot = dataRoot;
    }

    public void checkAppUpdate(boolean doExecUpdate) throws Exception {
        UtAppVersion_DbRW appVersionRW = new UtAppVersion_DbRW(db);
        String appVersionAllowed = appVersionRW.getAppVersionAllowed();
        String appVersionActual = UtRepl.getVersion();
        if (appVersionAllowed.length() == 0) {
            log.debug("appVersionAllowed.length == 0, appVersionActual: " + appVersionActual);
        } else if (appVersionActual.compareToIgnoreCase("SNAPSHOT") == 0) {
            log.warn("appVersionActual == SNAPSHOT, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
        } else if (appVersionAllowed.compareToIgnoreCase(appVersionActual) != 0) {
            log.info("appVersionAllowed != appVersionActual, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
            if (Ut.tryParseInteger(appVersionAllowed) != 0 && Ut.tryParseInteger(appVersionAllowed) < Ut.tryParseInteger(appVersionActual)) {
                // Установлена боле новая версия - не будем обновлять до более старой
                log.warn("appVersionAllowed < appVersionActual, skip application update");
            } else {
                if (doExecUpdate){
                    // Есть более новая версия - будем обновлять
                    doAppUpdate(appVersionAllowed);
                } else {
                    // Установлена старая версия - надо обновлять до новой, а пока не работаем
                    throw new XError("appVersionAllowed != appVersionActual, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
                }
            }
        }
    }

    void doAppUpdate(String appVersionAllowed) throws Exception {
        File exeFile = new File("install/JadatexSync-update-" + appVersionAllowed + ".exe");
        File appDirFile = new File(dataRoot).getParentFile().getParentFile();
        log.info("start app update, exeFile: " + exeFile + ", appDir: " + appDirFile);

        // Запуск обновления
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, exeFile.getAbsolutePath(), "/SILENT", "/repl-service-install", "/DIR=\"" + appDirFile.getAbsolutePath() + "\"");

        //
        if (exitCode != 0) {
            System.out.println("exitCode: " + exitCode);
            for (String outLine : res) {
                System.out.println(outLine);
            }

            //
            throw new XError("Failed to update application, appVersionAllowed: " + appVersionAllowed);
        }
    }

}
