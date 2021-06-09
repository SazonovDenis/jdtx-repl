package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import org.apache.commons.logging.*;

public class AppVersionManager {

    private Db db;

    //
    private static Log log = LogFactory.getLog("jdtx.AppVersionManager");


    public AppVersionManager(Db db) {
        this.db = db;
    }

    public String getAppVersionAllowed() throws Exception {
        DataRecord rec = db.loadSql("select app_version_allowed from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO").getCurRec();
        return rec.getValueString("app_version_allowed");
    }

    public void setAppVersionAllowed(String version) throws Exception {
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO set app_version_allowed = :app_version_allowed", UtCnv.toMap("app_version_allowed", version));
        log.info("setAppVersionAllowed: " + version);
    }


}
