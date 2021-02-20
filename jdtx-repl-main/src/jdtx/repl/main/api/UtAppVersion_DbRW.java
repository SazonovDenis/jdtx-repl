package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import org.apache.commons.logging.*;

public class UtAppVersion_DbRW {

    private Db db;

    //
    private static Log log = LogFactory.getLog("jdtx.UtAppVersion_DbRW");


    public UtAppVersion_DbRW(Db db) {
        this.db = db;
    }

    public String getAppVersionAllowed() throws Exception {
        DataRecord rec = db.loadSql("select app_version_allowed from " + JdxUtils.SYS_TABLE_PREFIX + "workstation").getCurRec();
        return rec.getValueString("app_version_allowed");
    }

    public void setAppVersionAllowed(String version) throws Exception {
        db.execSql("update " + JdxUtils.SYS_TABLE_PREFIX + "workstation set app_version_allowed = :app_version_allowed", UtCnv.toMap("app_version_allowed", version));
        log.info("setAppVersionAllowed: " + version);
    }


}
