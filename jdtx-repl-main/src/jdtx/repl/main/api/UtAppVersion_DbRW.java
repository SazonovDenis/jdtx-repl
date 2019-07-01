package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;

public class UtAppVersion_DbRW {

    private Db db;

    public UtAppVersion_DbRW(Db db) {
        this.db = db;
    }


    public String getAppVersionAllowed() throws Exception {
        DataRecord rec = db.loadSql("select app_version_allowed from Z_Z_state where id = 1").getCurRec();
        return rec.getValueString("app_version_allowed");
    }

    public void setAppVersionAllowed(String version) throws Exception {
        db.execSql("update Z_Z_state set app_version_allowed = :app_version_allowed where id = 1", UtCnv.toMap("app_version_allowed", version));
    }


}
