package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;

public class UtDbObjectDecodeManager {

    Db db = null;

    public UtDbObjectDecodeManager(Db db) {
        this.db = db;
    }

    public void createRefDecodeObject() throws Exception {
        String sql = UtFile.loadString("res:jdtx/repl/main/api/UtDbObjectDecodeManager.sql");
        UtDbObjectManager.execScript(sql, db);
    }

    public void dropRefDecodeObject() throws Exception {
        String[] jdx_sys_tables = new String[]{"decode"};
        UtDbObjectManager.dropAll(jdx_sys_tables, db);
    }

}
