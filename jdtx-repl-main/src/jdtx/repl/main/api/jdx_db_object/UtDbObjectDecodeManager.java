package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.web.*;
import jdtx.repl.main.api.util.*;

import java.util.*;

public class UtDbObjectDecodeManager {

    Db db = null;

    public UtDbObjectDecodeManager(Db db) {
        this.db = db;
    }

    public void createDbObject() throws Exception {
        //String sql = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectDecodeManager.sql");
        OutBuilder builder = new OutBuilder(db.getApp());
        Map args = new HashMap();
        args.put("SYS_TABLE_PREFIX", UtJdx.SYS_TABLE_PREFIX);
        args.put("SYS_GEN_PREFIX", UtJdx.SYS_GEN_PREFIX);
        builder.outTml("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectDecodeManager.oracle.sql", args, null);
        String sql = builder.toString();
        JdxDbUtils.execScript(sql, db);
    }

    public void dropDbObject() throws Exception {
        String[] jdx_sys_tables = new String[]{"decode"};
        UtDbObjectManager dbObjectManager = (UtDbObjectManager) UtDbObjectManager.createInst(db);
        dbObjectManager.dropSysTables(jdx_sys_tables);
    }

}
