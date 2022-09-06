package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jandcode.web.*;
import jdtx.repl.main.api.util.*;

import java.util.*;

public class UtDbObjectManager_Decode {

    Db db = null;

    public UtDbObjectManager_Decode(Db db) {
        this.db = db;
    }

    public void createDbObject() throws Exception {
        OutBuilder builder = new OutBuilder(db.getApp());
        Map args = new HashMap();
        args.put("SYS_TABLE_PREFIX", UtJdx.SYS_TABLE_PREFIX);
        args.put("SYS_GEN_PREFIX", UtJdx.SYS_GEN_PREFIX);
        builder.outTml("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectDecodeManager." + UtJdx.getDbType(db) + ".sql", args, null);
        String sql = builder.toString();
        //
        JdxDbUtils.execScript(sql, db);
    }

    public void dropDbObject() throws Exception {
        String[] jdx_sys_tables = new String[]{"decode"};
        UtDbObjectManager dbUtils = (UtDbObjectManager) DbToolsService.getDbObjectManager(db);
        dbUtils.dropSysTables(jdx_sys_tables);
    }

}
