package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class Update_009_010_age implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_009_010_age");

    @Override
    public void exec(Db db) throws Exception {
        IJdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        //
        UtAuditAgeManager que = new UtAuditAgeManager(db, struct);
        //
        long age = db.loadSql("select max(age) as maxAge from " + UtJdx.SYS_TABLE_PREFIX + "age").getCurRec().getValueLong("maxAge");
        //
        que.setAuditAge(age);
    }

}
