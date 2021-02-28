package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class Update_005_006 implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_005_006");

    @Override
    public void exec(Db db) throws Exception {
        IJdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        IJdxDbStruct struct = dbStructReader.readDbStruct();

        //
        UtDbObjectManager objectManager = new UtDbObjectManager(db);

        //
        for (IJdxTable table : struct.getTables()) {
            try {
                objectManager.createAuditTableIndex(table);
                log.info("createAuditTableIndex, table: " + table.getName());
            } catch (Exception e) {
                if (UtJdx.collectExceptionText(e).contains("Unknown columns in index")) {
                    log.warn("createAuditTableIndex, table: " + table.getName() + ", error: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else {
                    throw e;
                }
            }
        }
    }

}
