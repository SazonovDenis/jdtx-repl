package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class Update_013_014_index implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_013_014_index");

    @Override
    public void exec(Db db) throws Exception {
        IJdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        IJdxDbStruct struct = dbStructReader.readDbStruct();

        //
        UtDbObjectManager objectManager = (UtDbObjectManager) UtDbObjectManager.createInst(db);

        //
        for (IJdxTable table : struct.getTables()) {
            try {
                objectManager.createAuditTableIndex_OPR_DTTM(table);
                log.info("createAuditTableIndex_OPR_DTTM, table: " + table.getName());
            } catch (Exception e) {
                if (UtJdxErrors.collectExceptionText(e).contains("Unknown columns in index")) {
                    log.debug("createAuditTableIndex_OPR_DTTM, table: " + table.getName() + ", message: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else if (UtDbErrors.getInst(db).errorIs_IndexAlreadyExists(e)) {
                    log.debug("createAuditTableIndex_OPR_DTTM, table: " + table.getName() + ", IndexAlreadyExists, message: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else {
                    throw e;
                }
            }
        }
    }

}
