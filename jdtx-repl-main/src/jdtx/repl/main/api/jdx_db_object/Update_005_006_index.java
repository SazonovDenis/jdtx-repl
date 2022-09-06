package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class Update_005_006_index implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_005_006_index");

    @Override
    public void exec(Db db) throws Exception {
        IDbErrors dbErrors = db.getApp().service(DbToolsService.class).getDbErrors(db);

        //
        try {
            db.execSql("CREATE UNIQUE INDEX Z_Z_age_idx ON Z_Z_age (age, table_name)");
            log.info("CREATE INDEX Z_Z_age_idx");
        } catch (Exception e) {
            if (dbErrors.errorIs_IndexAlreadyExists(e)) {
                log.warn("CREATE INDEX Z_Z_age_idx, error: " + e.getMessage().replace("\n", " ").replace("~", ""));
            } else {
                throw e;
            }
        }

        //
        IJdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        IJdxDbStruct struct = dbStructReader.readDbStruct();

        //
        UtDbObjectManager objectManager = (UtDbObjectManager) DbToolsService.getDbObjectManager(db);

        //
        for (IJdxTable table : struct.getTables()) {
            try {
                objectManager.createAuditTableIndex_ID(table);
                log.info("createAuditTableIndex_ID, table: " + table.getName());
            } catch (Exception e) {
                if (UtJdxErrors.collectExceptionText(e).contains("Unknown columns in index")) {
                    log.debug("createAuditTableIndex_ID, table: " + table.getName() + ", message: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else if (dbErrors.errorIs_IndexAlreadyExists(e)) {
                    log.debug("createAuditTableIndex_ID, table: " + table.getName() + ", IndexAlreadyExists, message: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else {
                    throw e;
                }
            }
        }
    }

}
