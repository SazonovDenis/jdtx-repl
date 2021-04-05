package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class Update_005_006 implements ISqlScriptExecutor {

    protected static Log log = LogFactory.getLog("jdtx.Update_005_006");

    @Override
    public void exec(Db db) throws Exception {
        //
        try {
            db.execSql("CREATE UNIQUE INDEX Z_Z_age_idx ON Z_Z_age (age, table_name)");
            log.info("CREATE INDEX Z_Z_age_idx");
        } catch (Exception e) {
            if (UtJdx.errorIs_IndexAlreadyExists(e)) {
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
        UtDbObjectManager objectManager = new UtDbObjectManager(db);

        //
        for (IJdxTable table : struct.getTables()) {
            try {
                objectManager.createAuditTableIndex(table);
                log.info("createAuditTableIndex, table: " + table.getName());
            } catch (Exception e) {
                if (UtJdx.collectExceptionText(e).contains("Unknown columns in index")) {
                    log.warn("createAuditTableIndex, table: " + table.getName() + ", error: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else if (UtJdx.errorIs_IndexAlreadyExists(e)) {
                    log.warn("createAuditTableIndex, table: " + table.getName() + ", IndexAlreadyExists, error: " + e.getMessage().replace("\n", " ").replace("~", ""));
                } else {
                    throw e;
                }
            }
        }
    }

}
