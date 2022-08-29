package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;

public class DbObjectManager_Firebird extends UtDbObjectManager {

    public DbObjectManager_Firebird(Db db) {
        super(db);
    }

    void createGenerator(String generatorName) throws Exception {
        try {
            String sql = "create generator " + generatorName;
            db.execSql(sql);
            //
            sql = "set generator " + generatorName + " to 0";
            db.execSql(sql);
        } catch (Exception e) {
            if (UtDbErrors.getInst(db).errorIs_GeneratorAlreadyExists(e)) {
                log.warn("generator already exists: " + generatorName);
            } else {
                throw e;
            }
        }
    }

    void dropGenerator(String generatorName) throws Exception {
        try {
            String sql = "drop generator " + generatorName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (UtDbErrors.getInst(db).errorIs_GeneratorNotExists(e)) {
                log.debug("generator not exists: " + generatorName);
            } else {
                throw e;
            }
        }
    }
}
