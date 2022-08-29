package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;

public class DbObjectManager_Oracle extends UtDbObjectManager {

    public DbObjectManager_Oracle(Db db) {
        super(db);
    }

    @Override
    void createGenerator(String generatorName) throws Exception {
        try {
            String sql = "create sequence " + generatorName + " minvalue 0 start with 0 increment by 1";
            db.execSql(sql);
        } catch (Exception e) {
            if (UtDbErrors.getInst(db).errorIs_GeneratorAlreadyExists(e)) {
                log.warn("generator already exists: " + generatorName);
            } else {
                throw e;
            }
        }
    }

    @Override
    void dropGenerator(String generatorName) throws Exception {
        try {
            String sql = "drop sequence " + generatorName;
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
