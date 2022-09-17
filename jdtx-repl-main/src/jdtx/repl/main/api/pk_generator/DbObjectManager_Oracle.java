package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;

public class DbObjectManager_Oracle extends DbGenerators implements IDbGenerators {

    public DbObjectManager_Oracle(Db db) {
        super(db);
    }

    @Override
    public long getNextValue(String generatorName) throws Exception {
        long valueNext;
        DbQuery q = db.openSql("select " + generatorName + ".nextval as id from dual");
        try {
            valueNext = q.getValueLong("id");
        } finally {
            q.close();
        }

        //
        return valueNext;
    }

    @Override
    public long getValue(String generatorName) throws Exception {
        throw new XError("Not implemented");
    }

    @Override
    public void setValue(String generatorName, long value) throws Exception {
        throw new XError("Not implemented");
    }

    @Override
    public void createGenerator(String generatorName) throws Exception {
        try {
            String sql = "create sequence " + generatorName + " minvalue 0 start with 0 increment by 1";
            db.execSql(sql);
        } catch (Exception e) {
            if (dbErrors.errorIs_GeneratorAlreadyExists(e)) {
                log.warn("generator already exists: " + generatorName);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void dropGenerator(String generatorName) throws Exception {
        try {
            String sql = "drop sequence " + generatorName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (dbErrors.errorIs_GeneratorNotExists(e)) {
                log.debug("generator not exists: " + generatorName);
            } else {
                throw e;
            }
        }
    }

}
