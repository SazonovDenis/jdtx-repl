package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;

public class DbObjectManager_Firebird extends DbGenerators implements IDbGenerators {

    public DbObjectManager_Firebird(Db db) {
        super(db);
    }

    @Override
    public long getNextValue(String generatorName) throws Exception {
        long valueNext;
        DbQuery q = db.openSql("select gen_id(" + generatorName + ", 1) as id from dual");
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
        long valueCurr;
        DbQuery q = db.openSql("select gen_id(" + generatorName + ", 0) as id from dual");
        try {
            valueCurr = q.getValueLong("id");
        } finally {
            q.close();
        }

        //
        return valueCurr;
    }

    @Override
    public void setValue(String generatorName, long value) throws Exception {
        db.execSql("set generator " + generatorName + " to " + value + "");
    }


    @Override
    public void createGenerator(String generatorName) throws Exception {
        try {
            String sql = "create generator " + generatorName;
            db.execSql(sql);
            //
            sql = "set generator " + generatorName + " to 0";
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
            String sql = "drop generator " + generatorName;
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
