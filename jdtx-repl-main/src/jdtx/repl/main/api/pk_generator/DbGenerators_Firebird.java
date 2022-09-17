package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;

/**
 * Следует учесть, что в Firebird в генераторе записано последнее выданное значение
 */
public class DbGenerators_Firebird extends DbGeneratorsService implements IDbGenerators {

    @Override
    public long genNextValue(String generatorName) throws Exception {
        long valueNext;
        DbQuery q = getDb().openSql("select gen_id(" + generatorName + ", 1) as id from dual");
        try {
            valueNext = q.getValueLong("id");
        } finally {
            q.close();
        }

        //
        return valueNext;
    }

    @Override
    public long getLastValue(String generatorName) throws Exception {
        long valueCurr;
        DbQuery q = getDb().openSql("select gen_id(" + generatorName + ", 0) as id from dual");
        try {
            valueCurr = q.getValueLong("id");
        } finally {
            q.close();
        }

        //
        return valueCurr;
    }

    @Override
    public void setLastValue(String generatorName, long value) throws Exception {
        getDb().execSql("set generator " + generatorName + " to " + value + "");
    }


    @Override
    public void createGenerator(String generatorName) throws Exception {
        try {
            String sql = "create generator " + generatorName;
            getDb().execSql(sql);
            //
            sql = "set generator " + generatorName + " to 0";
            getDb().execSql(sql);
        } catch (Exception e) {
            if (getDbErrors().errorIs_GeneratorAlreadyExists(e)) {
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
            getDb().execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (getDbErrors().errorIs_GeneratorNotExists(e)) {
                log.debug("generator not exists: " + generatorName);
            } else {
                throw e;
            }
        }
    }

}
