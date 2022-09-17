package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jandcode.web.*;
import jdtx.repl.main.api.util.*;

import java.util.*;

public class DbGenerators_Oracle extends DbGeneratorsService implements IDbGenerators {

    @Override
    public long genNextValue(String generatorName) throws Exception {
        long valueNext;
        DbQuery q = getDb().openSql("select " + generatorName + ".nextval as id from dual");
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
        // В Oracle в sequence записано будущее значение
        long value;

        // Узнаем INCREMENT_BY
        long incBy = get_SEQUENCE_INCREMENT_BY(generatorName);

        // Возьмем новое значение
        value = genNextValue(generatorName);

        // Узнаем то значение, которое было перед нами
        value = value - incBy;

        // Восстановим то значение, которое было перед нами
        setLastValue(generatorName, value);

        //
        return value;
    }

    long get_SEQUENCE_INCREMENT_BY(String generatorName) throws Exception {
        long value;
        DbQuery q = getDb().openSql("SELECT * FROM user_sequences WHERE sequence_name = '" + generatorName + "'");
        try {
            value = q.getValueLong("INCREMENT_BY");
        } finally {
            q.close();
        }

        //
        return value;
    }

    void createProc() throws Exception {
        OutBuilder builder = new OutBuilder(getApp());
        Map args = new HashMap();
        args.put("SYS_PREFIX", UtJdx.SYS_PREFIX);
        builder.outTml("res:jdtx/repl/main/api/pk_generator/DbGenerators_Oracle.sql.gsp", args, null);
        String sqlCreateProc = builder.toString();
        getDb().execSqlNative(sqlCreateProc);
    }

    @Override
    public void setLastValue(String generatorName, long value) throws Exception {
        createProc();
        //
        getDb().execSqlNative("call " + UtJdx.SYS_PREFIX + "sequence_set_value('" + generatorName + "', " + value + ")");
    }

    @Override
    public void createGenerator(String generatorName) throws Exception {
        try {
            String sql = "create sequence " + generatorName + " minvalue 0 start with 1 increment by 1";
            //String sql = "create sequence " + generatorName + " minvalue 0 start with 1 increment by 2 NOCACHE";
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
            String sql = "drop sequence " + generatorName;
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
