package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.web.*;
import org.apache.commons.logging.*;
import org.joda.time.*;

import java.util.*;

/**
 * Смена способа хранения возрастов:
 * Возраста храним не по строке на каждую таблицу, а в одной строке (в blob)
 */
public class Update_011_012_fill_age implements ISqlScriptExecutor {

    protected Log log = LogFactory.getLog("jdtx.Update_011_012_fill_age");

    @Override
    public void exec(Db db) throws Exception {
        db.execSql("delete from Z_Z_AGE_TMP");

        //
        DataStore st = db.loadSql("select age, dt, count(*) as cnt from Z_Z_AGE group by age, dt order by age");

        //
        for (DataRecord rec : st) {
            Map maxIdsFixed = new HashMap<>();
            long age = rec.getValueLong("age");
            DateTime dt = rec.getValueDateTime("dt");
            loadMaxIds_ver011(db, age, maxIdsFixed);
            saveMaxIds_ver012(db, age, dt, maxIdsFixed);
        }
    }

    /**
     * Читает сохраненные id в таблицах аудита для возраста auditAge
     *
     * @param auditAge - читаем для этого возраста
     */
    private void loadMaxIds_ver011(Db db, long auditAge, Map maxIdsFixed) throws Exception {
        DataStore st = db.loadSql("select table_name, z_id as maxId from Z_Z_AGE where age = " + auditAge);
        for (DataRecord rec : st) {
            String tableName = rec.getValueString("table_name");
            long maxId = rec.getValueLong("maxId");
            maxIdsFixed.put(tableName, maxId);
        }
    }


    private void saveMaxIds_ver012(Db db, long auditAge, DateTime dt, Map maxIdsActual) throws Exception {
        String sql = "insert into Z_Z_AGE_TMP(age, dt, table_ids) values (:age, :dt, :table_ids)";
        String table_ids = UtJson.toString(maxIdsActual);
        Map params = UtCnv.toMap(
                "age", auditAge,
                "dt", dt,
                "table_ids", table_ids
        );
        db.execSql(sql, params);
    }


}