package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.joda.time.*;

import java.util.*;

/**
 * Работа с возрастом аудита
 */
public class UtAuditAgeManager {

    IJdxDbStruct struct;
    Db db;

    public UtAuditAgeManager(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    /**
     * Запомнить возраст рабочей станции
     */
    public long markAuditAge() throws Exception {
        // возраст базы данных
        long auditAgeFixed = getAuditAge();
        long auditAgeActual = auditAgeFixed;

        // стартуем транзакцию
        db.startTran();

        try {

            // Предыдущий возраст аудита каждой таблицы
            DataStore st = db.loadSql("select table_name, " + JdxUtils.prefix + "id as maxId from " + JdxUtils.sys_table_prefix + "age where age = " + auditAgeFixed);
            Map maxIdsFixed = new HashMap<>();
            for (DataRecord rec : st) {
                String tableName = rec.getValueString("table_name");
                long maxId = rec.getValueLong("maxId");
                maxIdsFixed.put(tableName, maxId);
            }

            // Текущий возраст (максимальный Z_ID) из аудита каждой таблицы
            Map maxIdsCurr = new HashMap<>();
            for (IJdxTableStruct table : struct.getTables()) {
                String tableName = table.getName();
                long maxId = db.loadSql("select max(" + JdxUtils.prefix + "id) as maxId from " + JdxUtils.audit_table_prefix + tableName).getCurRec().getValueLong("maxId");
                maxIdsCurr.put(tableName, maxId);
            }

            // Увелиился ли возраст БД ?
            boolean wasChange = false;
            for (IJdxTableStruct table : struct.getTables()) {
                String tableName = table.getName();
                long maxIdCurr = 0;
                long maxIdFixed = 0;
                if (maxIdsCurr.get(tableName) != null) {
                    maxIdCurr = (long) maxIdsCurr.get(tableName);
                }
                if (maxIdsFixed.get(tableName) != null) {
                    maxIdFixed = (long) maxIdsFixed.get(tableName);
                }
                if (maxIdCurr != maxIdFixed) {
                    wasChange = true;
                    break;
                }
            }

            if (wasChange) {
                auditAgeActual = auditAgeActual + 1;
                // пишем метки
                DateTime dt = new DateTime();
                String sqlIns = "insert into " + JdxUtils.sys_table_prefix + "age(age, table_name, " + JdxUtils.prefix + "id, dt) values (:age, :table_name, :maxId, :dt)";
                for (IJdxTableStruct table : struct.getTables()) {
                    String tableName = table.getName();
                    // максимальный Z_ID из аудита каждой таблицы
                    long maxId = db.loadSql("select max(" + JdxUtils.prefix + "id) as maxId from " + JdxUtils.audit_table_prefix + tableName).getCurRec().getValueLong("maxId");
                    //
                    db.execSql(sqlIns, UtCnv.toMap("age", auditAgeActual, "table_name", tableName, "maxId", maxId, "dt", dt));
                }
            }

            // коммитим транзакцию
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        // возвращаем новый возраст базы
        return auditAgeActual;
    }


    /**
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        return db.loadSql("select max(age) as age from " + JdxUtils.sys_table_prefix + "age").getCurRec().getValueLong("age");
    }

}
