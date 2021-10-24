package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.joda.time.*;
import org.json.simple.*;

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
     * Узнать отмеченный возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE";
        DataRecord rec = db.loadSql(sql).getCurRec();
        return rec.getValueLong("age");
    }

    /**
     * Задать возраст аудита рабочей станции
     */
    public void setAuditAge(long age) throws Exception {
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_STATE set age = " + age;
        db.execSql(sql);
    }

    /**
     * Проверить и зафиксировать изменения общего возраста рабочей станции,
     * основываясь на состоянии таблиц аудита для каждой таблицы.
     * Общий возраст рабочей станции есть отметка о совокупности возрастов всех таблиц.
     */
    public long markAuditAge() throws Exception {
        // Текущий возраст базы данных
        long auditAgeFixed = getAuditAge();
        // Будущий возраст
        long auditAgeNext = auditAgeFixed;

        //
        db.startTran();
        try {
            // Предыдущий записанный возраст аудита для каждой таблицы (максимальный z_id из аудита)
            Map<String, Long> maxIdsFixed = new HashMap<>();
            loadMaxIdsFixed(auditAgeFixed, maxIdsFixed);

            // Текущий актуальный возраст аудита каждой таблицы
            Map<String, Long> maxIdsCurr = new HashMap<>();
            loadMaxIdsCurr(maxIdsCurr);

            // Увеличился ли общий возраст БД (т.е. изменилась ли хоть одна таблица)?
            boolean wasTableChanged = false;
            for (IJdxTable table : struct.getTables()) {
                String tableName = table.getName();
                long maxIdCurr = -1;
                long maxIdFixed = -1;
                if (maxIdsCurr.get(tableName) != null) {
                    maxIdCurr = maxIdsCurr.get(tableName);
                }
                if (maxIdsFixed.get(tableName) != null) {
                    maxIdFixed = maxIdsFixed.get(tableName);
                }
                if (maxIdCurr != maxIdFixed) {
                    wasTableChanged = true;
                    break;
                }
            }

            // Увеличился возраст БД?
            if (wasTableChanged) {
                auditAgeNext = auditAgeNext + 1;
                // Запоминаем состояниния журналов аудита у каждой таблицы для возраста auditAgeNext
                saveMaxIds(auditAgeNext, maxIdsCurr);
                // Запоминаем новый возраст auditAgeNext
                setAuditAge(auditAgeNext);
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        // Возвращаем новый возраст базы
        return auditAgeNext;
    }

    /**
     * Читает текущие id в таблицах аудита (т.е. текущее состояние таблиц аудита)
     */
    private void loadMaxIdsCurr(Map<String, Long> maxIdsCurr) throws Exception {
        for (IJdxTable table : struct.getTables()) {
            if (!UtRepl.tableSkipRepl(table)) {
                String tableName = table.getName();
                DataRecord rec = db.loadSql("select max(" + UtJdx.PREFIX + "id) as maxId from " + UtJdx.AUDIT_TABLE_PREFIX + tableName).getCurRec();
                long maxId = rec.getValueLong("maxId");
                maxIdsCurr.put(tableName, maxId);
            }
        }
    }

    /**
     * Читает сохраненные id в таблицах аудита для возраста auditAge
     *
     * @param auditAge - читаем для этого возраста
     */
    public DateTime loadMaxIdsFixed(long auditAge, Map<String, Long> maxIdsFixed) throws Exception {
        DataRecord rec = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "age where age = " + auditAge).getCurRec();
        //
        byte[] table_idsBytes = (byte[]) rec.getValue("table_ids");
        if (table_idsBytes.length != 0) {
            String table_idsStr = new String(table_idsBytes);
            JSONObject table_ids = (JSONObject) UtJson.toObject(table_idsStr);
            maxIdsFixed.putAll(table_ids);
        }
        //
        return rec.getValueDateTime("dt");
    }

    /**
     * Записываем состояние id в таблицах аудита, для возраста auditAge
     *
     * @param auditAge - записываем для этого возраста
     */
    private void saveMaxIds(long auditAge, Map maxIdsActual) throws Exception {
        DateTime dt = new DateTime();
        String table_ids = UtJson.toString(maxIdsActual);
        String sqlIns = "insert into " + UtJdx.SYS_TABLE_PREFIX + "age(age, dt, table_ids) values (:age, :dt, :table_ids)";
        Map params = UtCnv.toMap(
                "age", auditAge,
                "dt", dt,
                "table_ids", table_ids
        );
        db.execSql(sqlIns, params);
    }


}
