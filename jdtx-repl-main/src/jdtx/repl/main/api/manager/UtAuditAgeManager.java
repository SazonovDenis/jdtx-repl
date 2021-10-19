package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
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
            Map maxIdsFixed = new HashMap<>();
            loadMaxIdsFixed(auditAgeFixed, maxIdsFixed);

            // Текущий актуальный возраст аудита каждой таблицы
            Map maxIdsCurr = new HashMap<>();
            loadMaxIdsCurr(maxIdsCurr);

            // Увеличился ли общий возраст БД (т.е. изменилась ли хоть одна таблица)?
            boolean wasTableChanged = false;
            for (IJdxTable table : struct.getTables()) {
                String tableName = table.getName();
                long maxIdCurr = -1;
                long maxIdFixed = -1;
                if (maxIdsCurr.get(tableName) != null) {
                    maxIdCurr = (long) maxIdsCurr.get(tableName);
                }
                if (maxIdsFixed.get(tableName) != null) {
                    maxIdFixed = (long) maxIdsFixed.get(tableName);
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
    private void loadMaxIdsCurr(Map maxIdsCurr) throws Exception {
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
    private void loadMaxIdsFixed(long auditAge, Map maxIdsFixed) throws Exception {
        DataStore st = db.loadSql("select table_name, " + UtJdx.PREFIX + "id as maxId from " + UtJdx.SYS_TABLE_PREFIX + "age where age = " + auditAge);
        for (DataRecord rec : st) {
            String tableName = rec.getValueString("table_name");
            long maxId = rec.getValueLong("maxId");
            maxIdsFixed.put(tableName, maxId);
        }
    }

    /**
     * Записываем состояние id в таблицах аудита, для возраста auditAge
     *
     * @param auditAge - записываем для этого возраста
     */
    private void saveMaxIds(long auditAge, Map maxIdsActual) throws Exception {
        DateTime dt = new DateTime();
        String sqlIns = "insert into " + UtJdx.SYS_TABLE_PREFIX + "age(age, table_name, " + UtJdx.PREFIX + "id, dt) values (:age, :table_name, :maxId, :dt)";

        // У каждой таблицы зафиксируем состояние журналов для возраста auditAge
        for (IJdxTable table : struct.getTables()) {
            String tableName = table.getName();
            long maxId = UtJdx.longValueOf(maxIdsActual.get(tableName), 0L);
            //
            db.execSql(sqlIns, UtCnv.toMap("age", auditAge, "table_name", tableName, "maxId", maxId, "dt", dt));
        }

        // Зафиксируем возраст auditAge для таблицы с пустым table_name.
        // Это нужно, если требуется запомнить возраст, а в структуре struct нет ни одной таблицы.
        // Так бывает при процессах первичной инициализации.
        if (struct.getTables().size() == 0) {
            db.execSql(sqlIns, UtCnv.toMap("age", auditAge, "table_name", "", "maxId", 0, "dt", dt));
        }
    }


}
