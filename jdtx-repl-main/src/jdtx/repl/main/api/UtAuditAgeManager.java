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
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        return db.loadSql("select max(age) as maxAge from " + JdxUtils.SYS_TABLE_PREFIX + "age").getCurRec().getValueLong("maxAge");
    }

    /**
     * Увеличить общий возраст рабочей станции,
     * не затрагивая и не учитывая возраста аудита таблиц.
     * Полезно, если хочется в исходящую очередь положить реплику, которая не является аудитом
     */
    public long incAuditAge() throws Exception {
        // Текущий возраст базы данных
        long auditAgeFixed = getAuditAge();
        // Будущий возраст
        long auditAgeNext = auditAgeFixed;

        //
        db.startTran();
        try {
            // Скопируем предыдущий записанный возраст аудита для каждой таблицы (максимальный z_id из аудита)
            Map maxIdsFixed = new HashMap<>();
            fillMaxIdsFixed(auditAgeFixed, maxIdsFixed);

            // Увеличиваем возраст БД
            auditAgeNext = auditAgeNext + 1;
            // Перезапоминаем состояниния журналов аудита у каждой таблицы для возраста auditAgeNext
            fillAgeTable(auditAgeNext, maxIdsFixed);

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
     * Проверить и зафиксировать изменения общего возраста рабочей станции,
     * основываясь на возрасте аудита каждой таблицы.
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
            fillMaxIdsFixed(auditAgeFixed, maxIdsFixed);

            // Текущий актуальный возраст аудита каждой таблицы
            Map maxIdsCurr = new HashMap<>();
            fillMaxIdsCurr(maxIdsCurr);

            // Увеличился ли общий возраст БД ?
            boolean wasChange = false;
            for (IJdxTable table : struct.getTables()) {
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
                // Увеличился возраст БД
                auditAgeNext = auditAgeNext + 1;
                // Запоминаем состояниния журналов аудита у каждой таблицы для возраста auditAgeNext
                fillAgeTable(auditAgeNext, maxIdsCurr);
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

    private void fillMaxIdsCurr(Map maxIdsCurr) throws Exception {
        for (IJdxTable table : struct.getTables()) {
            if (!UtRepl.tableSkipRepl(table)) {
                String tableName = table.getName();
                long maxId = db.loadSql("select max(" + JdxUtils.PREFIX + "id) as maxId from " + JdxUtils.AUDIT_TABLE_PREFIX + tableName).getCurRec().getValueLong("maxId");
                maxIdsCurr.put(tableName, maxId);
            }
        }
    }

    private void fillMaxIdsFixed(long auditAgeFixed, Map maxIdsFixed) throws Exception {
        DataStore st = db.loadSql("select table_name, " + JdxUtils.PREFIX + "id as maxId from " + JdxUtils.SYS_TABLE_PREFIX + "age where age = " + auditAgeFixed);
        for (DataRecord rec : st) {
            String tableName = rec.getValueString("table_name");
            long maxId = rec.getValueLong("maxId");
            maxIdsFixed.put(tableName, maxId);
        }
    }

    private void fillAgeTable(long auditAgeActual, Map maxIdsActual) throws Exception {
        DateTime dt = new DateTime();
        String sqlIns = "insert into " + JdxUtils.SYS_TABLE_PREFIX + "age(age, table_name, " + JdxUtils.PREFIX + "id, dt) values (:age, :table_name, :maxId, :dt)";

        // У каждой таблицы зафиксируем состояние журналов для возраста auditAgeActual
        for (IJdxTable table : struct.getTables()) {
            String tableName = table.getName();
            Object maxIdO = maxIdsActual.get(tableName);
            long maxId = 0;
            if (maxIdO != null) {
                maxId = (long) maxIdsActual.get(tableName);
            }
            //
            db.execSql(sqlIns, UtCnv.toMap("age", auditAgeActual, "table_name", tableName, "maxId", maxId, "dt", dt));
        }

        // Зафиксируем возраст auditAgeActual для таблицы с пустым table_name.
        // Это нужно, если требуется запомнить возраст, а в структуре struct нет ни одной таблицы.
        // Так бывает при процессах первичной инициализации.
        if (struct.getTables().size() == 0) {
            db.execSql(sqlIns, UtCnv.toMap("age", auditAgeActual, "table_name", "", "maxId", 0, "dt", dt));
        }
    }


}
