package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.joda.time.*;

/**
 * Работа с возрастом аудита
 */
public class UtAuditManager {

    IJdxDbStruct struct;
    Db db;

    public UtAuditManager(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    /**
     * Запомнить возраст рабочей станции
     * todo: оптимизация лишних фиксаций, если не было реальных изменений
     */
    public long markAuditAge() throws Exception {
        String query = "insert into " + JdxUtils.sys_table_prefix + "age(age, tableName, " + JdxUtils.prefix + "id, dt) values (:age, :tableName, :maxId, :dt)";

        // возраст базы данных
        long age = getAuditAge() + 1;

        // текущая дата
        DateTime dt = new DateTime();

        // стартуем транзакцию
        db.startTran();
        // пишем метки
        try {
            for (IJdxTableStruct table : struct.getTables()) {
                String tableName = table.getName();
                // максимальный ID из таблицы журнала изменений
                long maxId = db.loadSql("select max(" + JdxUtils.prefix + "id) as maxId from " + JdxUtils.audit_table_prefix + tableName).getCurRec().getValueLong("maxId");
                //
                db.execSql(query, UtCnv.toMap("age", age, "tableName", tableName, "maxId", maxId, "dt", dt));
            }
            // коммитим транзакцию
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
        }

        // возвращаем новый возраст базы
        return age;
    }


    /**
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        return db.loadSql("select max(age) as age from " + JdxUtils.sys_table_prefix + "age").getCurRec().getValueLong("age");
    }

}
