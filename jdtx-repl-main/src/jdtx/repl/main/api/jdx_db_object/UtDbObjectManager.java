package jdtx.repl.main.api.jdx_db_object;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

import java.util.*;

public class UtDbObjectManager {

    int CURRENT_VER_DB = 3;

    Db db;

    protected static Log log = LogFactory.getLog("jdtx");

    enum updMods {INSERT, UPDATE, DELETE}


    public UtDbObjectManager(Db db) {
        this.db = db;
    }


    /**
     * Проверяем, что репликация инициализировалась
     */
    public void checkReplDb() throws Exception {
        long ws_id = 0;

        //
        try {
            ws_id = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation").getCurRec().getValueLong("id");
        } catch (Exception e) {
            if (JdxUtils.errorIs_TableNotExists(e)) {
                throw new XError("Replication is not initialized");
            } else {
                throw e;
            }
        }

        //
        if (ws_id == 0) {
            throw new XError("Replication is not initialized");
        }
    }

    public void checkReplVerDb() throws Exception {
        // Версия в исходном состоянии
        int ver = 0;
        int ver_step = 0;

        // Читаем версию БД
        try {
            DataRecord rec = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "verdb where id = 1").getCurRec();
            ver = rec.getValueInt("ver");
            ver_step = rec.getValueInt("ver_step");
        } catch (Exception e) {
            if (JdxUtils.errorIs_TableNotExists(e)) {
                // Создаем таблицу verdb
                log.info("Создаем таблицу " + JdxUtils.sys_table_prefix + "verdb");
                //
                String sql = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectManager.verdb.sql");
                execScript(sql, db);
            } else {
                throw e;
            }
        }

        // Обновляем версию
        int ver_i = ver;
        int ver_to = CURRENT_VER_DB;
        int ver_step_i = ver_step;
        while (ver_i < ver_to) {
            log.info("Смена версии: " + ver_i + "." + ver_step_i + " -> " + (ver_i + 1) + ".0");

            //
            String updateFileName = "update_" + UtString.padLeft(String.valueOf(ver_i), 3, "0") + "_" + UtString.padLeft(String.valueOf(ver_i + 1), 3, "0") + ".sql";
            String sqls = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/" + updateFileName);

            //
            String[] sqlArr = sqls.split(";");
            for (ver_step_i = ver_step; ver_step_i < sqlArr.length; ver_step_i = ver_step_i + 1) {
                sqls = sqlArr[ver_step_i];
                if (sqls.trim().length() == 0) {
                    continue;
                }
                //
                log.info("Смена версии, шаг: " + ver_i + "." + (ver_step_i + 1));
                //
                db.execSql(sqls);
                //
                db.execSql("update " + JdxUtils.sys_table_prefix + "verdb set ver = " + ver_i + ", ver_step = " + (ver_step_i + 1) + " where id = 1");
            }

            //
            ver_i = ver_i + 1;
            //
            db.execSql("update " + JdxUtils.sys_table_prefix + "verdb set ver = " + ver_i + ", ver_step = 0 where id = 1");

            //
            log.info("Смена версии до: " + (ver_i) + ".0 - ok");
        }
    }

    public void createReplBase(long wsId, String guid) throws Exception {
        //
        log.info("createReplBase, ws_id: " + wsId + ", guid: " + guid);

        // Базовая структура (системные структуры)
        String sql = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectManager.sql");
        execScript(sql, db);

        // Обновления базовых структур
        checkReplVerDb();

        // Метка с guid БД и номером wsId
        sql = "update " + JdxUtils.sys_table_prefix + "workstation set id = " + wsId + ", guid = '" + guid + "'";
        db.execSql(sql);
    }


    public void dropAuditBase() throws Exception {
        // Удаляем системные таблицы и генераторы
        String[] jdx_sys_tables = new String[]{
                "db_info", // старая, но тоже удаляем
                "age", "flag_tab", "state", "state_ws", "workstation_list", "workstation",
                "que_in", "que_out", "que_common", "verdb"
        };
        dropSysTables(jdx_sys_tables, db);
    }

    public void createAuditTriggers(IJdxTable table) throws Exception {
        String sql;

        // тригер на вставку записи
        try {
            sql = getSqlCreateTrigger(table, updMods.INSERT);
            db.execSqlNative(sql);
        } catch (Exception e) {
            if (JdxUtils.errorIs_TriggerAlreadyExists(e)) {
                log.warn("createAuditTriggers, trigger " + updMods.INSERT + " already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }

        // тригер на обновление записи
        try {
            sql = getSqlCreateTrigger(table, updMods.UPDATE);
            db.execSqlNative(sql);
        } catch (Exception e) {
            if (JdxUtils.errorIs_TriggerAlreadyExists(e)) {
                log.warn("createAuditTriggers, trigger " + updMods.UPDATE + " already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }

        // тригер на удаление записи
        try {
            sql = getSqlCreateTrigger(table, updMods.DELETE);
            db.execSqlNative(sql);
        } catch (Exception e) {
            if (JdxUtils.errorIs_TriggerAlreadyExists(e)) {
                log.warn("createAuditTriggers, trigger " + updMods.DELETE + " already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }
    }

    private void createAuditTriggers_full(IJdxTable table) throws Exception {
        String sql;

        // тригер на вставку записи в аудит
        sql = createTrigger_full(table, updMods.INSERT);
        db.execSqlNative(sql);

        // тригер на обновление записи
        sql = createTrigger_full(table, updMods.UPDATE);
        db.execSqlNative(sql);

        // тригер на удаление записи
        sql = createTrigger_full(table, updMods.DELETE);
        db.execSqlNative(sql);
    }

    /**
     * Создаем аудит - для таблицы в БД создаем собственную таблицу журнала изменений
     * <p>
     * Допускаем, что аудит уже был создан.
     * Так бывает, если по каким-то причинам следующий блок "выгрузка snapshot" будет прерван,
     * и в итоге реплика не будет помечена как использованная. Тогда ее применение начнется снова,
     * но структуры аудита снова создавать не надо.
     *
     * @param table Имя таблицы
     */
    public void createAuditTable(IJdxTable table) throws Exception {
        // создание таблицы журнала
        String tableName = table.getName();
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        //
        String pkFieldDataType = table.getPrimaryKey().get(0).getDbDatatype();
        if (table.getPrimaryKey().get(0).getJdxDatatype() == JdxDataType.STRING) {
            pkFieldDataType = pkFieldDataType + "(" + table.getPrimaryKey().get(0).getSize() + ")";
        }
        String sql = "create table \n" +
                JdxUtils.audit_table_prefix + tableName + "(\n" +
                JdxUtils.audit_table_prefix + "id integer not null,\n" +
                JdxUtils.audit_table_prefix + "opr_type integer not null,\n" +
                JdxUtils.audit_table_prefix + "opr_dttm timestamp not null,\n" +
                pkFieldName + " " + pkFieldDataType + " not null\n" +
                ")";
        try {
            db.execSql(sql);
        } catch (Exception e) {
            if (JdxUtils.errorIs_TableAlreadyExists(e)) {
                log.warn("createAuditTable, audit table already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }

        // генератор Id для новой таблицы
        try {
            sql = "create generator " + JdxUtils.audit_gen_prefix + tableName;
            db.execSql(sql);
            //
            sql = "set generator " + JdxUtils.audit_gen_prefix + tableName + " to 0";
            db.execSql(sql);
        } catch (Exception e) {
            if (JdxUtils.errorIs_GeneratorAlreadyExists(e)) {
                log.warn("createAuditTable, generator already exists, table: " + table.getName());
            } else {
                throw e;
            }
        }
    }

    private void createAuditTable_full(IJdxTable table) throws Exception {
        String tableName = table.getName();
        List<IJdxField> fields = table.getFields();

        // формируем запрос на создание таблицы журнала изменений
        int fieldCount = fields.size();
        String fieldsStr = "";
        for (int i = 0; i < fieldCount; i++) {
            if (fieldsStr.length() != 0) {
                fieldsStr = fieldsStr + ", ";
            }

            fieldsStr = fieldsStr + fields.get(i).getName() + " " + fields.get(i).getDbDatatype();
            if (JdxDbStructReader.isStringField(fields.get(i).getDbDatatype())) {
                fieldsStr = fieldsStr + "(" + fields.get(i).getSize() + ")";
            }
        }

        String sql = "create table " + JdxUtils.audit_table_prefix + tableName + "(" +
                JdxUtils.audit_table_prefix + "id integer not null, " +
                JdxUtils.audit_table_prefix + "opr_type integer not null, " +
                fieldsStr +
                ")";

        db.execSql(sql);

        // генератор Id для новой таблицы
        sql = "create generator " + JdxUtils.audit_gen_prefix + tableName;
        db.execSql(sql);
        sql = "set generator " + JdxUtils.audit_gen_prefix + tableName + " to 0";
        db.execSql(sql);
    }

    private String getSqlCreateTrigger(IJdxTable table, updMods upd_mode) {
        String sql;
        String tableName = table.getName();
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        sql = "create trigger " + JdxUtils.trig_pref + tableName + "_" + upd_mode.toString().substring(0, 1) + " for " + tableName + " after " + upd_mode.toString() + " \n" +
                "as\n" +
                "declare variable trigger_flag_ integer;\n" +
                "declare variable opr_dttm_ timestamp;\n" +
                "begin\n" +
                "  select trigger_flag from " + JdxUtils.sys_table_prefix + "flag_tab where id=1 into :trigger_flag_;\n" +
                "  select current_timestamp from rdb$database into :opr_dttm_;\n" +
                "  if (trigger_flag_<>0) then\n" +
                "  begin\n" +
                "    insert into " + JdxUtils.audit_table_prefix + tableName + " (" + JdxUtils.audit_table_prefix + "id, " + JdxUtils.audit_table_prefix + "opr_type, " + JdxUtils.audit_table_prefix + "opr_dttm, " + pkFieldName + ") ";
        // значения
        sql = sql + "values (GEN_ID(" + JdxUtils.audit_gen_prefix + tableName + ", 1), " + (upd_mode.ordinal() + 1) + ", :opr_dttm_, ";
        //
        String contVar = "NEW.";
        if (upd_mode.toString().equals("DELETE")) {
            contVar = "OLD.";
        }
        sql = sql + contVar + pkFieldName;
        sql = sql + ");\n";
        //
        sql = sql + "  end\nend";
        return sql;
    }

    private String createTrigger_full(IJdxTable table, updMods upd_mode) {
        String tableName = table.getName();
        String sql;
        int fieldCount = table.getFields().size();
        sql = "create trigger " + JdxUtils.trig_pref + tableName + "_" + upd_mode.toString().substring(0, 1) + " for " + tableName + " after " + upd_mode.toString() + " \n" +
                "as\n" +
                "declare variable trigger_flag_ integer;\n" +
                "begin\n" +
                "  select trigger_flag from " + JdxUtils.sys_table_prefix + "flag_tab where id=1 into :trigger_flag_;\n" +
                "  if (trigger_flag_<>0) then\n" +
                "  begin\n" +
                "    insert into " + JdxUtils.audit_table_prefix + tableName + " (" + JdxUtils.audit_table_prefix + "id, " + JdxUtils.audit_table_prefix + "opr_type, ";
        // перечисление полей для вставки в них значений
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + table.getFields().get(i).getName() + ", ";
        }
        sql = sql + table.getFields().get(fieldCount - 1).getName() + ") ";
        // сами значения
        sql = sql + "values (GEN_ID(" + JdxUtils.audit_gen_prefix + tableName + ", 1), " + (upd_mode.ordinal() + 1) + ", ";
        String contVar = "NEW.";
        if (upd_mode.toString().equals("DELETE")) {
            contVar = "OLD.";
        }
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + contVar + table.getFields().get(i).getName() + ", ";
        }
        sql = sql + contVar + table.getFields().get(fieldCount - 1).getName() + "); \n";
        sql = sql + "  end\nend";
        return sql;
    }

    /**
     * Удаляем аудит (связанную с таблицей таблицу журнала изменений)
     *
     * @param tableName Таблица
     */
    public void dropAudit(String tableName) throws Exception {
        String sql;

        try {
            // удаляем триггеры
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_I";
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (JdxUtils.errorIs_TriggerNotExists(e)) {
                log.debug("dropAudit, audit trigger " + updMods.INSERT + " not exists, table: " + tableName);
            } else {
                throw e;
            }
        }

        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_U";
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (JdxUtils.errorIs_TriggerNotExists(e)) {
                log.debug("dropAudit, audit trigger " + updMods.UPDATE + " not exists, table: " + tableName);
            } else {
                throw e;
            }
        }

        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_D";
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (JdxUtils.errorIs_TriggerNotExists(e)) {
                log.debug("dropAudit, audit trigger " + updMods.DELETE + " not exists, table: " + tableName);
            } else {
                throw e;
            }
        }

        try {
            // удаляем саму таблицу журнала изменений
            sql = "drop table " + JdxUtils.audit_table_prefix + tableName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (JdxUtils.errorIs_TableNotExists(e)) {
                log.debug("dropAudit, audit table not exists, table: " + tableName);
            } else {
                throw e;
            }
        }

        try {
            // удаляем генератор для таблицы журнала изменений
            sql = "drop generator " + JdxUtils.audit_gen_prefix + tableName;
            db.execSql(sql);
        } catch (Exception e) {
            // если удаляемый объект не будет найден, программа продолжит работу
            if (JdxUtils.errorIs_GeneratorNotExists(e)) {
                log.debug("dropAudit, audit generator not exists, table: " + tableName);
            } else {
                throw e;
            }
        }
    }


    /**
     * Утилиты
     */

    static void execScript(String sqls, Db db) throws Exception {
        String[] sqlArr = sqls.split(";");
        for (String sql : sqlArr) {
            if (sql.trim().length() != 0) {
                db.execSql(sql);
            }
        }
    }

    static void dropSysTables(String[] sys_names, Db db) throws Exception {
        // удаляем генераторы
        for (String name : sys_names) {
            try {
                String query = "drop generator " + JdxUtils.sys_gen_prefix + name;
                db.execSql(query);
            } catch (Exception e) {
                // если удаляемый объект не будет найден, программа продолжит работу
                if (JdxUtils.errorIs_GeneratorNotExists(e)) {
                    log.debug("dropSysTables, generator not exists, table: " + name);
                } else {
                    throw e;
                }
            }
        }

        // удаляем таблицу
        for (String name : sys_names) {
            try {
                String query = "drop table " + JdxUtils.sys_table_prefix + name;
                db.execSql(query);
            } catch (Exception e) {
                // если удаляемый объект не будет найден, программа продолжит работу
                if (JdxUtils.errorIs_TableNotExists(e)) {
                    log.debug("dropSysTables, table not exists, table: " + name);
                } else {
                    throw e;
                }
            }
        }
    }

}
