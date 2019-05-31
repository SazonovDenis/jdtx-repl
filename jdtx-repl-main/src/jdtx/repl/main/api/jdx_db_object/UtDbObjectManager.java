package jdtx.repl.main.api.jdx_db_object;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

import java.sql.*;
import java.util.*;

public class UtDbObjectManager {

    int CURRENT_VER_DB = 5;

    IJdxDbStruct struct;
    Db db;

    protected static Log log = LogFactory.getLog("jdtx");

    enum updMods {INSERT, UPDATE, DELETE}


    public UtDbObjectManager(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    public void checkReplVerDb() throws Exception {

        // Проверяем, что репликация инициализировалась
        try {
            db.loadSql("select * from " + JdxUtils.sys_table_prefix + "db_info");
        } catch (Exception e) {
            if (e.getCause().getMessage().contains("Table unknown")) {
                throw new XError("Replication is not initialized");
            }
        }

        // Версия в исходном состоянии
        int ver = 0;
        int ver_step = 0;

        // Читаем версию БД
        try {
            DataRecord rec = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "verdb where id = 1").getCurRec();
            ver = rec.getValueInt("ver");
            ver_step = rec.getValueInt("ver_step");
        } catch (Exception e) {
            if (e.getCause().getMessage().contains("Table unknown")) {
                // Создаем таблицу verdb
                log.info("Создаем таблицу " + JdxUtils.sys_table_prefix + "verdb");
                //
                String sql = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectManager.verdb.sql");
                execScript(sql, db);
            }
        }

        // Обновляем версию
        int ver_i = ver;
        long ver_to = CURRENT_VER_DB;
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

    public void createRepl(long wsId, String guid) throws Exception {
        String sql;

        //
        log.info("createRepl - системные таблицы");

        // Начальная структура
        sql = UtFile.loadString("res:jdtx/repl/main/api/jdx_db_object/UtDbObjectManager.sql");
        execScript(sql, db);

        // Обновления структуры
        checkReplVerDb();


        // Создаем для каждой таблицы в БД собственную таблицу журнала изменений
        log.info("createRepl - таблицы аудита данных");

        //
        dbStructUpdate();

        // метка с номером БД
        sql = "update " + JdxUtils.sys_table_prefix + "db_info set ws_id = " + wsId + ", guid = '" + guid + "'";
        db.execSql(sql);

        //
        log.info("db_info, ws_id: " + wsId);
    }

    public void dbStructUpdate() throws Exception {
        log.info("dbStructUpdate");

        //
        UtDbStruct_DbRW dbStructRW = new UtDbStruct_DbRW(db);
        IJdxDbStruct structFixed = dbStructRW.getDbStructFixed();
        IJdxDbStruct structAllowed = dbStructRW.getDbStructAllowed();
        IJdxDbStruct structActual = struct;
        //
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();

        // Реальная отличается от зафиксированной?
        if (!UtDbComparer.dbStructIsEqual(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved)) {
            // Реальная совпадет с утвержденной?
            if (UtDbComparer.dbStructIsEqual(structActual, structAllowed)) {
                // Подгоняем структуру аудита под реальную структуру
                db.startTran();
                try {
                    long n;

                    // Удаляем аудит (связанную с каждой таблицей таблицу журнала изменений)
                    ArrayList<IJdxTableStruct> tablesRemoved = structDiffRemoved.getTables();
                    n = 0;
                    for (IJdxTableStruct table : tablesRemoved) {
                        n++;
                        log.debug("dropAudit " + n + "/" + tablesRemoved.size() + " " + table.getName());
                        //
                        dropAuditTable(table.getName());
                    }


                    // Создаем аудит (для каждой таблицы в БД собственную таблицу журнала изменений (обеспечиваем порядок сортировки таблиц с учетом foreign key))
                    List<IJdxTableStruct> tablesNew = JdxUtils.sortTables(structDiffNew.getTables());
                    n = 0;
                    for (IJdxTableStruct table : tablesNew) {
                        n++;
                        log.debug("createRepl, createAudit " + n + "/" + tablesNew.size() + " " + table.getName());

                        if (!UtRepl.tableSkipRepl(table)) {
                            // создание таблицы журнала аудита
                            createAuditTable(table);

                            // создание тригеров на изменение
                            createAuditTriggers(table);
                        } else {
                            log.debug("createRepl, skip createAudit, table: " + table.getName());
                        }
                    }


                    // Выгрузка snapshot
                    n = 0;
                    for (IJdxTableStruct table : tablesNew) {
                        n++;
                        if (!UtRepl.tableSkipRepl(table)) {
                            createTableSnapshot(table);
                        } else {
                            log.debug("createRepl, skip createAudit, table: " + table.getName());
                        }
                    }


                    // Запоминаем текущую структуру как фиксированную
                    //
                    //
                    //

                    //
                    db.commit();

                } catch (Exception e) {
                    db.rollback(e);
                    throw e;
                }

            }
        }
    }


    public void dropAudit() throws Exception {
        log.info("dropAudit - журналы");

        // Удаляем связанную с каждой таблицей таблицу журнала изменений
        ArrayList<IJdxTableStruct> tables = struct.getTables();
        long n = 0;
        for (IJdxTableStruct table : tables) {
            n++;
            log.debug("dropAudit " + n + "/" + tables.size() + " " + table.getName());
            //
            dropAuditTable(table.getName());
        }

        log.info("dropAudit - системные объекты");

        // Удаляем системные таблицы и генераторы
        String[] jdx_sys_tables = new String[]{
                "age", "flag_tab", "state", "state_ws", "workstation_list", "db_info",
                "que_in", "que_out", "que_common", "verdb"
        };
        dropAll(jdx_sys_tables, db);
    }

    private void createAuditTriggers(IJdxTableStruct table) throws Exception {
        String sql;

        // тригер на вставку записи в аудит
        sql = createTrigger(table, updMods.INSERT);
        db.execSqlNative(sql);

        // тригер на обновление записи
        sql = createTrigger(table, updMods.UPDATE);
        db.execSqlNative(sql);

        // тригер на удаление записи
        sql = createTrigger(table, updMods.DELETE);
        db.execSqlNative(sql);
    }

    private void createAuditTriggers_full(IJdxTableStruct table) throws Exception {
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

    private void createAuditTable(IJdxTableStruct table) throws Exception {
        String tableName = table.getName();
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        //
        String pkFieldDataType = table.getPrimaryKey().get(0).getDbDatatype();
        if (table.getPrimaryKey().get(0).getJdxDatatype() == JdxDataType.STRING) {
            pkFieldDataType = pkFieldDataType + "(" + table.getPrimaryKey().get(0).getSize() + ")";
        }
        //
        String sql = "create table \n" +
                JdxUtils.audit_table_prefix + tableName + "(\n" +
                JdxUtils.audit_table_prefix + "id integer not null,\n" +
                JdxUtils.audit_table_prefix + "opr_type integer not null,\n" +
                JdxUtils.audit_table_prefix + "opr_dttm timestamp not null,\n" +
                pkFieldName + " " + pkFieldDataType + " not null\n" +
                ")";
        db.execSql(sql);

        // генератор Id для новой таблицы
        sql = "create generator " + JdxUtils.audit_gen_prefix + tableName;
        db.execSql(sql);
        sql = "set generator " + JdxUtils.audit_gen_prefix + tableName + " to 0";
        db.execSql(sql);
    }

    private void createAuditTable_full(IJdxTableStruct table) throws Exception {
        String tableName = table.getName();
        List<IJdxFieldStruct> fields = table.getFields();

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

    private String createTrigger(IJdxTableStruct table, updMods upd_mode) {
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

    private String createTrigger_full(IJdxTableStruct table, updMods upd_mode) {
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

    private void dropAuditTable(String tableName) throws SQLException {
        //Statement st = conn.createStatement();
        String sql;
        try {
            // удаляем триггеры
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_I";
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемый триггер не будет найден, программа продолжит работу
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_D";
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемый триггер не будет найден, программа продолжит работу
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_U";
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемый триггер не будет найден, программа продолжит работу
        }
        try {
            // удаляем саму таблицу журнала изменений
            sql = "drop table " + JdxUtils.audit_table_prefix + tableName;
            db.execSql(sql);
            // удаляем генератор для таблицы журнала изменений
            sql = "drop generator " + JdxUtils.audit_gen_prefix + tableName;
            db.execSql(sql);
        } catch (Exception ex) {
            // если удаляемая таблица не будет найдена, программа продолжит работу
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

    static void dropAll(String[] sys_names, Db db) {
        // удаляем генераторы
        for (String jdx_sys_generator : sys_names) {
            try {
                String query = "drop generator " + JdxUtils.sys_gen_prefix + jdx_sys_generator;
                db.execSql(query);
            } catch (Exception e) {
                // если удаляемый объект не будет найден, программа продолжит работу
                if (!e.getCause().toString().contains("Generator not found")) {
                    System.out.println(e.getMessage());
                    System.out.println(e.getCause().toString());
                }
            }
        }

        // удаляем таблицу
        for (String jdx_sys_table : sys_names) {
            try {
                String query = "drop table " + JdxUtils.sys_table_prefix + jdx_sys_table;
                db.execSql(query);
            } catch (Exception e) {
                // если удаляемый объект не будет найден, программа продолжит работу
                if (!e.getCause().toString().contains("does not exist")) {
                    System.out.println(e.getMessage());
                    System.out.println(e.getCause().toString());
                }
            }
        }
    }

}
