package jdtx.repl.main.api;


import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.sql.*;
import java.util.*;

public class UtDbObjectManager {

    IJdxDbStruct struct;
    Db db;

    public UtDbObjectManager(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }


    public void createAudit() throws Exception {
        String sql;
        //Statement st = conn.createStatement();
        DbQuery query = null;
        try {
            // ������� tableList �� ������� �������� ������, �� ����������� � ������� ���� �������
            sql = "create table " + JdxUtils.prefix + "tableList(Id int not null, Name varchar(50) default '' not null)";
            db.execSql(sql);
            // ��������� ����
            sql = "alter table " + JdxUtils.prefix + "tableList add constraint pk_" + JdxUtils.prefix + "tableList_id primary key (id)";
            db.execSql(sql);
            // ��������� Id ��� ����� �������
            sql = "create generator " + JdxUtils.gen_pref + "tableList";
            db.execSql(sql);
            sql = "set generator " + JdxUtils.gen_pref + "tableList to 0";
            db.execSql(sql);

            // ������� � ������ ������ ���������
            sql = "create table " + JdxUtils.prefix + "flag_tab(id integer not null, trigger_flag integer not null)";
            db.execSql(sql);
            sql = "insert into " + JdxUtils.prefix + "flag_tab(id, trigger_flag) values (1, 1)";
            db.execSql(sql);


            // ��������� � ��������� ������� �������� ���� ������

            // ������ �� ������� � ������� tableList �������� ���� ������ �� ����
            query = db.createQuery("insert into " + JdxUtils.prefix + "tableList(Id, Name) values (GEN_ID(" + JdxUtils.gen_pref + "tableList, 1), :Name)");
            //
            ArrayList<IJdxTableStruct> tables = struct.getTables();
            for (IJdxTableStruct table : tables) {
                query.setParams(UtCnv.toMap("Name", table.getName()));
                query.execUpdate();
                //pst.setString(1, table.getName());
                //pst.executeUpdate();
                // ������� ������� ������� ��������� ��� ������ �������
                createAuditTable(table.getName());
            }

            // ������� ���������: ��� �������� �������� ����������� ������ � �.�.
            sql = "create table " + JdxUtils.prefix + "state(id integer not null, que_in_receive int not null, que_in_applied int not null, que_in_formed_out int not null, audit_formed_dir_age int not null, audit_formed_out_age int not null)";
            db.execSql(sql);
            sql = "insert into " + JdxUtils.prefix + "state(id, que_in_receive, que_in_applied, que_in_formed_out, audit_formed_dir_age, audit_formed_out_age) values (1, 0, 0, 0, 0, 0)";
            db.execSql(sql);

            // ������ ������� �������
            sql = "create table " + JdxUtils.prefix + "workstation_list(id integer not null, parent integer not null, ws_name varchar(50) not null)";
            db.execSql(sql);
            // ��������� Id ��� ������ ������� �������
            sql = "create generator " + JdxUtils.gen_pref + "workstation_list";
            db.execSql(sql);

            // ��������� ������� �������
            sql = "create table " + JdxUtils.prefix + "workstation_state(id integer not null, que_out_send int not null)";
            db.execSql(sql);
            // ��������� Id ��� ������ ������� �������
            sql = "create generator " + JdxUtils.gen_pref + "workstation_state";
            db.execSql(sql);

            // ��������� �������
            sql = "create table " + JdxUtils.prefix + "que_out(id integer not null, db_code int not null, age int not null)";
            db.execSql(sql);

            // �������� �������
            sql = "create table " + JdxUtils.prefix + "que_in(id integer not null, db_code int not null, age int not null)";
            db.execSql(sql);


            // ������� ��� �������� �������� ����
            sql = "create table " + JdxUtils.prefix + "age(age int not null, tableName varchar(50) default '' not null, " + JdxUtils.prefix + "Id int not null, dt timestamp not null)";
            db.execSql(sql);
        } finally {
            //st.close();
            if (query != null) {
                query.close();
            }
        }
    }

    public void dropAudit() throws SQLException {
        String query;

        // ������� �������: ��� �������� ���������, ������ ������� �������, ��������� � �������� �������, ������� ��� �������� �������� ����, ������� � ������ ������ ���������
        String[] jdx_sys_tables = new String[]{"state", "workstation_list", "que_out", "que_in", "age", "tableList", "flag_tab"};
        for (String jdx_sys_table : jdx_sys_tables) {
            try {
                // ������� ������� ��� �������� ���������
                query = "drop table " + JdxUtils.prefix + jdx_sys_table;
                db.execSql(query);
            } catch (Exception ex) {
                // ���� ��������� ������� �� ����� �������, ��������� ��������� ������
            }
        }

        // ������� ����������: ������� ��� �������� ���������, ������� �� ������� �������� ������, �� ����������� � ������� ���� �������
        String[] jdx_sys_generators = new String[]{"age", "tableList", "workstation_list"};
        for (String jdx_sys_generator : jdx_sys_generators) {
            try {
                query = "drop generator " + JdxUtils.gen_pref + jdx_sys_generator;
                db.execSql(query);
            } catch (Exception ex) {
                // ���� ��������� ��������� �� ����� ������, ��������� ��������� ������
            }
        }

        // ������� ��������� � ������ �������� ������� ������� ���������
        ArrayList<IJdxTableStruct> tables = struct.getTables();
        for (IJdxTableStruct table : tables) {
            dropAuditTable(table.getName());
        }
    }

    private void createAuditTable(String tableName) throws Exception {
        IJdxTableStruct table = struct.getTable(tableName);
        ArrayList<IJdxFieldStruct> fields = table.getFields();
        int fieldCount = fields.size();
        // ��������� ������ �� �������� ������� ������� ���������
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

        String query = "create table " + JdxUtils.prefix + tableName + "(" +
                JdxUtils.prefix + "Id integer not null, " +
                JdxUtils.prefix + "opr_type integer not null, " +
                JdxUtils.prefix + "trigger_flag integer not null, " +
                fieldsStr +
                ")";

        db.execSql(query);

        // ��������� Id ��� ����� �������
        query = "create generator " + JdxUtils.gen_pref + tableName;
        db.execSql(query);
        query = "set generator " + JdxUtils.gen_pref + tableName + " to 0";
        db.execSql(query);

        // ������ �� ������� ������
        query = createTrigger(tableName, fields, updMods.INSERT);
        //System.out.println(query);
        db.execSqlNative(query);
        // ������ �� ���������� ������
        query = createTrigger(tableName, fields, updMods.UPDATE);
        db.execSqlNative(query);
        // ������ �� �������� ������
        query = createTrigger(tableName, fields, updMods.DELETE);
        db.execSqlNative(query);
    }

    private String createTrigger(String tableName, ArrayList<IJdxFieldStruct> fields, updMods upd_mode) {
        String sql;
        int fieldCount = fields.size();
        sql = "create trigger " + JdxUtils.trig_pref + tableName + "_" + upd_mode.toString().substring(0, 1) + " for " + tableName + " after " + upd_mode.toString() + " \n" +
                "as\n" +
                "declare variable trigger_flag_ integer;\n" +
                "begin\n" +
                "  select trigger_flag from " + JdxUtils.prefix + "flag_tab where id=1 into :trigger_flag_;\n" +
                "  if (trigger_flag_<>0) then\n" +
                "  begin\n" +
                "    insert into " + JdxUtils.prefix + tableName + " (" + JdxUtils.prefix + "Id, " + JdxUtils.prefix + "opr_type, " + JdxUtils.prefix + "trigger_flag, ";
        // ������������ ����� ��� ������� � ��� ��������
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + fields.get(i).getName() + ", ";
        }
        sql = sql + fields.get(fieldCount - 1).getName() + ") ";
        // ���� ��������
        sql = sql + "values (GEN_ID(" + JdxUtils.gen_pref + tableName + ", 1), " + (upd_mode.ordinal() + 1) + ", :trigger_flag_, ";
        String contVar = "NEW.";
        if (upd_mode.toString().equals("DELETE")) {
            contVar = "OLD.";
        }
        for (int i = 0; i < fieldCount - 1; i++) {
            sql = sql + contVar + fields.get(i).getName() + ", ";
        }
        sql = sql + contVar + fields.get(fieldCount - 1).getName() + "); \n";
        sql = sql + "  end\nend";
        return sql;
    }

    private void dropAuditTable(String tableName) throws SQLException {
        //Statement st = conn.createStatement();
        String sql;
        try {
            // ������� ��������
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_I";
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� ������, ��������� ��������� ������
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_D";
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� ������, ��������� ��������� ������
        }
        try {
            sql = "drop trigger " + JdxUtils.trig_pref + tableName + "_U";
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� ������, ��������� ��������� ������
        }
        try {
            // ������� ���� ������� ������� ���������
            sql = "drop table " + JdxUtils.prefix + tableName;
            db.execSql(sql);
            // ������� ��������� ��� ������� ������� ���������
            sql = "drop generator " + JdxUtils.gen_pref + tableName;
            db.execSql(sql);
        } catch (Exception ex) {
            // ���� ��������� ������� �� ����� �������, ��������� ��������� ������
        }
    }

    enum updMods {INSERT, UPDATE, DELETE}

}
