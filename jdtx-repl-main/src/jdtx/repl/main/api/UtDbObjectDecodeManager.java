package jdtx.repl.main.api;

import jandcode.dbm.db.*;

public class UtDbObjectDecodeManager {

    Db db = null;

    public UtDbObjectDecodeManager(Db db) {
        this.db = db;
    }

    public void createRefDecodeObject() throws Exception {
        db.execSql("create table " + JdxUtils.sys_table_prefix + "decode (ws_id integer, table_name varchar(150), ws_slot integer, own_slot integer)");
        db.execSql("create unique index " + JdxUtils.sys_table_prefix + "decode_idx1 on " + JdxUtils.sys_table_prefix + "decode (ws_id, table_name, ws_slot)");
        db.execSql("create unique index " + JdxUtils.sys_table_prefix + "decode_idx2 on " + JdxUtils.sys_table_prefix + "decode (table_name, own_slot)");
    }

    public void dropRefDecodeObject() throws Exception {
        String query = "drop table " + JdxUtils.sys_table_prefix + "decode";
        try {
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
