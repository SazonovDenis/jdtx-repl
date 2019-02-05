package jdtx.repl.main.api;

import jandcode.dbm.db.*;

public class DbAuditTriggersManager {

    Db db;

    boolean triggersIsOn = true;

    public DbAuditTriggersManager(Db db) {
        this.db = db;
    }

    public boolean triggersIsOn() throws Exception {
        return this.triggersIsOn;
    }

    public void setTriggersOff() throws Exception {
        db.execSql("update " + JdxUtils.sys_table_prefix + "flag_tab set trigger_flag=0 where id=1");
        this.triggersIsOn = false;
    }

    public void setTriggersOn() throws Exception {
        db.execSql("update " + JdxUtils.sys_table_prefix + "flag_tab set trigger_flag=1 where id=1");
        this.triggersIsOn = true;
    }


}
