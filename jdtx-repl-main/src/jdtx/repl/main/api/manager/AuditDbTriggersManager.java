package jdtx.repl.main.api.manager;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;

public class AuditDbTriggersManager {

    Db db;

    boolean triggersIsOn = true;

    public AuditDbTriggersManager(Db db) {
        this.db = db;
    }

    public boolean triggersIsOn() throws Exception {
        return this.triggersIsOn;
    }

    public void setTriggersOff() throws Exception {
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "flag_tab set trigger_flag=0 where id=1");
        this.triggersIsOn = false;
    }

    public void setTriggersOn() throws Exception {
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "flag_tab set trigger_flag=1 where id=1");
        this.triggersIsOn = true;
    }


}
