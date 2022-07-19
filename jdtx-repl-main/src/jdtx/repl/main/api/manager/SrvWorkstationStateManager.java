package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class SrvWorkstationStateManager {

    private Db db;

    protected static Log log = LogFactory.getLog("jdtx.SrvWorkstationStateManager");

    public SrvWorkstationStateManager(Db db) {
        this.db = db;
    }

    /**
     *
     */
    public void setValue(long wsId, String param_name, long param_value) throws Exception {
        DataRecord rec = loadRec(wsId, param_name);
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set param_value = " + param_value + " where id = " + rec.getValueLong("id");
        db.execSql(sql);
    }

    /**
     *
     */
    public long getValue(long wsId, String param_name) throws Exception {
        DataRecord rec = loadRec(wsId, param_name);
        return rec.getValueLong("param_value");
    }


    private DataRecord loadRec(long wsId, String param_name) throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE where ws_id = " + wsId + " and param_name = '" + param_name + "'";
        DataRecord rec = db.loadSql(sql).getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Не найдена запись для значения в " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE, ws_id: " + wsId + ", param_name: " + param_name);
        }
        return rec;
    }

}
