package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 *
 */
public class Update_015_016_srv_workstation_state implements ISqlScriptExecutor {

    protected Log log = LogFactory.getLog("jdtx.Update_015_016_srv_workstation_state");

    @Override
    public void exec(Db db) throws Exception {
        // Берем из SRV_WORKSTATION_STATE
        DataStore st = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE");

        // Запишем в SRV_WORKSTATION_STATE_TMP
        db.execSql("delete from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE_TMP");
        String sqlIns = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE_TMP (id, ws_id, param_name, param_value) values (:id, :ws_id, :param_name, :param_value)";
        for (DataRecord rec : st) {
            long wsId = rec.getValueLong("ws_id");
            long id = wsId * 1000;
            for (String param_name : JdxReplSrv.ws_param_names) {
                long param_value = rec.getValueLong(param_name);
                Map values = UtCnv.toMap("id", id, "param_name", param_name, "ws_id", wsId, "param_value", param_value);
                //
                db.execSql(sqlIns, values);
                //
                id = id + 1;
            }
        }
    }


}