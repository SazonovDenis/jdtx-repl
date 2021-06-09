package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import org.json.simple.*;

/**
 * Сохранение конфигураций в БД
 */
public class CfgManager {

    private Db db;

    public CfgManager(Db db) {
        this.db = db;
    }

    public JSONObject getSelfCfg(String cfgName) throws Exception {
        DataStore st = db.loadSql("select " + cfgName + " from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO");

        //
        JSONObject cfg = getCfgFromDataRecord(st.getCurRec(), cfgName);

        //
        return cfg;
    }

    public void setSelfCfg(JSONObject cfg, String cfgName) throws Exception {
        CfgType.validateCfgCode(cfgName);
        //
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO set " + cfgName + " = :cfg", UtCnv.toMap("cfg", cfgStr));
    }

    public void setWsCfg(JSONObject cfg, String cfgName, long wsId) throws Exception {
        CfgType.validateCfgCode(cfgName);
        //
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST set " + cfgName + " = :cfg where id = :id", UtCnv.toMap("cfg", cfgStr, "id", wsId));
    }

    public static JSONObject getCfgFromDataRecord(DataRecord rec, String cfgName) throws Exception {
        byte[] cfgBytes = (byte[]) rec.getValue(cfgName);
        if (cfgBytes.length == 0) {
            return null;
        }

        //
        String cfgStr = new String(cfgBytes);
        JSONObject cfg = UtRepl.loadAndValidateJsonStr(cfgStr);

        //
        return cfg;
    }

}
