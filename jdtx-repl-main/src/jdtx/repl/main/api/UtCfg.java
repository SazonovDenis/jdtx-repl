package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.web.*;
import org.json.simple.*;

/**
 * Сохранение конфигураций в БД
 */
public class UtCfg {

    private Db db;

    public UtCfg(Db db) {
        this.db = db;
    }

    JSONObject getSelfCfg(String cfgName) throws Exception {
        DataStore st = db.loadSql("select " + cfgName + " from " + JdxUtils.sys_table_prefix + "workstation");

        //
        byte[] cfgBytes = (byte[]) st.getCurRec().getValue(cfgName);
        if (cfgBytes.length == 0) {
            return null;
        }

        //
        String cfgStr = new String(cfgBytes);
        JSONObject cfg = (JSONObject) UtJson.toObject(cfgStr);

        //
        return cfg;
    }

    void setSelfCfg(JSONObject cfg, String cfgName) throws Exception {
        UtCfgType.validateCfgCode(cfgName);
        //
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + JdxUtils.sys_table_prefix + "workstation set " + cfgName + " = :cfg", UtCnv.toMap("cfg", cfgStr));
    }

    void setWsCfg(JSONObject cfg, String cfgName, long wsId) throws Exception {
        UtCfgType.validateCfgCode(cfgName);
        //
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + JdxUtils.sys_table_prefix + "workstation_list set " + cfgName + " = :cfg where id = :id", UtCnv.toMap("cfg", cfgStr, "id", wsId));
    }

}
