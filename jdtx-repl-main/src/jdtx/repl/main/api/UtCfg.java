package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
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


    // =========================================
    // Z_Z_workstation
    // =========================================

    public JSONObject getCfgPublications() throws Exception {
        return getCfgInternal("cfg_publications");
    }

    public JSONObject getCfgDecode() throws Exception {
        return getCfgInternal("cfg_decode");
    }

    public JSONObject getCfgWs() throws Exception {
        return getCfgInternal("cfg_ws");
    }

    public void setCfgPublications(JSONObject cfg) throws Exception {
        setCfgSelfInternal(cfg, "cfg_publications");
    }

    public void setCfgDecode(JSONObject cfg) throws Exception {
        setCfgSelfInternal(cfg, "cfg_decode");
    }

    public void setCfgWs(JSONObject cfg) throws Exception {
        setCfgSelfInternal(cfg, "cfg_ws");
    }

    public void setCfg(JSONObject cfg, String cfgType, long wsId) throws Exception {
        switch (cfgType) {
            case "cfg_publications": {
                setCfgPublications(cfg, wsId);
                break;
            }
            case "cfg_decode": {
                setCfgDecode(cfg, wsId);
                break;
            }
            case "cfg_ws": {
                setCfgWs(cfg, wsId);
                break;
            }
            default: {
                throw new XError("Unknown cfg type: " + cfgType);
            }
        }
    }


    // =========================================
    // Z_Z_workstation_ws
    // =========================================

    public void setCfgPublications(JSONObject cfg, long wsId) throws Exception {
        setCfgWsInternal(cfg, "cfg_publications", wsId);
    }

    public void setCfgDecode(JSONObject cfg, long wsId) throws Exception {
        setCfgWsInternal(cfg, "cfg_decode", wsId);
    }

    public void setCfgWs(JSONObject cfg, long wsId) throws Exception {
        setCfgWsInternal(cfg, "cfg_ws", wsId);
    }


    // =========================================
    // Internal
    // =========================================

    JSONObject getCfgInternal(String structCode) throws Exception {
        DataStore st = db.loadSql("select " + structCode + " from " + JdxUtils.sys_table_prefix + "workstation");

        //
        byte[] cfgBytes = (byte[]) st.getCurRec().getValue(structCode);
        if (cfgBytes.length == 0) {
            return null;
        }
        String cfgStr = new String(cfgBytes);
        JSONObject cfg = (JSONObject) UtJson.toObject(cfgStr);

        //
        return cfg;
    }

    void setCfgSelfInternal(JSONObject cfg, String cfgCode) throws Exception {
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + JdxUtils.sys_table_prefix + "workstation set " + cfgCode + " = :cfg", UtCnv.toMap("cfg", cfgStr));
    }

    private void setCfgWsInternal(JSONObject cfg, String cfgCode, long wsId) throws Exception {
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + JdxUtils.sys_table_prefix + "workstation_list set " + cfgCode + " = :cfg where id = :id", UtCnv.toMap("cfg", cfgStr, "id", wsId));
    }

}
