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
        setCfgInternal(cfg, "cfg_publications");
    }

    public void setCfgDecode(JSONObject cfg) throws Exception {
        setCfgInternal(cfg, "cfg_decode");
    }

    public void setCfgWs(JSONObject cfg) throws Exception {
        setCfgInternal(cfg, "cfg_ws");
    }

    JSONObject getCfgInternal(String structCode) throws Exception {
        DataStore st = db.loadSql("select " + structCode + " from Z_Z_workstation");

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

    void setCfgInternal(JSONObject cfg, String structCode) throws Exception {
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update Z_Z_workstation set " + structCode + " = :cfg", UtCnv.toMap("cfg", cfgStr));
    }

}
