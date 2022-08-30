package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;

/**
 * Сохранение конфигураций в БД
 */
public class CfgManager {

    private Db db;

    public CfgManager(Db db) {
        this.db = db;
    }


    /*
     * На рабочей станции
     */
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

    /*
     * На сервере по каждой рабочей станции
     */

    public JSONObject getWsCfg(String cfgName, long wsId) throws Exception {
        DataStore st = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST where id = " + wsId);

        //
        if (st.size() == 0) {
            throw new XError("Рабочая станция не найдена: " + wsId);
        }

        //
        JSONObject cfg = getCfgFromDataRecord(st.getCurRec(), cfgName);

        //
        return cfg;
    }

    public void setWsCfg(JSONObject cfg, String cfgName, long wsId) throws Exception {
        CfgType.validateCfgCode(cfgName);
        //
        String cfgStr = UtJson.toString(cfg);
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST set " + cfgName + " = :cfg where id = :id", UtCnv.toMap("cfg", cfgStr, "id", wsId));
    }

    public IJdxDbStruct getWsStruct(long wsId) throws Exception {
        DataStore st = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST where id = " + wsId);

        //
        if (st.size() == 0) {
            throw new XError("Рабочая станция не найдена: " + wsId);
        }

        //
        IJdxDbStruct struct = getStructFromDataRecord(st.getCurRec());

        //
        return struct;
    }

    public void setWsStruct(IJdxDbStruct struct, long wsId) throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        String structStr = struct_rw.toString(struct);
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST set db_struct = :db_struct where id = :id", UtCnv.toMap("db_struct", structStr, "id", wsId));
    }



    /*
     * Private
     */

    private static JSONObject getCfgFromDataRecord(DataRecord rec, String cfgName) throws Exception {
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

    private IJdxDbStruct getStructFromDataRecord(DataRecord rec) throws Exception {
        byte[] structBytes = (byte[]) rec.getValue("db_struct");
        if (structBytes.length == 0) {
            return null;
        }

        //
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        IJdxDbStruct struct = struct_rw.read(structBytes);

        //
        return struct;
    }

}
