package jdtx.repl.main.api.settings;

import jandcode.app.*;
import jandcode.dbm.*;
import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

public class WsSettingsService extends CompRt implements IWsSettings {

    static Log log = LogFactory.getLog("jdtx.WsSettingsService");

    Db db = null;

    public Db getDb() {
        if (db == null) {
            db = getApp().service(ModelService.class).getModel().getDb();
        }

        return db;
    }

    @Override
    public long getWsId() throws Exception {
        DataRecord rec = loadRec();
        return rec.getValueLong("ws_id");
    }

    @Override
    public String getWsGuid() throws Exception {
        DataRecord rec = loadRec();
        return rec.getValueString("guid");
    }

    private DataRecord loadRec() throws Exception {
        return getDb().loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO").getCurRec();
    }

    @Override
    public void setWsIdGuid(long wsId, String wsGuid) throws Exception {
        log.info("Помечаем рабочую станцию, ws_id: " + wsId + ", guid: " + wsGuid);

        // Второй раз - не помечаем
        if (getWsId() != 0) {
            throw new XError("Workstation already marked, ws_id: " + getWsId() + ", guid: " + getWsGuid());
        }

        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO set ws_id = " + wsId + ", guid = '" + wsGuid + "'";
        getDb().execSql(sql);
    }

    @Override
    public JSONObject getCfgDecode() throws Exception {
        CfgManager cfgManager = new CfgManager(getDb());
        JSONObject cfgDecode = cfgManager.getSelfCfg(CfgType.DECODE);
        return cfgDecode;
    }

    @Override
    public void setCfgDecode(JSONObject cfg) throws Exception {
        CfgManager cfgManager = new CfgManager(getDb());
        cfgManager.setSelfCfg(cfg, CfgType.DECODE);
    }

}
