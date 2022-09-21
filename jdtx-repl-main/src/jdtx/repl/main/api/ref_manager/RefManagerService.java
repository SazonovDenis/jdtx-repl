package jdtx.repl.main.api.ref_manager;

import jandcode.app.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.settings.*;
import org.apache.commons.logging.*;

public abstract class RefManagerService extends CompRt implements IRefManager {

    static Log log = LogFactory.getLog("jdtx.RefManagerService");

    Db db = null;

    IWsSettings wsSettings = null;

    public Db getDb() {
        if (db == null) {
            db = getApp().service(ModelService.class).getModel().getDb();
        }

        return db;
    }

    protected IWsSettings getWsSettings() {
        if (wsSettings == null) {
            wsSettings = getApp().service(WsSettingsService.class);
        }

        return wsSettings;
    }


}
