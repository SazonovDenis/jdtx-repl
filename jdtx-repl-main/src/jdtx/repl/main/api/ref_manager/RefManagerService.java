package jdtx.repl.main.api.ref_manager;

import jandcode.app.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.settings.*;
import org.apache.commons.logging.*;

public abstract class RefManagerService extends CompRt implements IRefManager {

    static Log log = LogFactory.getLog("jdtx.RefManagerService");

    public Db getDb() throws Exception {
        Db db = getApp().service(ModelService.class).getModel().getDb();
        return db;
    }

    protected IWsSettings getWsSettings() {
        IWsSettings wsSettings = getApp().service(WsSettingsService.class);
        return wsSettings;
    }

    @Override
    public IRefManager getInstance() throws Exception {
        return this;
    }

}
