package jdtx.repl.main.api.pk_generator;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public abstract class AppPkRulesService extends CompRt implements IAppPkRules {

    Db db = null;
    IJdxDbStruct struct = null;

    protected static Log log = LogFactory.getLog("jdtx.AppPkRulesService");

    void init(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    public IAppPkRules createPkGenerator(Db db, IJdxDbStruct struct) throws Exception {
        Class<? extends IAppPkRules> cls = getApp().service(AppPkRulesService.class).getClass();
        AppPkRulesService instance = (AppPkRulesService) cls.newInstance();
        //
        instance.init(db, struct);
        //
        return instance;
    }

}
