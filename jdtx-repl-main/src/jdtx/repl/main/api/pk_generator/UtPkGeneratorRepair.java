package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class UtPkGeneratorRepair {

    Db db;
    IJdxDbStruct struct;
    IDbGenerators dbGenerators;
    IAppPkRules pkRules;
    IRefManager refManager;

    protected static Log log = LogFactory.getLog("jdtx.UtPkGeneratorRepair");

    public UtPkGeneratorRepair(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
        this.dbGenerators = DbToolsService.getDbGenerators(db);
        this.pkRules = db.getApp().service(AppPkRulesService.class);
        this.refManager = db.getApp().service(RefManagerService.class);
    }

    public void repairGenerator(IJdxTable table) throws Exception {
        long valueMaxId = refManager.get_max_own_id(table);
        String generatorName = pkRules.getGeneratorName(table.getName());
        long valueGen = dbGenerators.getValue(generatorName);
        if (valueMaxId > valueGen) {
            log.info("repairGenerator: " + generatorName + ", set " + valueGen + " to " + valueMaxId);
            dbGenerators.setValue(generatorName, valueMaxId);
        }
    }

    public void repairGenerators() throws Exception {
        for (IJdxTable table : struct.getTables()) {
            repairGenerator(table);
        }
    }

}
