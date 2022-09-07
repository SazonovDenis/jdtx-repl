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
        long valueMaxId = getMaxPkValue(table);
        String generatorName = pkRules.getGeneratorName(table.getName());
        long valueGen = dbGenerators.getGeneratorCurrValue(generatorName);
        if (valueMaxId > valueGen) {
            log.info("repairGenerator: " + generatorName + ", set " + valueGen + " to " + valueMaxId);
            dbGenerators.setGeneratorValue(generatorName, valueMaxId);
        }
    }

    public void repairGenerators() throws Exception {
        for (IJdxTable table : struct.getTables()) {
            repairGenerator(table);
        }
    }

    /**
     * Возврящает последний занятый PK для таблицы.
     * Выражает особенности организации (разведения) PK в приложении.
     */
    public long getMaxPkValue(IJdxTable table) throws Exception {
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        String sql = "select max(" + pkFieldName + ") as maxId from " + table.getName() + " where " + pkFieldName + " <= " + refManager.get_max_own_id();
        long maxId = db.loadSql(sql).getCurRec().getValueLong("maxId");
        //
        return maxId;
    }


}
