package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public abstract class PkGenerator implements IPkGenerator {

    Db db;
    IJdxDbStruct struct;

    protected static Log log = LogFactory.getLog("jdtx.UtGenerators");

    public PkGenerator(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    public void repairGenerators() throws Exception {
        for (IJdxTable table : struct.getTables()) {
            repairGenerator(table);
        }
    }

    @Override
    public void repairGenerator(IJdxTable table) throws Exception {
        long valueMaxId = getMaxPk(table.getName());
        String generatorName = getGeneratorName(table.getName());
        long valueGen = getValue(generatorName);
        if (valueMaxId > valueGen) {
            log.info("repairGenerator: " + generatorName + ", set " + valueGen + " to " + valueMaxId);
            setValue(generatorName, valueMaxId);
        }
    }

}
