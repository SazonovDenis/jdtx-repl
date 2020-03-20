package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

public class UtGenerators {

    Db db;
    IJdxDbStruct struct;

    protected static Log log = LogFactory.getLog("jdtx.UtGenerators");

    public UtGenerators(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    public void repairGenerators() throws Exception {
        for (IJdxTable table : struct.getTables()) {
            repairGenerator(table);
        }
    }

    void repairGenerator(IJdxTable table) throws Exception {
        long valueMaxId = getMaxPk(table);
        String generatorName = getGeneratorName(table.getName());
        long valueGen = getValue(generatorName);
        if (valueMaxId > valueGen) {
            log.info("repairGenerator: " + generatorName + ", set " + valueGen + " to " + valueMaxId);
            setValue(generatorName, valueMaxId);
        }

    }

    long getMaxPk(IJdxTable table) throws Exception {
        return 0;
    }

    String getGeneratorName(String tableName) {
        return null;
    }

    long getValue(String generatorName) throws Exception {
        return 0;
    }

    void setValue(String generatorName, long value) throws Exception {

    }

}
