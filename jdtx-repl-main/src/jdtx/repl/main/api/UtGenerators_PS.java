package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;

/**
 * Реализация UtGenerators для PawnShop и RefDecoder с перекодировкой ссылок.
 * Грязно и по месту.
 */
public class UtGenerators_PS extends UtGenerators {

    public UtGenerators_PS(Db db, IJdxDbStruct struct) {
        super(db, struct);
    }

    @Override
    long getMaxPk(IJdxTable table) throws Exception {
        String idFieldName = table.getPrimaryKey().get(0).getName();
        String sql = "select max(" + idFieldName + ") as maxId from " + table.getName() + " where " + idFieldName + " <= " + RefDecoder.get_max_own_id();
        long maxId = db.loadSql(sql).getCurRec().getValueLong("maxId");
        return maxId;
    }

    @Override
    String getGeneratorName(String tableName) {
        return "g_" + tableName;
    }

    @Override
    long getValue(String generatorName) throws Exception {
        long valueCurr = db.loadSql("select gen_id(" + generatorName + ", 0) as valueCurr from dual").getCurRec().getValueLong("valueCurr");
        return valueCurr;
    }

    @Override
    void setValue(String generatorName, long value) throws Exception {
        db.execSql("set generator " + generatorName + " to " + value + "");
    }

}
