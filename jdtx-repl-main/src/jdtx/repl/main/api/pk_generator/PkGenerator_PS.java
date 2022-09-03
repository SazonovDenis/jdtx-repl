package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;

/**
 * Реализация PkGenerators для PawnShop и RefDecoder с учетом перекодировкой ссылок.
 * Грязно и по месту.
 * todo: хорошо бы реализовать прочие генераторы (номер билета)
 */
public class PkGenerator_PS extends PkGenerator implements IPkGenerator {

    public PkGenerator_PS(Db db, IJdxDbStruct struct) {
        super(db, struct);
    }

    @Override
    public long getMaxPk(String tableName) throws Exception {
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        String sql = "select max(" + pkFieldName + ") as maxId from " + table.getName() + " where " + pkFieldName + " <= " + RefManagerDecode.get_max_own_id();
        long maxId = db.loadSql(sql).getCurRec().getValueLong("maxId");
        return maxId;
    }

    @Override
    public String getGeneratorName(String tableName) {
        return "g_" + tableName;
    }

    @Override
    public long getValue(String generatorName) throws Exception {
        long valueCurr = db.loadSql("select gen_id(" + generatorName + ", 0) as valueCurr from dual").getCurRec().getValueLong("valueCurr");
        return valueCurr;
    }

    @Override
    public void setValue(String generatorName, long value) throws Exception {
        db.execSql("set generator " + generatorName + " to " + value + "");
    }

}
