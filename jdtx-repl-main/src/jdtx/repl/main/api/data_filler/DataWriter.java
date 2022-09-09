package jdtx.repl.main.api.data_filler;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;

import java.util.*;

public class DataWriter implements IDataWriter {


    Db db;
    IJdxDbStruct struct;
    IDataFiller filler;
    JdxDbUtils dbu;

    public DataWriter(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
        this.filler = new DataFiller(db, struct);
        dbu = new JdxDbUtils(db, struct);
    }

    @Override
    public Map<Long, Map> ins(String tableName, int count) throws Exception {
        return ins(tableName, count, null);
    }

    @Override
    public Map<Long, Map> ins(String tableName, int count, Map<String, Object> generatorsDefault) throws Exception {
        Map<Long, Map> res = new HashMap<>();

        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();

        // Нагенерим генераторов для остальных полей в таблице
        Map<String, Object> generators = filler.createGenerators(table, generatorsDefault);

        // Нагенерим записей по шаблонам
        for (int i = 0; i < count; i++) {
            Map recValues = filler.genRecord(table, generators);
            recValues.put(pkFieldName, null);
            long id = dbu.insertRec(tableName, recValues);
            res.put(id, recValues);
        }

        //
        return res;
    }

    @Override
    public Map<Long, DataFillerRec> upd(String tableName, int count) {
        return null;
    }

    @Override
    public Map<Long, DataFillerRec> upd(String tableName, int count, Map<String, Object> valuesVariansSet) {
        return null;
    }

    @Override
    public Map<Long, DataFillerRec> upd(String tableName, Collection<Long> ids) {
        return null;
    }

    @Override
    public Map<Long, DataFillerRec> upd(String tableName, Collection<Long> ids, Map<String, Object> valuesVariansSet) {
        return null;
    }

    @Override
    public Map<Long, DataFillerRec> del(String tableName, int count, boolean cascade) {
        return null;
    }

    @Override
    public Map<Long, DataFillerRec> del(String tableName, Collection<Long> ids, boolean cascade) {
        return null;
    }


}
