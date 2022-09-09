package jdtx.repl.main.api.data_filler;

import jandcode.dbm.data.*;
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
    public Map<Long, DataFillerRec> del(String tableName, int count, boolean cascade) throws Exception {
        // Получим все id
        DataStore st1 = db.loadSql("select id from " + tableName);
        Set set = UtData.uniqueValues(st1, "id");

        // Отберем из них сколько просили
        Set setDel = choiceSubsetFromSet(set, count);

        // Удалим отобранные id
        return del(tableName, setDel, cascade);
    }

    @Override
    // todo доделать cascade и возврат результата
    public Map<Long, DataFillerRec> del(String tableName, Collection<Long> ids, boolean cascade) throws Exception {
        Map<Long, DataFillerRec> res = new HashMap<>();

        if (ids.size() != 0) {
            String idsStr = ids.toString();
            idsStr = idsStr.substring(1, idsStr.length() - 1);
            String pkFieldName = struct.getTable(tableName).getPrimaryKey().get(0).getName();
            String sqlDelete = "delete from " + tableName + " where " + pkFieldName + " in (" + idsStr + ")";

            db.execSql(sqlDelete);
        }

        return res;
    }

    /**
     * Из набора set выбирает случайно count элементов.
     * Если запрошено больше, чем есть исходно - вернет все, что есть.
     *
     * @return Новый набор
     */
    public Set<Object> choiceSubsetFromSet(Set<Object> set, Integer count) {
        Set<Object> res = new HashSet<>();

        if (set.size() <= count) {
            res.addAll(set);
            return res;
        }

        // todo 99999 из 100000 будет долго выбирать - сделать скользящий алгоритм
        Random rnd = new Random();
        Object[] arr = set.toArray();
        while (res.size() < count) {
            int idx = rnd.nextInt(set.size());
            res.add(arr[idx]);
        }

        return res;
    }

}
