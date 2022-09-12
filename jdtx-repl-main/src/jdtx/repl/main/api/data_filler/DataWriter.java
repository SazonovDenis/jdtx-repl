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

    public DataWriter(Db db, IJdxDbStruct struct, Map<String, Object> defaultGenerators) throws Exception {
        this.db = db;
        this.dbu = new JdxDbUtils(db, struct);
        this.struct = struct;
        this.filler = new DataFiller(db, struct, defaultGenerators);
    }

    public DataWriter(Db db, IJdxDbStruct struct) throws Exception {
        this(db, struct, null);
    }

    @Override
    public Map<Long, Map<String, Object>> ins(String tableName, int count) throws Exception {
        return ins(tableName, count);
    }

    @Override
    public Map<Long, Map<String, Object>> ins(String tableName, int count, Map<String, Object> tableGenerators) throws Exception {
        Map<Long, Map<String, Object>> res = new HashMap<>();

        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();

        // Создадим генераторы для всех полей в таблице
        Map<String, Object> generators = filler.createGenerators(table, tableGenerators);

        // Нагенерим записей по шаблонам
        for (int i = 0; i < count; i++) {
            Map<String, Object> recValues = filler.genRecord(table, generators);
            recValues.put(pkFieldName, null);
            long id = dbu.insertRec(tableName, recValues);
            res.put(id, recValues);
        }

        // Если в таблицу добавили записей - сброим кэш значений ссылок на нее
        String keyRef = filler.getRefKey(table);
        filler.getGeneratorsCache().remove(keyRef);

        //
        return res;
    }

    @Override
    public Map<Long, Map<String, Object>> upd(String tableName, int count) throws Exception {
        // Получим все id
        Set<Long> setFull = loadAllIds(tableName);

        // Отберем из них сколько просили
        Set<Long> set = choiceSubsetFromSet(setFull, count);

        // Изменим отобранные id
        return upd(tableName, set, null);
    }

    @Override
    public Map<Long, Map<String, Object>> upd(String tableName, int count, Map<String, Object> tableGenerators) throws Exception {
        // Получим все id
        Set<Long> setFull = loadAllIds(tableName);

        // Отберем из них сколько просили
        Set<Long> set = choiceSubsetFromSet(setFull, count);

        // Изменим отобранные id
        return upd(tableName, set, tableGenerators);
    }

    @Override
    public Map<Long, Map<String, Object>> upd(String tableName, Collection<Long> ids) throws Exception {
        // Изменим отобранные id
        return upd(tableName, ids, null);
    }

    @Override
    public Map<Long, Map<String, Object>> upd(String tableName, Collection<Long> ids, Map<String, Object> tableGenerators) throws Exception {
        Map<Long, Map<String, Object>> res = new HashMap<>();

        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();

        // Создадим генераторы для всех полей в таблице
        Map<String, Object> generators = filler.createGenerators(table, tableGenerators);

        // Нагенерим записей по шаблонам
        for (Long id : ids) {
            Map<String, Object> recValues = filler.genRecord(table, generators);
            recValues.put(pkFieldName, id);
            dbu.updateRec(tableName, recValues);
            res.put(id, recValues);
        }

        // Если в таблицу добавили записей - сброим кэш значений ссылок на нее
        String keyRef = filler.getRefKey(table);
        filler.getGeneratorsCache().remove(keyRef);

        //
        return res;
    }


    @Override
    public void del(String tableName, int count, boolean cascade) throws Exception {
        // Получим все id
        Set<Long> setFull = loadAllIds(tableName);

        // Отберем из них сколько просили
        Set<Long> set = choiceSubsetFromSet(setFull, count);

        // Удалим отобранные id
        del(tableName, set, cascade);
    }

    @Override
    // todo доделать cascade и возврат результата
    public void del(String tableName, Collection<Long> ids, boolean cascade) throws Exception {
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = table.getPrimaryKey().get(0).getName();

        if (ids.size() != 0) {
            String idsStr = ids.toString();
            idsStr = idsStr.substring(1, idsStr.length() - 1);
            String sqlDelete = "delete from " + tableName + " where " + pkFieldName + " in (" + idsStr + ")";

            db.execSql(sqlDelete);
        }

        // Если из таблицы удалили записи - сброим кэш значений ссылок на нее
        String keyRef = filler.getRefKey(table);
        filler.getGeneratorsCache().remove(keyRef);
    }

    /**
     * Получим все id в таблице tableName
     */
    public Set<Long> loadAllIds(String tableName) throws Exception {
        DataStore st1 = db.loadSql("select id from " + tableName);
        Set<Long> setFull = UtData.uniqueValues(st1, "id");
        return setFull;
    }

    /**
     * Из набора set выбирает случайно count элементов.
     * Если запрошено больше, чем есть исходно - вернет все, что есть.
     *
     * @return Новый набор
     */
    public Set<Long> choiceSubsetFromSet(Set<Long> set, Integer count) {
        Set<Long> res = new HashSet<>();

        if (set.size() <= count) {
            res.addAll(set);
            return res;
        }

        // todo 99999 из 100000 будет долго выбирать - сделать скользящий алгоритм
        Random rnd = new Random();
        Object[] arr = set.toArray();
        while (res.size() < count) {
            int idx = rnd.nextInt(set.size());
            res.add((Long)arr[idx]);
        }

        return res;
    }

}
