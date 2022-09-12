package jdtx.repl.main.api.data_filler;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

public class UtFiller {

    Db db;
    IJdxDbStruct struct;
    Random rnd;

    public UtFiller(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
        this.rnd = new Random();
    }

    /**
     * Для таблицы table собирает все id (т.е. все возможные значения ссылок на нее)
     */
    public Set<Long> loadAllIds(String tableName) throws Exception {
        return loadAllIds(struct.getTable(tableName));
    }

    Set<Long> loadAllIds(IJdxTable table) throws Exception {
        String refTableName = table.getName();
        IJdxField pkField = table.getPrimaryKey().get(0);
        String pkFieldName = pkField.getName();
        DataStore refSt = db.loadSql("select " + pkFieldName + " as id from " + refTableName);
        return UtData.uniqueValues(refSt, "id");
    }


    /**
     * Из набора set выбирает случайно count элементов.
     * Если запрошено больше, чем есть в наборе - вернет весь набор.
     *
     * @return Новый набор
     */
    Set<Long> choiceSubsetFromSet(Set<Long> set, int count) {
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
            res.add((Long) arr[idx]);
        }

        return res;
    }

    /**
     * Из набора set выбирает случайно count элементов, леэащих в указанном диапазоне min..max.
     * Если запрошено больше, чем есть в наборе - вернет весь набор.
     *
     * @return Новый набор
     */
    Set<Long> choiceSubsetFromSet(Set<Long> set, long min, long max, int count) {
        Set<Long> tmp = new HashSet<>();

        for (Long val : set) {
            if (val >= min && val <= max) {
                tmp.add(val);
            }
        }

        //
        return choiceSubsetFromSet(tmp, count);
    }


    /**
     * Если object является коллекцией, то выбирает случайно один элемент,
     * иначе возвращает сам object.
     */
    public Object selectOneObject(Object object) {
        Object res;

        if (object instanceof List) {
            List<Object> list = (List) object;
            int rndIdx = rnd.nextInt(list.size());
            res = list.get(rndIdx);
        } else if (object instanceof Collection) {
            Collection<Object> collection = (Collection) object;
            int rndIdx = rnd.nextInt(collection.size());
            res = collection.toArray()[rndIdx];
        } else {
            res = object;
        }

        return res;
    }


}
