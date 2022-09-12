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
    public Set<Long> loadAllIds(IJdxTable table) throws Exception {
        String refTableName = table.getName();
        IJdxField pkField = table.getPrimaryKey().get(0);
        String pkFieldName = pkField.getName();
        DataStore refSt = db.loadSql("select " + pkFieldName + " as id from " + refTableName);
        return UtData.uniqueValues(refSt, "id");
    }

    public Set<Long> loadAllIds(String tableName) throws Exception {
        return loadAllIds(struct.getTable(tableName));
    }


    /**
     * Из набора set выбирает случайно count элементов.
     * Если запрошено больше, чем есть исходно - вернет все, что есть.
     *
     * @return Новый набор
     */
    Set<Long> choiceSubsetFromSet(Set<Long> set, Integer count) {
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


    public Object selectOneObject(Object object) {
        Object res;

        if (object instanceof List) {
            List<Object> list = (List) object;
            int rndIdx = rnd.nextInt(list.size());
            res = list.get(rndIdx);
        } else if (object instanceof Set) {
            Set<Object> set = (Set) object;
            int idx = rnd.nextInt(set.size());
            res = set.toArray()[idx];
        } else {
            res = object;
        }

        return res;
    }


}
