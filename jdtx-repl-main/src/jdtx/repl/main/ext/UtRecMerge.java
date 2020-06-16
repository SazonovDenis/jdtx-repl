package jdtx.repl.main.ext;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;

import java.util.*;

/**
 * Утилиты по слиянию записей
 */
class UtRecMerge {

    class UtRecMergeRes {
        Map params;
        DataStore store;
    }

    Db db;

    public UtRecMerge(Db db) {
        this.db = db;
    }

    public List<UtRecMergeRes> recordAnalys(String tableName, String[] fieldNames) throws Exception {
        List<UtRecMergeRes> resList = new ArrayList<>();

        //
        String sqlAll = "select * from " + tableName + " order by id";

        //
        String sqlRec = "select * from " + tableName + " where ";
        StringBuilder sb = new StringBuilder();
        for (String name : fieldNames) {
            if (sb.length() != 0) {
                sb.append(" and ");
            }
            sb.append("(");
            sb.append(name);
            sb.append(" is not null and upper(");
            sb.append(name);
            sb.append(") = upper(:");
            sb.append(name);
            sb.append(")");
            sb.append(")");

        }
        sqlRec = sqlRec + sb.toString();

        //
        Set paramsHashSet = new HashSet();


        //
        DataStore query = db.loadSql(sqlAll);
        for (DataRecord rec : query) {
            Map params = new HashMap();
            boolean valueWasEmpty = false;
            for (String name : fieldNames) {
                Object value = rec.getValue(name);
                if (valueIsEmpty(value)) {
                    valueWasEmpty = true;
                }
                params.put(name, value);
            }

            // Не ищем дубли по пустым полям
            if (valueWasEmpty) {
                continue;
            }

            // Чтобы раз найденный дубль не находился много раз
            String paramsHash = getParamsHash(params);
            if (paramsHashSet.contains(paramsHash)) {
                continue;
            }

            //
            DataStore store = db.loadSql(sqlRec, params);
            if (store.size() > 1) {
                UtRecMergeRes res = new UtRecMergeRes();
                res.params = params;
                res.store = store;
                resList.add(res);
                //
                paramsHashSet.add(paramsHash);
            }
        }


        //
        return resList;
    }

    private String getParamsHash(Map params) {
        StringBuilder sb = new StringBuilder();
        for (Object value : params.entrySet()) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(((Map.Entry) value).getKey());
            sb.append(":");
            sb.append(((Map.Entry) value).getValue().toString().toUpperCase());
        }
        return sb.toString();
    }

    private boolean valueIsEmpty(Object value) {
        if (value == null) {
            return true;
        } else if (value instanceof String) {
            return UtString.isWhite((String) value);
        }
        return false;
    }


    static void record_merge(String fileName) {

    }

}
