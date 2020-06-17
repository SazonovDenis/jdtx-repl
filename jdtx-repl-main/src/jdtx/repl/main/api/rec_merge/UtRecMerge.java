package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Утилиты по слиянию записей
 */
class UtRecMerge implements IUtRecMerge {

    Db db;
    IJdxDbStruct struct;

    public UtRecMerge(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    @Override
    public Collection<String> loadTables() {
        Collection<String> res = new ArrayList<>();
        for (IJdxTable table : struct.getTables()) {
            res.add(table.getName());
        }
        return res;
    }

    @Override
    public Collection<String> loadTableFields(String tableName) {
        Collection<String> res = new ArrayList<>();
        for (IJdxField field : struct.getTable(tableName).getFields()) {
            res.add(field.getName());
        }
        return res;
    }


    @Override
    public Collection<UtRecDuplicate> loadTableDuplicates(String tableName, String[] fieldNames) throws Exception {
        List<UtRecDuplicate> resList = new ArrayList<>();

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
                UtRecDuplicate res = new UtRecDuplicate();
                res.params = params;
                res.records = store;
                resList.add(res);
                //
                paramsHashSet.add(paramsHash);
            }
        }


        //
        return resList;
    }

    @Override
    public Collection<UtRemoveDuplicatesRes> execRemoveDuplicates(Collection<UtRecMergeTask> tasks) throws Exception {
        Collection<UtRemoveDuplicatesRes> res = new ArrayList<>();
        //
        for (UtRecMergeTask task : tasks) {
            UtRemoveDuplicatesRes removeDuplicatesRes = new UtRemoveDuplicatesRes();
            //
            removeDuplicatesRes.tableName = null;
            removeDuplicatesRes.recordsUpdated = null;
            //
            res.add(removeDuplicatesRes);
        }
        //
        return res;
    }

    /**
     * "Наивное" превращение ВСЕХ дублей в задание на удаление
     */
    public Collection<UtRecMergeTask> prepareRemoveDuplicatesTaskAsIs(String tableName, Collection<UtRecDuplicate> duplicates) throws Exception {
        String pkField = "id"; //todo - узнавать PK по-нормальному!
        //
        Collection<UtRecMergeTask> res = new ArrayList<>();
        //
        for (UtRecDuplicate duplicate : duplicates) {
            UtRecMergeTask task = new UtRecMergeTask();
            //
            task.tableName = tableName;
            task.recordEtalon = duplicate.records.get(0);
            for (int i = 1; i < duplicate.records.size(); i++) {
                task.recordsDelete.add(duplicate.records.get(i).getValueLong(pkField));
            }
            //
            res.add(task);
        }
        //
        return res;
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
