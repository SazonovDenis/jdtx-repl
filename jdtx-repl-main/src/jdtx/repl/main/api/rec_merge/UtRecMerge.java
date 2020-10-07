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
    public Collection<RecDuplicate> loadTableDuplicates(String tableName, String[] fieldNames) throws Exception {
        List<RecDuplicate> resList = new ArrayList<>();

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
                RecDuplicate res = new RecDuplicate();
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
    public Map<String, Map<String, RecMergeResultRefTable>> execMergeTask(Collection<RecMergeTask> mergeTasks, boolean doDelete) throws Exception {
        Map<String, Map<String, RecMergeResultRefTable>> result = new HashMap<>();

        //
        for (RecMergeTask mergeTask : mergeTasks) {
            db.startTran();
            try {
                //
                Map<String, RecMergeResultRefTable> taskResult = result.get(mergeTask.tableName);
                if (taskResult == null) {
                    taskResult = new HashMap();
                    result.put(mergeTask.tableName, taskResult);
                }

                //
                String pkField = struct.getTable(mergeTask.tableName).getPrimaryKey().get(0).getName();
                Map<String, IJdxForeignKey> refsToTable = getRefsToTable(mergeTask.tableName);

                // UPD - Перебиваем ссылки у зависимых таблиц
                for (String refTableName : refsToTable.keySet()) {
                    IJdxForeignKey fk = refsToTable.get(refTableName);

                    //
                    String fkRefFieldName = fk.getField().getName();

                    //
                    String key = refTableName + "_" + fkRefFieldName;
                    RecMergeResultRefTable taskRecResult = taskResult.get(key);
                    if (taskRecResult == null) {
                        taskRecResult = new RecMergeResultRefTable();
                        taskRecResult.refTtableName = refTableName;
                        taskRecResult.refTtableRefFieldName = fkRefFieldName;
                        taskResult.put(key, taskRecResult);
                    }

                    //
                    String sqlUpdate = "update " + refTableName + " set " + fkRefFieldName + " = :" + fkRefFieldName + "_NEW" + " where " + fkRefFieldName + " = :" + fkRefFieldName + "_OLD";
                    String sqlSelect = "select * from " + refTableName + " where " + fkRefFieldName + " = :" + fkRefFieldName + "_OLD";

                    //
                    for (long deleteRecId : mergeTask.recordsDelete) {
                        Map params = UtCnv.toMap(
                                fkRefFieldName + "_OLD", deleteRecId,
                                fkRefFieldName + "_NEW", mergeTask.recordEtalon.getValue(pkField)
                        );

                        // Селектим как сейчас
                        DataStore st = db.loadSql(sqlSelect, params);

                        // Апдейтим
                        db.execSql(sqlUpdate, params);

                        // Отчитаемся
                        if (taskRecResult.recordsUpdated == null) {
                            taskRecResult.recordsUpdated = st;
                        } else {
                            UtData.copyStore(st, taskRecResult.recordsUpdated);
                        }
                    }

                }

                // DEL - Удаляем лишние (теперь уже) записи
                if (doDelete) {
                    String sqlDelete = "delete from " + mergeTask.tableName + " where " + pkField + " = :" + pkField;

                    //
                    for (long deleteRecId : mergeTask.recordsDelete) {
                        // Удаляем
                        Map params = UtCnv.toMap(pkField, deleteRecId);
                        db.execSql(sqlDelete, params);
                    }
                }


                //
                db.commit();
            } catch (Exception e) {
                db.rollback();
                throw e;
            }

        }

        //
        return result;
    }

    /**
     * @return Список таблиц, которые имеют ссылки на таблицу tableName
     */
    private Map<String, IJdxForeignKey> getRefsToTable(String tableName) {
        Map<String, IJdxForeignKey> res = new HashMap();

        //
        IJdxTable table = struct.getTable(tableName);

        //
        for (IJdxTable refTable : struct.getTables()) {
            for (IJdxForeignKey refTableFk : refTable.getForeignKeys()) {
                if (refTableFk.getTable().getName().equals(table.getName())) {
                    res.put(refTable.getName(), refTableFk);
                }
            }
        }

        //
        return res;
    }

    /**
     * "Наивное" превращение ВСЕХ дублей в задание на удаление
     */
    public Collection<RecMergeTask> prepareRemoveDuplicatesTaskAsIs(String tableName, Collection<RecDuplicate> duplicates) throws Exception {
        String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        //
        Collection<RecMergeTask> res = new ArrayList<>();
        //
        for (RecDuplicate duplicate : duplicates) {
            RecMergeTask task = new RecMergeTask();
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


}
