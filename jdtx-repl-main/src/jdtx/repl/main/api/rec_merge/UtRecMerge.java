package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Утилиты по слиянию записей
 */
public class UtRecMerge implements IUtRecMerge {

    public static final boolean DO_DELETE = true;
    public static final boolean UPDATE_ONLY = false;

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
    public Collection<RecDuplicate> findTableDuplicates(String tableName, String[] fieldNames) throws Exception {
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
        sb.append(" order by id");
        sqlRec = sqlRec + sb.toString();

        //
        Set paramsHashSet = new HashSet();

        //
        int recordsTotal = db.loadSql("select count(*) cnt from " + tableName).getCurRec().getValueInt("cnt");

        //
        DataStore query = db.loadSql(sqlAll);
        int n = 0;
        for (DataRecord rec : query) {
            n = n + 1;
            if (recordsTotal > 1000 && n % 100 == 0) {
                System.out.println(tableName + ": " + n + "/" + recordsTotal);
            }
            //
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
    public Map<String, MergeResultTable> execMergeTask(Collection<RecMergeTask> mergeTasks, boolean doDelete) throws Exception {
        Map<String, MergeResultTable> result = new HashMap<>();

        //
        for (RecMergeTask mergeTask : mergeTasks) {
            db.startTran();
            try {
                //
                MergeResultTable taskResultTable = result.get(mergeTask.tableName);
                if (taskResultTable == null) {
                    taskResultTable = new MergeResultTable();
                    result.put(mergeTask.tableName, taskResultTable);
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
                    MergeResultRefTable taskRecResult = taskResultTable.mergeResultsRefTable.get(key);
                    if (taskRecResult == null) {
                        taskRecResult = new MergeResultRefTable();
                        taskRecResult.refTtableName = refTableName;
                        taskRecResult.refTtableRefFieldName = fkRefFieldName;
                        taskResultTable.mergeResultsRefTable.put(key, taskRecResult);
                    }

                    //
                    String sqlUpdate = "update " + refTableName + " set " + fkRefFieldName + " = :" + fkRefFieldName + "_NEW" + " where " + fkRefFieldName + " = :" + fkRefFieldName + "_OLD";
                    String sqlSelect = "select * from " + refTableName + " where " + fkRefFieldName + " = :" + fkRefFieldName + "_OLD";

                    //
                    for (long deleteRecId : mergeTask.recordsDelete) {
                        Map params = UtCnv.toMap(
                                fkRefFieldName + "_OLD", deleteRecId,
                                fkRefFieldName + "_NEW", mergeTask.recordEtalon.get(pkField)
                        );

                        // Селектим как есть сейчас
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
                    String sqlSelect = "select * from " + mergeTask.tableName + " where " + pkField + " = :" + pkField;
                    String sqlDelete = "delete from " + mergeTask.tableName + " where " + pkField + " = :" + pkField;

                    //
                    for (long deleteRecId : mergeTask.recordsDelete) {
                        Map params = UtCnv.toMap(pkField, deleteRecId);

                        // Селектим как есть сейчас
                        DataStore st = db.loadSql(sqlSelect, params);
                        //
                        if (taskResultTable.recordsDeleted == null) {
                            taskResultTable.recordsDeleted = st;
                        } else {
                            UtData.copyStore(st, taskResultTable.recordsDeleted);
                        }

                        // Удаляем
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

    @Override
    public void revertExecTask(Map<String, MergeResultTable> taskResults) {

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
            task.recordEtalon = duplicate.records.get(0).getValues();
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

    public static void printTasks(Collection<RecMergeTask> mergeTasks) {
        System.out.println("MergeTasks count: " + mergeTasks.size());
        System.out.println();
        for (RecMergeTask mergeTask : mergeTasks) {
            System.out.println("Table: " + mergeTask.tableName);
            System.out.println("Etalon: " + mergeTask.recordEtalon);
            System.out.println("Delete: " + mergeTask.recordsDelete);
            System.out.println();
        }
    }

    public static void printMergeResults(Map<String, MergeResultTable> mergeResults) {
        System.out.println("MergeResults:");
        System.out.println();
        for (String taskTableName : mergeResults.keySet()) {
            System.out.println("TableName: " + taskTableName);
            System.out.println();

            MergeResultTable mergeResultTable = mergeResults.get(taskTableName);

            for (String refTableName : mergeResultTable.mergeResultsRefTable.keySet()) {
                MergeResultRefTable mergeResultRefTable = mergeResultTable.mergeResultsRefTable.get(refTableName);
                System.out.println("Ref table: " + mergeResultRefTable.refTtableName);
                System.out.println("Ref field: " + mergeResultRefTable.refTtableRefFieldName + " -> " + taskTableName);
                if (mergeResultRefTable.recordsUpdated == null || mergeResultRefTable.recordsUpdated.size() == 0) {
                    System.out.println("Ref records updated: empty");
                } else {
                    UtData.outTable(mergeResultRefTable.recordsUpdated);
                }
                System.out.println();
            }

            System.out.println("Records deleted from " + taskTableName + ":");
            if (mergeResultTable.recordsDeleted == null || mergeResultTable.recordsDeleted.size() == 0) {
                System.out.println("Records deleted: empty");
            } else {
                UtData.outTable(mergeResultTable.recordsDeleted);
            }
        }
    }

}
