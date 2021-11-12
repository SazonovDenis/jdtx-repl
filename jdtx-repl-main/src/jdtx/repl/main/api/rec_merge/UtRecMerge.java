package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 * Утилиты по поиску и слиянию дубликатов записей (исполнитель)
 */
public class UtRecMerge implements IUtRecMerge {


    int COMMIT_SIZE = 100;

    Db db;
    JdxDbUtils dbu;
    IJdxDbStruct struct;
    public GroupsStrategyStorage groupsStrategyStorage;

    //
    protected static Log log = LogFactory.getLog("jdtx.UtRecMerge");

    //
    public UtRecMerge(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
        this.groupsStrategyStorage = new GroupsStrategyStorage();
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
    public Collection<RecDuplicate> findTableDuplicates(String tableName, Collection<String> fieldNames, boolean useNullValues) throws Exception {
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
            if (!useNullValues) {
                sb.append(name);
                sb.append(" is not null and ");
            }
            sb.append("upper(");
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
            boolean valueWasEmptyAll = true;
            for (String name : fieldNames) {
                Object value = rec.getValue(name);
                if (valueIsEmpty(value)) {
                    valueWasEmpty = true;
                } else {
                    valueWasEmptyAll = false;
                }
                params.put(name, value);
            }

            // Не ищем дубли для записи, если пусты НЕКОТОРЫЕ её поля, по которым надо искать (поля, перечисленные в fieldNames)
            if (!useNullValues && valueWasEmpty) {
                continue;
            }

            // Не ищем дубли для записи, если пусты ВСЕ её поля, по которым надо искать
            if (valueWasEmptyAll) {
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


    public Collection<RecDuplicate> findTableDuplicates(String tableName, Collection<String> fieldNames) throws Exception {
        return findTableDuplicates(tableName, fieldNames, true);
    }


    public void execMergePlan(Collection<RecMergePlan> plans, File resultFile) throws Exception {
        // Сохраняем результат выполнения задачи
        RecMergeResultWriter recMergeResultWriter = new RecMergeResultWriter();
        recMergeResultWriter.open(resultFile);

        // Исполняем
        execMergePlan(plans, recMergeResultWriter);

        // Сохраняем
        recMergeResultWriter.close();
    }

    @Override
    public void execMergePlan(Collection<RecMergePlan> plans, RecMergeResultWriter resultWriter) throws Exception {
        boolean doDelete = true;  // Жалко удалять режим, но пока не понятно, зачем он практически нужен

        //
        try {
            int count = plans.size();
            int done = 0;
            int done_portion = 0;

            //
            db.startTran();

            //
            for (RecMergePlan mergePlan : plans) {
                // INS - Создаем эталонную запись.
                // "Эталонная" запись должна быть именно ВСТАВЛЕНА, а не выбранной из уже существующих,
                // т.к. на рабочей станции может НЕ ОКАЗАТЬСЯ той записи, которую назначили как "эталонная".
                IJdxTable table = struct.getTable(mergePlan.tableName);
                Map<String, Object> params = prepareValues(mergePlan.recordEtalon, table.getFields());
                //
                String pkField = table.getPrimaryKey().get(0).getName();
                params.put(pkField, null);
                //
                long etalonRecId = dbu.insertRec(mergePlan.tableName, params);


                // DEL - Сохранияем то, что нужно удалить
                if (doDelete) {
                    recordsDeleteSave(mergePlan.tableName, mergePlan.recordsDelete, resultWriter);
                }

                // UPD - Сохранияем то, где нужно перебить ссылки
                recordsRelocateSave(mergePlan.tableName, mergePlan.recordsDelete, resultWriter);


                // UPD - Перебиваем ссылки у зависимых таблиц
                recordsRelocateExec(mergePlan.tableName, mergePlan.recordsDelete, etalonRecId);

                // DEL - Удаляем лишние (теперь уже) записи
                if (doDelete) {
                    recordsDeleteExec(mergePlan.tableName, mergePlan.recordsDelete);
                }


                //
                done = done + 1;
                if (done % 10 == 0) {
                    log.info("done: " + done + "/" + count);
                }

                // Порция commit
                done_portion = done_portion + 1;
                if (done_portion >= COMMIT_SIZE) {
                    db.commit();
                    db.startTran();
                    //
                    done_portion = 0;
                }
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

    /**
     * Десериализация значений полей (перед записью в БД)
     *
     * @param valuesStr Данные в строковом виде (из XML или JSON)
     * @return Типизированные данные
     */
    Map<String, Object> prepareValues(Map<String, String> valuesStr, Collection<IJdxField> fields) {
        Map<String, Object> res = new HashMap<>();

        //
        for (IJdxField field : fields) {
            String fieldName = field.getName();
            String fieldValueStr = valuesStr.get(fieldName);
            res.put(fieldName, UtXml.strToValue(fieldValueStr, field));
        }

        //
        return res;
    }

    /**
     * Сериализация значений полей (перед записью в БД)
     *
     * @param values Типизированные данные
     * @return Данные в строковом виде (для XML или JSON)
     */
    Map<String, String> prepareValuesStr(Map<String, Object> values) {
        Map<String, String> res = new HashMap<>();

        //
        for (String fieldName : values.keySet()) {
            Object fieldValue = values.get(fieldName);
            res.put(fieldName, UtXml.valueToStr(fieldValue));
        }

        //
        return res;
    }

    /**
     * Вытаскиват все, что нужно будет обновить (в разных таблицах),
     * если делать relocate/delete записи idSour в таблице tableName
     */
    public void recordsRelocateSave(String tableName, Collection<Long> recordsDelete, RecMergeResultWriter resultWriter) throws Exception {
        // Собираем зависимости
        Map<String, Collection<IJdxForeignKey>> refsToTable = getRefsToTable(tableName);

        // Обрабатываем зависимости
        for (String refTableName : refsToTable.keySet()) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();
                String sqlSelect = "select * from " + refTableName + " where " + refFieldName + " = :" + refFieldName;

                //
                for (long deleteRecId : recordsDelete) {
                    Map params = UtCnv.toMap(refFieldName, deleteRecId);

                    // Селектим как есть сейчас
                    DbQuery stUpdated = db.openSql(sqlSelect, params);

                    // Отчитаемся
                    resultWriter.writeTableItem(new MergeResultTableItem(refTableName, MergeOprType.UPD));
                    while (!stUpdated.eof()) {
                        resultWriter.writeRec(stUpdated);
                        stUpdated.next();
                    }
                }
            }
        }
    }

    public void recordsRelocateExec(String tableName, Collection<Long> recordsDelete, long etalonRecId) throws Exception {
        // Собираем зависимости
        Map<String, Collection<IJdxForeignKey>> refsToTable = getRefsToTable(tableName);

        // Обрабатываем зависимости
        for (String refTableName : refsToTable.keySet()) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();
                String sqlUpdate = "update " + refTableName + " set " + refFieldName + " = :" + refFieldName + "_NEW" + " where " + refFieldName + " = :" + refFieldName + "_OLD";

                //
                for (long deleteRecId : recordsDelete) {
                    Map params = UtCnv.toMap(
                            refFieldName + "_OLD", deleteRecId,
                            refFieldName + "_NEW", etalonRecId
                    );

                    // Апдейтим
                    db.execSql(sqlUpdate, params);
                }
            }
        }
    }

    /**
     * Сохраняем записи recordsDelete из tableName
     */
    public void recordsDeleteSave(String tableName, Collection<Long> recordsDelete, RecMergeResultWriter resultWriter) throws Exception {
        String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        String sqlSelect = "select * from " + tableName + " where " + pkField + " = :" + pkField;

        //
        resultWriter.writeTableItem(new MergeResultTableItem(tableName, MergeOprType.DEL));

        //
        for (long deleteRecId : recordsDelete) {
            Map params = UtCnv.toMap(pkField, deleteRecId);

            // Селектим как есть сейчас
            DataStore store = db.loadSql(sqlSelect, params);

            // Отчитаемся
            for (DataRecord rec : store) {
                resultWriter.writeRec(rec);
            }
        }
    }

    /**
     * Удаляем записи recordsDelete из tableName
     */
    public void recordsDeleteExec(String tableName, Collection<Long> recordsDelete) throws Exception {
        String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        String sqlDelete = "delete from " + tableName + " where " + pkField + " = :" + pkField;

        //
        for (long deleteRecId : recordsDelete) {
            Map params = UtCnv.toMap(pkField, deleteRecId);

            // Удаляем
            db.execSql(sqlDelete, params);
        }
    }


    @Override
    public void revertExecMergePlan(RecMergeResultReader resultReader) throws Exception {
        log.info("revertExecMergePlan");

        //
        try {
            db.startTran();

            //
            MergeResultTableItem tableItem = resultReader.nextResultTable();

            while (tableItem != null) {
                String tableName = tableItem.tableName;
                IJdxTable table = struct.getTable(tableName);


                //
                log.info("revertExecMergePlan, table: " + tableName);

                //
                long doneRecs = doRecs(resultReader, tableItem.tableOperation, table);

                //
                log.info("  table done: " + tableName + ", total: " + doneRecs);


                //
                tableItem = resultReader.nextResultTable();
            }


            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }

        //
        log.info("revertExecMergePlan done");
    }

    private long doRecs(RecMergeResultReader resultReader, MergeOprType tableOperation, IJdxTable table) throws Exception {
        String sql;
        if (tableOperation == MergeOprType.UPD) {
            sql = dbu.generateSqlUpdate(table.getName(), null, null);
        } else {
            sql = dbu.generateSqlInsert(table.getName(), null, null);
        }

        //
        long doneRecs = 0;

        //
        Map<String, String> rec = resultReader.nextRec();
        while (rec != null) {
            Map<String, Object> params = prepareValues(rec, table.getFields());
            db.execSql(sql, params);

            //
            doneRecs++;
            if (doneRecs % 10000 == 0) {
                log.info("  table: " + table.getName() + ", " + doneRecs);
            }

            //
            rec = resultReader.nextRec();
        }

        //
        return doneRecs;
    }


    @Override
    public Collection<RecMergePlan> prepareMergePlan(String tableName, Collection<RecDuplicate> duplicates) throws Exception {
        return prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);
    }

    /**
     * Превращение ВСЕХ дублей в план на удаление в "лоб".
     * За образец берем первую запись (на ее основе делаем новую запись),
     * а все старые записи - планируем удалить.
     */
    private Collection<RecMergePlan> prepareRemoveDuplicatesTaskAsIs(String tableName, Collection<RecDuplicate> duplicates) throws Exception {
        Collection<RecMergePlan> res = new ArrayList<>();
        //
        IJdxTable table = struct.getTable(tableName);
        String pkField = table.getPrimaryKey().get(0).getName();
        //
        GroupStrategy tableGroups = groupsStrategyStorage.getForTable(tableName);
        //
        for (RecDuplicate duplicate : duplicates) {
            // Собираем данные со всех записей:
            //  - копим recordsDelete всех записей
            //  - recordEtalon пополняется полями из всех записей
            Map<String, Object> recordEtalon = new HashMap<>();
            Collection<Long> recordsDelete = new ArrayList<>();
            // За образец берем первую запись
            recordEtalon.putAll(duplicate.records.get(0).getValues());
            //
            for (int i = 0; i < duplicate.records.size(); i++) {
                recordsDelete.add(duplicate.records.get(i).getValueLong(pkField));
                assignNotEmptyFields(duplicate.records.get(i).getValues(), recordEtalon, tableGroups);
            }
            // Задача
            RecMergePlan task = new RecMergePlan();
            task.tableName = tableName;
            task.recordEtalon.putAll(prepareValuesStr(recordEtalon));
            task.recordsDelete.addAll(recordsDelete);
            //
            res.add(task);
        }
        //
        return res;
    }

    /**
     * Запись recordRes - пополняется полями из записи record.
     * <p>
     * Реализация стартегии слияния частично заполенных полей в разных экземплярах.
     * Например у человека в одной записи есть телефон, а в другой - номер дома,
     * тогда в качестве кандидата получалась "объединанная" по полям запись.
     * <p>
     * Проработатны "антагонистичные" поля! - Иногда либо
     * 1) не все поля могут быть заполнены одновременно либо
     * 2) они заполняются в связи с друг с другом (см. "Группы связных полей" в своей докуметашке)
     * Иметь в виду, что при наличии "антагонистичных" полей усложняется выбор записи:
     * если у записи А есть "дата документа", а у записи Б есть и "дата документа" и "номер документа",
     * то нужно предпочесть ПАРУ полей из записи Б
     */
    void assignNotEmptyFields(Map<String, Object> record, Map<String, Object> recordRes, GroupStrategy groupStrategy) {
        for (String fieldNameForGroup : record.keySet()) {
            Collection<String> fieldsGroup = groupStrategy.getForField(fieldNameForGroup);

            // Ишем какая запись полнее заполнена для группы полй fieldsGroup
            int recordEtalonFilledCount = 0;
            int recordFilledCount = 0;
            for (String fieldName : fieldsGroup) {
                if (!valueIsEmpty(recordRes.get(fieldName))) {
                    recordEtalonFilledCount = recordEtalonFilledCount + 1;
                }
                if (!valueIsEmpty(record.get(fieldName))) {
                    recordFilledCount = recordFilledCount + 1;
                }
            }

            // Если запись record полнее заполнена, то заполняем поля в recordRes
            if (recordFilledCount > recordEtalonFilledCount) {
                for (String fieldName : fieldsGroup) {
                    recordRes.put(fieldName, record.get(fieldName));
                }

            }
        }
    }

    /**
     * Учитывает, что ссылок ИЗ таблицы на другую таблицу бывает более одной.
     *
     * @return Список ссылок из всех таблиц, которыессылаются на таблицу tableName
     */
    private Map<String, Collection<IJdxForeignKey>> getRefsToTable(String tableName) {
        Map<String, Collection<IJdxForeignKey>> res = new HashMap<>();

        //
        IJdxTable table = struct.getTable(tableName);

        //
        for (IJdxTable refTable : struct.getTables()) {
            Collection<IJdxForeignKey> tableFkList = new ArrayList<>();
            for (IJdxForeignKey refTableFk : refTable.getForeignKeys()) {
                if (refTableFk.getTable().getName().equals(table.getName())) {
                    tableFkList.add(refTableFk);
                }
            }
            if (tableFkList.size() != 0) {
                res.put(refTable.getName(), tableFkList);
            }
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
