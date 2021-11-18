package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.data_serializer.UtData;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.io.*;
import java.util.*;

/**
 * Утилиты по поиску и слиянию дубликатов записей (исполнитель)
 */
public class JdxRecMerger implements IJdxRecMerger {


    int COMMIT_SIZE = 100;

    Db db;
    IJdxDbStruct struct;
    JdxDbUtils dbu;
    IJdxDataSerializer dataSerializer;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxRecMerger");

    //
    public JdxRecMerger(Db db, IJdxDbStruct struct, IJdxDataSerializer dataSerializer) {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
        this.dataSerializer = dataSerializer;
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
    public Collection<RecDuplicate> findTableDuplicates(String tableName, String tableFields, boolean useNullValues) throws Exception {
        List<RecDuplicate> resList = new ArrayList<>();

        //
        String sqlAll = "select * from " + tableName + " order by id";
        String sqlRec = "select * from " + tableName + " where ";

        //
        Collection<String> fieldNames = Arrays.asList(tableFields.split(","));

        //
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

            // Не ищем дубли для записи, если пусты НЕКОТОРЫЕ её поля, по которым надо искать (поля, перечисленные в tableFields)
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
        boolean doDelete = true;  // Жалко удалять режим, но пока не понятно, зачем он нужен практически

        //
        UtRecMerger utRecMerger = new UtRecMerger(db, struct);

        //
        try {
            int count = plans.size();
            int done = 0;
            int done_portion = 0;

            //
            db.startTran();

            //
            for (RecMergePlan mergePlan : plans) {
                log.info("execMergePlan, table: " + mergePlan.tableName);
                log.info("  recordsDelete.count: " + mergePlan.recordsDelete.size());
                log.info("  recordEtalon: " + mergePlan.recordEtalon);


                // INS - Создаем эталонную запись.
                // "Эталонная" запись должна быть именно ВСТАВЛЕНА, а не выбранной из уже существующих,
                // т.к. на рабочей станции может НЕ ОКАЗАТЬСЯ в наличии той записи,
                // которую на сервере назначили как "эталонная".
                IJdxTable table = struct.getTable(mergePlan.tableName);
                dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));
                Map<String, Object> values = dataSerializer.prepareValues(mergePlan.recordEtalon);
                // Чтобы вставилось с новой Id
                IJdxField pkField = table.getPrimaryKey().get(0);
                String pkFieldName = pkField.getName();
                values.put(pkFieldName, null);
                //
                long etalonRecId = dbu.insertRec(mergePlan.tableName, values);

                // Распаковываем PK удаляемых записей
                Collection<Long> recordsDelete = new ArrayList<>();
                for (String recordDeletePkStr : mergePlan.recordsDelete) {
                    Long recordDeletePk = UtData.longValueOf(dataSerializer.prepareValue(recordDeletePkStr, pkField));
                    recordsDelete.add(recordDeletePk);
                }

                // DEL - Сохранияем то, что нужно удалить
                if (doDelete) {
                    utRecMerger.recordsDeleteSave(mergePlan.tableName, recordsDelete, dataSerializer, resultWriter);
                }

                // UPD - Сохранияем то, где нужно перебить ссылки
                utRecMerger.recordsRelocateSave(mergePlan.tableName, recordsDelete, dataSerializer, resultWriter);


                // UPD - Перебиваем ссылки у зависимых таблиц
                utRecMerger.recordsRelocateExec(mergePlan.tableName, recordsDelete, etalonRecId);

                // DEL - Удаляем лишние (теперь уже) записи
                if (doDelete) {
                    utRecMerger.recordsDeleteExec(mergePlan.tableName, recordsDelete);
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
                dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));
                long doneRecs = revertRecs(resultReader, tableItem.tableOperation, table);

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

    private long revertRecs(RecMergeResultReader resultReader, MergeOprType tableOperation, IJdxTable table) throws Exception {
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
            Map<String, Object> params = dataSerializer.prepareValues(rec);
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
     * <p>
     * "Эталонная" запись должна быть именно ВСТАВЛЕНА, а не выбранной из уже существующих,
     * т.к. на рабочей станции может НЕ ОКАЗАТЬСЯ в наличии той записи,
     * которую на сервере назначили как "эталонная".
     */
    private Collection<RecMergePlan> prepareRemoveDuplicatesTaskAsIs(String tableName, Collection<RecDuplicate> duplicates) throws Exception {
        Collection<RecMergePlan> res = new ArrayList<>();
        //
        IJdxTable table = struct.getTable(tableName);
        IJdxField pkField = table.getPrimaryKey().get(0);
        String pkFieldName = pkField.getName();
        //
        dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));
        //
        GroupStrategy tableGroups = GroupsStrategyStorage.getInstance().getForTable(tableName);
        //
        for (RecDuplicate duplicate : duplicates) {
            Map<String, Object> recordEtalon = new HashMap<>();
            Collection<String> recordsDelete = new ArrayList<>();

            // За образец берем первую запись
            recordEtalon.putAll(duplicate.records.get(0).getValues());

            // Собираем данные со всех записей:
            for (DataRecord recordDuplicate : duplicate.records) {
                // Копим recordsDelete со всех записей
                Long recordDuplicatePk = recordDuplicate.getValueLong(pkFieldName);
                String recordDuplicatePkStr = dataSerializer.prepareValueStr(recordDuplicatePk, pkField);
                recordsDelete.add(recordDuplicatePkStr);
                // Пополняеем recordEtalon полями из всех записей
                assignNotEmptyFields(recordDuplicate.getValues(), recordEtalon, tableGroups);
            }

            // Задача
            RecMergePlan task = new RecMergePlan();
            task.tableName = tableName;
            task.recordEtalon.putAll(dataSerializer.prepareValuesStr(recordEtalon));
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
