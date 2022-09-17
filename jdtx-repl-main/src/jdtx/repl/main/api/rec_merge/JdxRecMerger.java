package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.pk_generator.*;
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
    IDbErrors dbErrors;

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxRecMerger");

    //
    public JdxRecMerger(Db db, IJdxDbStruct struct, IJdxDataSerializer dataSerializer) throws Exception {
        this.db = db;
        this.struct = struct;
        this.dbu = new JdxDbUtils(db, struct);
        this.dataSerializer = dataSerializer;
        this.dbErrors = db.service(DbErrorsService.class);
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


    @Override
    public void execMergePlan(Collection<RecMergePlan> plans, File resultFile) throws Exception {
        UtRecMerger utRecMerger = new UtRecMerger(db, struct);

        // Не затирать существующий
        if (resultFile.exists()) {
            throw new XError("Result file already exists: " + resultFile.getCanonicalPath());
        }

        // Начинаем писать результат выполнения задачи
        RecMergeResultWriter resultWriter = new RecMergeResultWriter();

        //
        try {
            int count = plans.size();
            int done = 0;
            int done_portion = 0;

            //
            resultWriter.open(resultFile);

            //
            db.startTran();

            //
            for (RecMergePlan mergePlan : plans) {
                log.info("execMergePlan, table: " + mergePlan.tableName);
                log.info("  recordsDelete.count: " + mergePlan.recordsDelete.size());
                log.info("  recordEtalon: " + mergePlan.recordEtalon);

                //
                IJdxTable table = struct.getTable(mergePlan.tableName);
                String tableFieldNamesStr = UtJdx.fieldsToString(table.getFields());
                IJdxField pkField = table.getPrimaryKey().get(0);
                String pkFieldName = pkField.getName();

                // INS - Создаем эталонную запись.

                // Таблица и поля в Serializer-е
                dataSerializer.setTable(table, tableFieldNamesStr);
                Map<String, Object> values = dataSerializer.prepareValues(mergePlan.recordEtalon);
                //
                Long insertedRecId = dbu.insertOrUpdate(mergePlan.tableName, values, tableFieldNamesStr);
                //
                Long etalonRecId = (Long) values.get(pkFieldName);
                if (etalonRecId == null) {
                    etalonRecId = insertedRecId;
                }

                // Распаковываем PK удаляемых записей
                List<Long> recordsDelete = new ArrayList<>();
                for (String recordDeletePkStr : mergePlan.recordsDelete) {
                    Long recordDeletePk = UtJdxData.longValueOf(dataSerializer.prepareValue(recordDeletePkStr, pkField));
                    recordsDelete.add(recordDeletePk);
                }

                // DEL - Сохраняем то, что нужно удалить
                utRecMerger.saveRecordsTable(mergePlan.tableName, recordsDelete, dataSerializer, resultWriter);

                // UPD - Сохраняем то, где нужно перебить ссылки
                utRecMerger.loadRecordsRefTable(mergePlan.tableName, recordsDelete, dataSerializer, resultWriter, MergeOprType.UPD);


                // UPD - Перебиваем ссылки у зависимых таблиц
                utRecMerger.execRecordsUpdateRefs(mergePlan.tableName, recordsDelete, etalonRecId);

                // DEL - Удаляем лишние (теперь уже) записи
                utRecMerger.execRecordsDelete(mergePlan.tableName, recordsDelete);


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

            // Завершаем писать
            resultWriter.close();
        } catch (Exception e) {
            db.rollback();
            resultWriter.close();
            throw e;
        }


    }


    @Override
    public void revertExec(File resultFile) throws Exception {
        log.info("revertExec");

        //
        RecMergeResultReader resultReader = new RecMergeResultReader(new FileInputStream(resultFile));

        //
        try {
            db.startTran();

            //
            MergeResultTableItem tableItem = resultReader.nextResultTable();

            while (tableItem != null) {
                String tableName = tableItem.tableName;
                IJdxTable table = struct.getTable(tableName);


                //
                log.info("revertExec, table: " + tableName);

                //
                dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));
                long doneRecs = revertTableRecs(table, tableItem.tableOperation, resultReader);

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
        resultReader.close();

        //
        log.info("revertExec done");
    }

    private long revertTableRecs(IJdxTable table, MergeOprType tableOperation, RecMergeResultReader resultReader) throws Exception {
        String sql;
        if (tableOperation == MergeOprType.UPD) {
            sql = dbu.generateSqlUpdate(table.getName(), null, null);
        } else {
            sql = dbu.generateSqlInsert(table.getName(), null, null);
        }

        //
        long doneRecs = 0;

        //
        Map<String, String> recValuesStr = resultReader.nextRec();
        while (recValuesStr != null) {
            Map<String, Object> recValues = dataSerializer.prepareValues(recValuesStr);
            try {
                db.execSql(sql, recValues);
            } catch (Exception e) {
                if (!dbErrors.errorIs_PrimaryKeyError(e)) {
                    log.error(e.getMessage());
                    log.error("table: " + table.getName());
                    log.error("oprType: " + tableOperation);
                    log.error("recParams: " + recValues);
                    log.error("recValuesStr: " + recValuesStr);
                    throw e;
                }
            }

            //
            doneRecs++;
            if (doneRecs % 10000 == 0) {
                log.info("  table: " + table.getName() + ", " + doneRecs);
            }

            //
            recValuesStr = resultReader.nextRec();
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
        // Таблица и поля в Serializer-е
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

            // "Эталонная" запись должна быть именно ВСТАВЛЕНА, а не выбранной из уже существующих,
            // т.к. на рабочей станции может НЕ ОКАЗАТЬСЯ в наличии той записи,
            // которую на сервере назначили как "эталонная",
            // поэтому id эталонной будет null.
            recordEtalon.put(pkFieldName, null);

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
