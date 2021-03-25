package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.joda.time.*;

import java.util.*;

/**
 * Утилиты по слиянию записей (исполнитель)
 */
public class UtRecMerge implements IUtRecMerge {

    public static final boolean DO_DELETE = true;
    public static final boolean UPDATE_ONLY = false;

    Db db;
    JdxDbUtils dbu;
    IJdxDbStruct struct;
    public GroupsStrategyStorage groupsStrategyStorage;

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

            // Не ищем дубли для записи, если пусты те её поля, по которым надо искать (поля, перечисленные в fieldNames)
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
    public MergeResultTableMap execMergePlan(Collection<RecMergePlan> plans, boolean doDelete) throws Exception {
        MergeResultTableMap result = new MergeResultTableMap();

        //
        for (RecMergePlan mergePlan : plans) {
            db.startTran();
            try {
                //
                MergeResultTable taskResultForTable = result.addForTable(mergePlan.tableName);

                // INS - Создаем эталонную запись
                Map params = prepareParams(mergePlan.recordEtalon, struct.getTable(mergePlan.tableName));
                //
                String pkField = struct.getTable(mergePlan.tableName).getPrimaryKey().get(0).getName();
                params.put(pkField, null);
                //
                long etalonRecId = dbu.insertRec(mergePlan.tableName, params);

                // UPD - Перебиваем ссылки у зависимых таблиц
                recordsRelocateRefs(mergePlan.tableName, etalonRecId, mergePlan.recordsDelete, taskResultForTable);

                // DEL - Удаляем лишние (теперь уже) записи
                if (doDelete) {
                    recordsDelete(mergePlan.tableName, mergePlan.recordsDelete, taskResultForTable);
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

    // Подготовка recParams - значений полей для записи в БД
    Map prepareParams(Map recValues, IJdxTable table) {
        Map recParams = new HashMap();

        //
        for (IJdxField publicationField : table.getFields()) {
            String publicationFieldName = publicationField.getName();
            IJdxField field = table.getField(publicationFieldName);

            // Поле - BLOB?
            if (UtAuditApplyer.getDataType(field.getDbDatatype()) == DataType.BLOB) {
                String blobBase64 = (String) recValues.get(publicationFieldName);
                byte[] blob = UtString.decodeBase64(blobBase64);
                recParams.put(publicationFieldName, blob);
                continue;
            }

            // Поле - дата/время?
            if (UtAuditApplyer.getDataType(field.getDbDatatype()) == DataType.DATETIME) {
                String valueStr = (String) recValues.get(publicationFieldName);
                DateTime value = null;
                if (valueStr != null && valueStr.length() != 0) {
                    value = new DateTime(valueStr);
                }
                recParams.put(publicationFieldName, value);
                continue;
            }

            // Просто поле, без изменений
            recParams.put(publicationFieldName, recValues.get(publicationFieldName));
        }

        //
        return recParams;
    }

    /**
     * Перемещатель id записей
     */
    public void relocateId(String tableName, long idSour, long idDest) throws Exception {
        if (idSour == idDest) {
            throw new XError("Error relocateId: idSour == idDest");
        }
        if (idSour == 0) {
            throw new XError("Error relocateId: idSour == 0");
        }
        if (idDest == 0) {
            throw new XError("Error relocateId: idDest == 0");
        }


        db.startTran();
        try {
            // Проверяем, что idSour не пустая
            String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();
            String sql = "select * from " + tableName + " where " + pkField + " = :" + pkField;
            DataRecord recSour = dbu.loadSqlRec(sql, UtCnv.toMap(pkField, idSour));

            // Копируем запись tableName.idSour в tableName.idDest
            recSour.setValue(pkField, idDest);
            dbu.insertRec(tableName, recSour.getValues());

            //
            UtRecMerge utrm = new UtRecMerge(db, struct);
            MergeResultTable taskResultForTable = new MergeResultTable();
            ArrayList<Long> recordsDelete = new ArrayList<>();
            recordsDelete.add(idSour);

            // Перебиваем ссылки у зависимых таблиц с tableName.idSour на tableName.idDest
            utrm.recordsRelocateRefs(tableName, idDest, recordsDelete, taskResultForTable);

            // Удаляем старую запись tableName.idSour
            utrm.recordsDelete(tableName, recordsDelete, taskResultForTable);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }


    // Вытаскиват все, что нужно будет обновить (в разных таблицах),
    // если делать relocate/delete записи idSour в таблице tableName
    public MergeResultTable recordsRelocateFindRefs(String tableName, long idSour) throws Exception {
        MergeResultTable relocateCheckResult = new MergeResultTable();

        // Проверяем, что idSour не пустая
        String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        String sql = "select * from " + tableName + " where " + pkField + " = :" + pkField;
        relocateCheckResult.recordsDeleted = db.loadSql(sql, UtCnv.toMap(pkField, idSour));

        // Проверяем все ссылки tableName.idSour на tableName.idDest
        Map<String, Collection<IJdxForeignKey>> refsToTable = getRefsToTable(tableName);
        for (String refTableName : refsToTable.keySet()) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();
                //
                String sqlSelect = "select * from " + refTableName + " where " + refFieldName + " = :" + refFieldName + "_SOUR";
                Map paramsSelect = UtCnv.toMap(refFieldName + "_SOUR", idSour);
                DataStore refData = db.loadSql(sqlSelect, paramsSelect);
                // Селектим как есть сейчас
                RecordsUpdated recordsUpdateInfo = relocateCheckResult.recordsUpdated.getOrAddForTable(refTableName, refFieldName);
                // Отчитаемся
                if (recordsUpdateInfo.recordsUpdated == null) {
                    recordsUpdateInfo.recordsUpdated = refData;
                } else {
                    UtData.copyStore(refData, recordsUpdateInfo.recordsUpdated);
                }
            }
        }

        //
        return relocateCheckResult;
    }


    // UPD - Перебиваем ссылки у зависимых таблиц tableName с записей recordsDelete на запись etalonRecId
    public void recordsRelocateRefs(String tableName, long etalonRecId, Collection<Long> recordsDelete, MergeResultTable taskResultForTable) throws Exception {
        // Собираем зависимости
        Map<String, Collection<IJdxForeignKey>> refsToTable = getRefsToTable(tableName);

        // Обрабатываем зависимости
        for (String refTableName : refsToTable.keySet()) {
            Collection<IJdxForeignKey> fkList = refsToTable.get(refTableName);
            for (IJdxForeignKey fk : fkList) {
                String refFieldName = fk.getField().getName();

                //
                RecordsUpdated taskRecResult = taskResultForTable.recordsUpdated.getOrAddForTable(refTableName, refFieldName);

                //
                String sqlUpdate = "update " + refTableName + " set " + refFieldName + " = :" + refFieldName + "_NEW" + " where " + refFieldName + " = :" + refFieldName + "_OLD";
                String sqlSelect = "select * from " + refTableName + " where " + refFieldName + " = :" + refFieldName + "_OLD";

                //
                for (long deleteRecId : recordsDelete) {
                    Map params = UtCnv.toMap(
                            refFieldName + "_OLD", deleteRecId,
                            refFieldName + "_NEW", etalonRecId
                    );

                    // Селектим как есть сейчас
                    DataStore stUpdated = db.loadSql(sqlSelect, params);

                    // Апдейтим
                    db.execSql(sqlUpdate, params);

                    // Отчитаемся
                    if (taskRecResult.recordsUpdated == null) {
                        taskRecResult.recordsUpdated = stUpdated;
                    } else {
                        UtData.copyStore(stUpdated, taskRecResult.recordsUpdated);
                    }
                }
            }
        }
    }

    // DEL - Удаляем записи recordsDelete из tableName
    public void recordsDelete(String tableName, Collection<Long> recordsDelete, MergeResultTable taskResultForTable) throws Exception {
        String pkField = struct.getTable(tableName).getPrimaryKey().get(0).getName();
        //
        String sqlSelect = "select * from " + tableName + " where " + pkField + " = :" + pkField;
        String sqlDelete = "delete from " + tableName + " where " + pkField + " = :" + pkField;

        //
        for (long deleteRecId : recordsDelete) {
            Map params = UtCnv.toMap(pkField, deleteRecId);

            // Селектим как есть сейчас
            DataStore refData = db.loadSql(sqlSelect, params);
            // Отчитаемся
            if (taskResultForTable.recordsDeleted == null) {
                taskResultForTable.recordsDeleted = refData;
            } else {
                UtData.copyStore(refData, taskResultForTable.recordsDeleted);
            }

            // Удаляем
            db.execSql(sqlDelete, params);
        }
    }


    @Override
    public void revertExecMergePlan(MergeResultTableMap taskResults) {
        // todo: реализовать
        throw new XError("Not implemented");
    }


    @Override
    public Collection<RecMergePlan> prepareMergePlan(String tableName, Collection<RecDuplicate> duplicates) throws Exception {
        return prepareRemoveDuplicatesTaskAsIs(tableName, duplicates);
    }

    /**
     * Превращение ВСЕХ дублей в план на удаление в "лоб".
     * За образец берем первую запись (на ее основе делаем новую запись), все записи - планируем удалить.
     */
    private Collection<RecMergePlan> prepareRemoveDuplicatesTaskAsIs(String tableName, Collection<RecDuplicate> duplicates) throws Exception {
        IJdxTable table = struct.getTable(tableName);
        String pkField = table.getPrimaryKey().get(0).getName();
        //
        Collection<RecMergePlan> res = new ArrayList<>();
        //
        for (RecDuplicate duplicate : duplicates) {
            RecMergePlan task = new RecMergePlan();
            //
            task.tableName = tableName;
            task.recordEtalon = duplicate.records.get(0).getValues();
            //
            GroupStrategy tableGroups = groupsStrategyStorage.getForTable(tableName);
            //
            for (int i = 0; i < duplicate.records.size(); i++) {
                task.recordsDelete.add(duplicate.records.get(i).getValueLong(pkField));
                // Запись task.recordEtalon - пополняется полями из всех записей
                assignNotEmptyFields(duplicate.records.get(i).getValues(), task.recordEtalon, tableGroups);
            }
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
