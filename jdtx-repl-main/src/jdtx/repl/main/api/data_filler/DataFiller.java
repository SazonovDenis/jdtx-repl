package jdtx.repl.main.api.data_filler;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

public class DataFiller implements IDataFiller {

    Db db;
    IJdxDbStruct struct;
    Map<String, Object> generatorsCache;
    Random rnd;

    @Override
    public Map<String, Object> getGeneratorsCache() {
        return generatorsCache;
    }

    public DataFiller(Db db, IJdxDbStruct struct, Map<String, Object> defaultGenerators) {
        this.db = db;
        this.struct = struct;
        if (defaultGenerators == null) {
            this.generatorsCache = new HashMap<>();
        } else {
            this.generatorsCache = defaultGenerators;
        }
        this.rnd = new Random();
    }

    public DataFiller(Db db, IJdxDbStruct struct) {
        this(db, struct, null);
    }

    @Override
    public Map<String, Object> genRecord(IJdxTable table, Map<String, Object> tableGenerators) {
        Map<String, Object> rec = new HashMap<>();

        for (IJdxField field : table.getFields()) {
            String fieldName = field.getName();
            Object value = genValue(field, tableGenerators.get(fieldName));
            rec.put(fieldName, value);
        }

        return rec;
    }

    /**
     * Для ссылочных полей загрузит допустимые значения ссылок из БД
     */
    @Override
    public Map<String, Object> createGenerators(IJdxTable table, Map<String, Object> tableGenerators) throws Exception {
        HashMapNoCase<Object> res = new HashMapNoCase<>();

        for (IJdxField field : table.getFields()) {
            String fieldName = field.getName();

            String keyTableField = getTableFieldKey(table, field);
            String keyField = getFieldKey(field);

            // Поищем генератор в общих кэшах
            if (generatorsCache.containsKey(keyTableField)) {
                res.put(fieldName, generatorsCache.get(keyTableField));
                continue;
            }
            if (generatorsCache.containsKey(keyField)) {
                res.put(fieldName, generatorsCache.get(keyField));
                continue;
            }

            // Поищем генератор в генераторах для таблицы
            if (tableGenerators != null && tableGenerators.containsKey(fieldName)) {
                res.put(fieldName, tableGenerators.get(fieldName));
                continue;
            }

            // Определим генератор сами
            Object generator;
            IJdxTable refTable = field.getRefTable();
            if (refTable != null) {
                // Правильно подготовим набор значений для ссылочных полей
                generator = getRefValuesSet(refTable);
                // Закэшируем набор значений - повторно ссылки искать - дорого
                generatorsCache.put(keyField, generator);
            } else {
                generator = createGeneratorByDatatype(field);
            }
            res.put(fieldName, generator);
        }

        return res;
    }

    public Map<String, Object> createGenerators(IJdxTable table) throws Exception {
        return createGenerators(table, null);
    }

    @Override
    public String getFieldKey(IJdxField field) {
        String datatypeKey;

        IJdxTable refTable = field.getRefTable();
        if (refTable != null) {
            datatypeKey = getRefKey(refTable);
        } else {
            datatypeKey = getDatatypeKey(field);
        }

        return datatypeKey;
    }

    @Override
    public String getTableFieldKey(IJdxTable table, IJdxField field) {
        return "field:" + table.getName() + "." + field.getName();
    }

    @Override
    public String getRefKey(IJdxTable refTable) {
        return "ref:" + refTable.getName();
    }

    @Override
    public String getDatatypeKey(IJdxField field) {
        return "datatype:" + field.getJdxDatatype().toString();
    }

    private Object createGeneratorByDatatype(IJdxField field) {
        IFieldValueGenerator generator;

        switch (field.getJdxDatatype()) {
            case DOUBLE:
                generator = new FieldValueGenerator_Number(0.0, Math.pow(10, field.getSize()), 3);
                break;
            case INTEGER:
                generator = new FieldValueGenerator_Number(0.0, Math.pow(10, field.getSize()), 0);
                break;
            case STRING:
                generator = new FieldValueGenerator_String(UtString.repeat("*", field.getSize()), field.getSize());
                break;
            case BLOB:
                generator = new FieldValueGenerator_Blob();
                break;
            case DATETIME:
                generator = new FieldValueGenerator_DateTime();
                break;
            default:
                generator = new FieldValueGenerator_String(UtString.repeat("*", field.getSize()), field.getSize());
        }

        return generator;
    }

    /**
     * Для таблицы refTable собирает возможные значения ссылок на нее
     */
    private Set getRefValuesSet(IJdxTable refTable) throws Exception {
        String refTableName = refTable.getName();
        IJdxField pkField = refTable.getPrimaryKey().get(0);
        String pkFieldName = pkField.getName();
        DataStore refSt = db.loadSql("select distinct " + pkFieldName + " as id from " + refTableName);
        return UtData.uniqueValues(refSt, "id");
    }

    private Object genValue(IJdxField field, Object fieldsTemplates) {
        Object value;

        // Можно передать несколь генераторов, тут выберем один
        Object fieldGenerator = selectOneGenerator(fieldsTemplates);

        if (fieldGenerator instanceof IFieldValueGenerator) {
            // Можно передать заполнятель
            IFieldValueGenerator generator = (IFieldValueGenerator) fieldGenerator;
            value = generator.genValue(field);
        } else {
            // А можно передать и значение
            value = fieldGenerator;
        }

        return value;
    }

    private Object selectOneGenerator(Object generators) {
        Object generator;

        if (generators instanceof List) {
            List<Object> fieldTemplatesList = (List) generators;
            int rndIdx = rnd.nextInt(fieldTemplatesList.size());
            generator = fieldTemplatesList.get(rndIdx);
        } else if (generators instanceof Set) {
            Set<Object> fieldTemplatesSet = (Set) generators;
            int idx = rnd.nextInt(fieldTemplatesSet.size());
            generator = fieldTemplatesSet.toArray()[idx];
        } else {
            generator = generators;
        }

        return generator;
    }


}
