package jdtx.repl.main.api.data_filler;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

public class DataFiller implements IDataFiller {

    Db db;
    IJdxDbStruct struct;
    Random rnd;

    public Map<String, Object> generatorsCache = new HashMap<>();

    public DataFiller(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
        this.rnd = new Random();
    }

    @Override
    public Map<String, Object> genRecord(IJdxTable table, Map generators) {
        Map<String, Object> rec = new HashMap<>();

        for (IJdxField field : table.getFields()) {
            String fieldName = field.getName();
            Object value = genValue(field, generators.get(fieldName));
            rec.put(fieldName, value);
        }

        return rec;
    }

    /**
     * Для ссылочных полей загрузит допустимые значения ссылок из БД
     */
    @Override
    public Map<String, Object> createGenerators(IJdxTable table, Map generatorsDefault) throws Exception {
        HashMapNoCase<Object> res = new HashMapNoCase<>();

        String tableName = table.getName();

        for (IJdxField field : table.getFields()) {
            String fieldName = field.getName();

            String keyField = getTableFieldKey(table, field);
            String keyDatatype = getDatatypeKey(field);

            // Поищем генератор в кэшах
            if (generatorsCache.containsKey(keyField)) {
                res.put(fieldName, generatorsCache.get(keyField));
                continue;
            }
            if (generatorsCache.containsKey(keyDatatype)) {
                res.put(fieldName, generatorsCache.get(keyDatatype));
                continue;
            }

            if (generatorsDefault != null && generatorsDefault.containsKey(fieldName)) {
                res.put(fieldName, generatorsDefault.get(fieldName));
                continue;
            }

            // Определим заполнятель сами
            Object generator;
            IJdxTable refTable = field.getRefTable();
            if (refTable != null) {
                // Правильно подготовим набор значений для ссылочных полей
                generator = getRefValuesSet(refTable);
                // Закэшируем набор значений - повторно ссылки искать - дорого
                generatorsCache.put(keyDatatype, generator);
            } else {
                generator = getTemplateByDatatype(field);
            }
            res.put(fieldName, generator);
        }

        return res;
    }

    public Map<String, Object> genTemplates(IJdxTable table) throws Exception {
        return createGenerators(table, null);
    }

    private String getDatatypeKey(IJdxField field) {
        String datatype;

        IJdxTable refTable = field.getRefTable();
        if (refTable != null) {
            datatype = "ref:" + refTable.getName();
        } else {
            datatype = field.getJdxDatatype().toString();
        }

        return "datatype:" + datatype;
    }

    private String getTableFieldKey(IJdxTable table, IJdxField field) {
        return "field:" + table.getName() + "." + field.getName();
    }

    private Object getTemplateByDatatype(IJdxField field) {
        IFieldValueGenerator generator;

        switch (field.getJdxDatatype()) {
            case DOUBLE:
            case INTEGER:
                generator = new FieldValueGenerator_Number();
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
