package jdtx.repl.main.api.data_filler;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

public class DataFiller implements IDataFiller {

    Db db;
    IJdxDbStruct struct;
    Map<String, Object> generatorsCache;
    Map<String, Set<Long>> refValuesCache;
    UtFiller utFiller;

    @Override
    public Map<String, Object> getGeneratorsCache() {
        return generatorsCache;
    }

    @Override
    public Map<String, Set<Long>> getRefValuesCache() {
        return refValuesCache;
    }

    public DataFiller(Db db, IJdxDbStruct struct, Map<String, Object> defaultGenerators) {
        this.db = db;
        this.struct = struct;
        if (defaultGenerators == null) {
            this.generatorsCache = new HashMap<>();
        } else {
            this.generatorsCache = defaultGenerators;
        }
        this.refValuesCache = new HashMap<>();
        this.utFiller = new UtFiller(db, struct);
    }

    public DataFiller(Db db, IJdxDbStruct struct) {
        this(db, struct, null);
    }

    @Override
    public Map<String, Object> genRecord(IJdxTable table, Map<String, Object> tableGenerators) throws Exception {
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
                generator = new FieldValueGenerator_Ref(db, struct, this);
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
                generator = new FieldValueGenerator_Number(0.0, Math.pow(10, field.getSize() - 1), 3);
                break;
            case INTEGER:
                generator = new FieldValueGenerator_Number(0.0, Math.pow(10, field.getSize() - 1), 0);
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

    private Object genValue(IJdxField field, Object fieldGenerators) throws Exception {
        Object value;

        // Можно передать несколь генераторов, тут выберем один
        Object fieldGenerator = utFiller.selectOneObject(fieldGenerators);

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


}
