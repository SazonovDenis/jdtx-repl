package jdtx.repl.main.api.data_serializer;

import jdtx.repl.main.api.struct.*;

import java.util.*;


public class JdxDataSerializerCustom extends DataSerializerService implements IJdxDataSerializer {

    protected IJdxTable table = null;
    protected Collection<IJdxField> fields = null;

    @Override
    public void setTable(IJdxTable table, String tableFieldNamesStr) {
        this.table = table;
        fields = new ArrayList<>();
        for (String fieldName : tableFieldNamesStr.split(",")) {
            fields.add(table.getField(fieldName));
        }
    }

    @Override
    public String prepareValueStr(Object fieldValue, IJdxField field) throws Exception {
        return null;
    }

    @Override
    public Object prepareValue(String fieldValueStr, IJdxField field) throws Exception {
        return null;
    }

    /**
     * Сериализация значений полей (перед записью в БД).
     * С запаковкой ссылок.
     *
     * @param values Типизированные данные
     * @return Данные в строковом виде (для XML или JSON)
     */
    @Override
    public Map<String, String> prepareValuesStr(Map<String, Object> values) throws Exception {
        Map<String, String> res = new HashMap<>();

        //
        for (IJdxField field : fields) {
            String fieldName = field.getName();
            Object fieldValue = values.get(field.getName());

            //
            if (fieldValue != null) {
                String fieldValueStr = prepareValueStr(fieldValue, field);
                res.put(fieldName, fieldValueStr);
            }
        }

        //
        return res;
    }

    /**
     * Десериализация значений полей (перед записью в БД и т.п.).
     * Из строки в Object и распаковка ссылок.
     *
     * @param valuesStr Данные в строковом виде (из XML)
     * @return Типизированные данные
     */
    @Override
    public Map<String, Object> prepareValues(Map<String, String> valuesStr) throws Exception {
        Map<String, Object> res = new HashMap<>();

        //
        for (IJdxField field : fields) {
            String fieldName = field.getName();
            String fieldValueStr = valuesStr.get(fieldName);
            Object fieldValue = prepareValue(fieldValueStr, field);

            //
            res.put(fieldName, fieldValue);
        }

        //
        return res;
    }

    @Override
    public IJdxDataSerializer getInstance() throws Exception {
        return this;
    }

}
