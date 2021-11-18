package jdtx.repl.main.api.data_binder;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;

import java.util.*;


public class JdxDataSerializer_decode implements IJdxDataSerializer {


    private IJdxTable table = null;
    private String[] tableFieldNames = null;
    private Collection<IJdxField> fields = null;
    private IRefDecoder decoder;
    private long wsId;

    public JdxDataSerializer_decode(Db db, long wsId) throws Exception {
        this.decoder = new RefDecoder(db, wsId);
        this.wsId = wsId;
    }

    @Override
    public void setTable(IJdxTable table, String tableFieldNamesStr) {
        this.table = table;
        this.tableFieldNames = tableFieldNamesStr.split(",");

        fields = new ArrayList<>();
        for (String fieldName : this.tableFieldNames) {
            fields.add(table.getField(fieldName));
        }
    }

    public String prepareValueStr(Object fieldValue, IJdxField field) throws Exception {
        String fieldValueStr;

        //
        IJdxTable refTable = field.getRefTable();
        if (field.isPrimaryKey() || refTable != null) {
            // Это поле - ссылка
            String refTableName;
            if (field.isPrimaryKey()) {
                refTableName = table.getName();
            } else {
                refTableName = refTable.getName();
            }
            // Запаковка ссылки в JdxRef
            JdxRef ref = decoder.get_ref(refTableName, UtJdx.longValueOf(fieldValue));
            fieldValueStr = ref.toString();
        } else {
            // Это значение других типов
            fieldValueStr = UtXml.valueToStr(fieldValue);
        }

        //
        return fieldValueStr;
    }

    public Object prepareValue(String fieldValueStr, IJdxField field) throws Exception {
        Object fieldValue;

        // Поле - ссылка?
        IJdxTable refTable = field.getRefTable();
        if (fieldValueStr != null && (field.isPrimaryKey() || refTable != null)) {
            // Это поле - ссылка
            JdxRef fieldValueRef = JdxRef.parse(fieldValueStr);
            // Дополнение ws_id для ссылки
            if (fieldValueRef.ws_id == -1) {
                if (wsId != -1) {
                    fieldValueRef.ws_id = wsId;
                    throw new XError("prepareValue, fieldValueRef.ws_id is not set");
                } else {
                    throw new XError("prepareValue, wsIdDefault is not set");
                }
            }
            // Распаковка ссылки в long
            String refTableName;
            if (field.isPrimaryKey()) {
                refTableName = table.getName();
            } else {
                refTableName = refTable.getName();
            }
            fieldValue = decoder.get_id_own(refTableName, fieldValueRef.ws_id, fieldValueRef.value);
        } else {
            // Поле других типов
            fieldValue = UtXml.strToValue(fieldValueStr, field);
        }

        //
        return fieldValue;
    }

    /**
     * Сериализация значений полей (перед записью в БД).
     * С запаковкой ссылок.
     *
     * @param values Типизированные данные
     * @return Данные в строковом виде (для XML или JSON)
     */
    public Map<String, String> prepareValuesStr(Map<String, Object> values) throws Exception {
        Map<String, String> res = new HashMap<>();

        //
        for (IJdxField field : fields) {
            String fieldName = field.getName();
            Object fieldValue = values.get(field.getName());
            String fieldValueStr = prepareValueStr(fieldValue, field);

            //
            res.put(fieldName, fieldValueStr);
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

}
