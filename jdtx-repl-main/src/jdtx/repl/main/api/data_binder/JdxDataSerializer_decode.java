package jdtx.repl.main.api.data_binder;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;


public class JdxDataSerializer_decode extends JdxDataSerializer_custom {


    private IRefDecoder decoder;
    private long wsId;

    public JdxDataSerializer_decode(Db db, long wsId) throws Exception {
        this.decoder = new RefDecoder(db, wsId);
        this.wsId = wsId;
    }

    public String prepareValueStr(Object fieldValue, IJdxField field) throws Exception {
        String fieldValueStr;

        //
        if (fieldValue == null) {
            fieldValueStr = null; //UtXml.valueToStr(fieldValue);
        } else {
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
                fieldValueStr = String.valueOf(ref);
            } else {
                // Это значение других типов
                fieldValueStr = UtXml.valueToStr(fieldValue);
            }
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


}
