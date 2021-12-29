package jdtx.repl.main.api.data_serializer;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;


public class JdxDataSerializerDecode extends JdxDataSerializerCustom {


    private IRefDecoder decoder;
    private long wsId;

    public JdxDataSerializerDecode(Db db, long wsId) throws Exception {
        this.decoder = new RefDecoder(db, wsId);
        this.wsId = wsId;
    }

    public String prepareValueStr(Object fieldValue, IJdxField field) throws Exception {
        String fieldValueStr;

        //
        IJdxTable refTable = field.getRefTable();

        //
        if (fieldValue == null) {
            fieldValueStr = null;
        } else if (field.isPrimaryKey() || refTable != null) {
            // Это поле - ссылка
            String refTableName;
            if (field.isPrimaryKey()) {
                refTableName = table.getName();
            } else {
                refTableName = refTable.getName();
            }
            // Запаковка ссылки в JdxRef
            JdxRef ref = decoder.get_ref(refTableName, UtJdxData.longValueOf(fieldValue));
            fieldValueStr = String.valueOf(ref);
        } else {
            // Поле других типов
            fieldValueStr = UtXml.valueToStr(fieldValue);
        }

        //
        return fieldValueStr;
    }

    public Object prepareValue(String fieldValueStr, IJdxField field) throws Exception {
        Object fieldValue;

        //
        IJdxTable refTable = field.getRefTable();

        //
        if (fieldValueStr == null) {
            fieldValue = null;
        } else if (field.isPrimaryKey() || refTable != null) {
            // Это поле - ссылка
            JdxRef fieldValueRef = JdxRef.parse(fieldValueStr);
            if (fieldValueRef == null) {
                // Ссылка равна null
                fieldValue = null;
            } else {
                // Распаковка ссылки в long
                String refTableName;
                if (field.isPrimaryKey()) {
                    refTableName = table.getName();
                } else {
                    refTableName = refTable.getName();
                }
                fieldValue = decoder.get_id_own(refTableName, fieldValueRef.ws_id, fieldValueRef.value);
            }
        } else {
            // Поле других типов
            fieldValue = UtXml.strToValue(fieldValueStr, field);
        }

        //
        return fieldValue;
    }


}
