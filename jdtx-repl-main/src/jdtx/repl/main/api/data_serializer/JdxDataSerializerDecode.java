package jdtx.repl.main.api.data_serializer;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;


public class JdxDataSerializerDecode extends JdxDataSerializerCustom {


    private IRefDecoder decoder;

    // Рабочая станция по умолчанию для десериализации  локальных ссылок.
    private long wsIdDefault;

    public JdxDataSerializerDecode(Db db, long wsIdDefault) throws Exception {
        this.decoder = new RefDecoder(db, wsIdDefault);
        this.wsIdDefault = wsIdDefault;
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
                // Сюда может прийти как глобальная (полная) ссылка,
                // например "12:1324", так и локальная, например "101000001324".
                // Например, так бывает, если план слияния был составлен (и сохранен в xml) локально
                // (и поэтому все ссылки локальные), а мы хотим отправить план на филиалы
                // (для чего нужно превратить наши ломкальные ссылки в глобальные).
                // Выполним дополнение ws_id, если пришла локальная ссылка, а не глобальная.
                if (fieldValueRef.isEmptyWs() && this.wsIdDefault > 0) {
                    fieldValueRef.ws_id = wsIdDefault;
                }
                // Распаковка ссылки в long
                String refTableName;
                if (field.isPrimaryKey()) {
                    refTableName = table.getName();
                } else {
                    refTableName = refTable.getName();
                }
                fieldValue = decoder.get_id_local(refTableName, fieldValueRef);
            }
        } else {
            // Поле других типов
            fieldValue = UtXml.strToValue(fieldValueStr, field);
        }

        //
        return fieldValue;
    }


}
