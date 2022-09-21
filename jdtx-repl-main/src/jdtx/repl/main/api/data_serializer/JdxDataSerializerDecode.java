package jdtx.repl.main.api.data_serializer;

import jdtx.repl.main.api.ref_manager.*;
import jdtx.repl.main.api.settings.*;
import jdtx.repl.main.api.struct.*;


public class JdxDataSerializerDecode extends JdxDataSerializerCustom {

    // Кто занимается настройками
    private IWsSettings wsSettings = null;

    // Кто занимается ссылками
    private IRefManager decoder = null;

    // Рабочая станция по умолчанию (для десериализации  локальных ссылок).
    private long wsIdDefault = 0;

    public IRefManager getDecoder() throws Exception {
        checkInit();
        return decoder;
    }

    public long getWsIdDefault() throws Exception {
        checkInit();
        return wsIdDefault;
    }

    private void checkInit() throws Exception {
        if (wsSettings == null) {
            this.wsSettings = getApp().service(WsSettingsService.class);
            this.decoder = getApp().service(RefManagerService.class);
            this.wsIdDefault = wsSettings.getWsId();
        }
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
            JdxRef ref = getDecoder().get_ref(refTableName, UtJdxData.longValueOf(fieldValue));
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
                // Так бывает, если план слияния был составлен (и сохранен в xml) локально
                // (и поэтому все ссылки локальные), а мы хотим отправить план на филиалы
                // (для чего нужно превратить наши локальные ссылки в глобальные).
                // Выполним дополнение ws_id, если пришла локальная ссылка, а не глобальная.
                if (fieldValueRef.isEmptyWs() && getWsIdDefault() > 0) {
                    fieldValueRef.ws_id = getWsIdDefault();
                }
                // Распаковка ссылки в long
                String refTableName;
                if (field.isPrimaryKey()) {
                    refTableName = table.getName();
                } else {
                    refTableName = refTable.getName();
                }
                fieldValue = getDecoder().get_id_local(refTableName, fieldValueRef);
            }
        } else {
            // Поле других типов
            fieldValue = UtXml.strToValue(fieldValueStr, field);
        }

        //
        return fieldValue;
    }


}
