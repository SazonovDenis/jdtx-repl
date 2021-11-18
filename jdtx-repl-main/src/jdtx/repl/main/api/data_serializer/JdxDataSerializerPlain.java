package jdtx.repl.main.api.data_serializer;

import jdtx.repl.main.api.struct.*;


public class JdxDataSerializerPlain extends JdxDataSerializerCustom {

    public String prepareValueStr(Object fieldValue, IJdxField field) throws Exception {
        String fieldValueStr;

        // Это значение других типов
        fieldValueStr = UtXml.valueToStr(fieldValue);

        //
        return fieldValueStr;
    }

    public Object prepareValue(String fieldValueStr, IJdxField field) throws Exception {
        Object fieldValue;

        // Поле других типов
        fieldValue = UtXml.strToValue(fieldValueStr, field);

        //
        return fieldValue;
    }

}
