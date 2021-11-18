package jdtx.repl.main.api.data_binder;

import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;


public class JdxDataSerializer_plain extends JdxDataSerializer_custom {

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
