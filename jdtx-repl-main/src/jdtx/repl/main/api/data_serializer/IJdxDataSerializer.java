package jdtx.repl.main.api.data_serializer;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public interface IJdxDataSerializer {

    // todo: почему table как объект, а поля - как строка?
    void setTable(IJdxTable table, String tableFieldNamesStr);

    String prepareValueStr(Object fieldValue, IJdxField field) throws Exception;

    Object prepareValue(String fieldValueStr, IJdxField field) throws Exception;

    Map<String, String> prepareValuesStr(Map<String, Object> values) throws Exception;

    Map<String, Object> prepareValues(Map<String, String> valuesStr) throws Exception;

}
