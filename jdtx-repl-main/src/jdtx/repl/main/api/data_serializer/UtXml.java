package jdtx.repl.main.api.data_serializer;

import jandcode.utils.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.joda.time.*;

import java.util.*;

public class UtXml {

    public static String valueToStr(Object value) {
        String fieldValueStr;

        //
        if (value == null) {
            fieldValueStr = null; //UtXml.valueToStr(fieldValue);
        } else if (value instanceof byte[]) {
            // Особая сериализация для BLOB
            byte[] blob = (byte[]) value;
            fieldValueStr = UtString.encodeBase64(blob);
        } else if (value instanceof DateTime) {
            // Сериализация с или без timezone
            // todo: Проверить сериализацию и десериализацию с/без timezone
            fieldValueStr = UtDate.toString((DateTime) value);
        } else {
            // Обычная сериализация
            fieldValueStr = String.valueOf(value);
        }

        //
        return fieldValueStr;
    }

    public static Object strToValue(String valueStr, IJdxField field) {
        if (valueStr == null) {
            return null;
        }

        // Поле - BLOB?
        if (field.getJdxDatatype() == JdxDataType.BLOB) {
            byte[] valueBlob = UtString.decodeBase64(valueStr);
            return valueBlob;
        }

        // Поле - дата/время?
        if (field.getJdxDatatype() == JdxDataType.DATETIME) {
            DateTime valueDateTime = UtJdxData.dateTimeValueOf(valueStr);
            return valueDateTime;
        }

        // 
        if (field.getJdxDatatype() == JdxDataType.DOUBLE) {
            double valueDouble = UtJdxData.doubleValueOf(valueStr);
            return valueDouble;
        }

        if (UtAuditApplyer.getDataType(field.getDbDatatype()) == DataType.INT) {
            int valueInteger = UtJdxData.intValueOf(valueStr);
            return valueInteger;
        }

        if (UtAuditApplyer.getDataType(field.getDbDatatype()) == DataType.LONG) {
            long valueLong = UtJdxData.longValueOf(valueStr);
            return valueLong;
        }

        // Просто поле, без изменений
        return valueStr;
    }

    /**
     * Запись в писателя значений всех полей, которые не null.
     */
    public static void recToWriter(Map<String, String> recValuesStr, String recFieldNames, JdxReplicaWriterXml writer) throws Exception {
        for (String fieldName : recFieldNames.split(",")) {
            String fieldValueStr = recValuesStr.get(fieldName);
            if (fieldValueStr != null) {
                writer.writeRecValue(fieldName, fieldValueStr);
            }
        }
    }

}
