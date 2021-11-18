package jdtx.repl.main.api.util;

import groovy.json.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.struct.*;
import org.joda.time.*;

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
            fieldValueStr = UtStringEscape.escapeJava(String.valueOf(value));
        }

        //
        return fieldValueStr;
    }

    public static Object strToValue(String valueStr, IJdxField field) {
        if (valueStr == null) {
            return null;
        }

        // Поле - BLOB?
        if (UtAuditApplyer.getDataType(field.getDbDatatype()) == DataType.BLOB) {
            byte[] valueBlob = UtString.decodeBase64(valueStr);
            return valueBlob;
        }

        // Поле - дата/время?
        if (UtAuditApplyer.getDataType(field.getDbDatatype()) == DataType.DATETIME) {
            DateTime valueDateTime = UtJdx.dateTimeValueOf(valueStr);
            return valueDateTime;
        }

        // Просто поле, без изменений
        return StringEscapeUtils.unescapeJava(valueStr);
    }

}
