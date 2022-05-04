package jdtx.repl.main.api.data_serializer;

import org.joda.time.*;

public class UtJdxData {

    public static Long longValueOf(Object value) {
        return longValueOf(value, null);
    }

    public static Integer intValueOf(Object value) {
        return intValueOf(value, null);
    }

    public static Double doubleValueOf(Object value) {
        return doubleValueOf(value, null);
    }

    public static String stringValueOf(Object value, String valueIfNull) {
        String valueString;
        if (value == null) {
            valueString = valueIfNull;
        } else {
            valueString = value.toString();
            if (valueString.length() == 0) {
                valueString = valueIfNull;
            } else if (valueString.compareToIgnoreCase("null") == 0) {
                valueString = valueIfNull;
            }
        }
        return valueString;
    }

    public static Long longValueOf(Object value, Long valueIfNull) {
        Long valueLong;
        if (value == null) {
            valueLong = valueIfNull;
        } else if (value instanceof Long) {
            valueLong = (Long) value;
        } else if (value instanceof Integer) {
            valueLong = Long.valueOf((Integer) value);
        } else {
            String valueString = value.toString();
            if (valueString.length() == 0) {
                valueLong = valueIfNull;
            } else if (valueString.compareToIgnoreCase("null") == 0) {
                valueLong = valueIfNull;
            } else {
                valueLong = Long.valueOf(valueString);
            }
        }
        return valueLong;
    }

    public static Integer intValueOf(Object value, Integer valueIfNull) {
        Integer valueInteger;
        if (value == null) {
            valueInteger = valueIfNull;
        } else if (value instanceof Integer) {
            valueInteger = (Integer) value;
        } else if (value instanceof Long) {
            // В value может оказаться long, но на самом деле - не более чем int, например в ReplicaInfo.fromJSONObject, в infoJson.get("replicaType") оказывается Long
            valueInteger = Integer.valueOf(value.toString());
        } else {
            String valueString = value.toString();
            if (valueString.length() == 0) {
                valueInteger = valueIfNull;
            } else if (valueString.compareToIgnoreCase("null") == 0) {
                valueInteger = valueIfNull;
            } else {
                valueInteger = Integer.valueOf(valueString);
            }
        }
        return valueInteger;
    }

    public static Double doubleValueOf(Object value, Double valueIfNull) {
        Double valueDouble;
        if (value == null) {
            valueDouble = valueIfNull;
        } else if (value instanceof Double) {
            valueDouble = (Double) value;
        } else if (value instanceof Integer) {
            valueDouble = Double.valueOf(value.toString());
        } else if (value instanceof Long) {
            valueDouble = Double.valueOf(value.toString());
        } else {
            String valueString = value.toString();
            if (valueString.length() == 0) {
                valueDouble = valueIfNull;
            } else if (valueString.compareToIgnoreCase("null") == 0) {
                valueDouble = valueIfNull;
            } else {
                valueDouble = Double.valueOf(valueString);
            }
        }
        return valueDouble;
    }

    public static Boolean booleanValueOf(Object value, boolean valueIfNull) {
        Boolean valueBoolean;
        if (value == null) {
            valueBoolean = valueIfNull;
        } else if (value instanceof Boolean) {
            valueBoolean = (Boolean) value;
        } else {
            String valueString = value.toString();
            if (valueString.length() == 0) {
                valueBoolean = valueIfNull;
            } else if (valueString.compareToIgnoreCase("null") == 0) {
                valueBoolean = valueIfNull;
            } else {
                valueBoolean = Boolean.valueOf(valueString);
            }
        }
        return valueBoolean;
    }

    public static DateTime dateTimeValueOf(String valueStr) {
        DateTime valueDateTime;

        if (valueStr == null) {
            valueDateTime = null;
        } else if (valueStr.compareToIgnoreCase("null") == 0) {
            valueDateTime = null;
        } else if (valueStr.length() == 0) {
            valueDateTime = null;
        } else if (valueStr.length() == 10) {
            // 2015-10-09
            LocalDate vLocalDate = new LocalDate(valueStr);
            valueDateTime = vLocalDate.toDateTimeAtStartOfDay();
        } else {
            // 2015-04-01T21:54:05.000+07:00
            valueDateTime = new DateTime(valueStr);
        }

        return valueDateTime;
    }

}
