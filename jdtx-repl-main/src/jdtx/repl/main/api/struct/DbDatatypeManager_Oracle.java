package jdtx.repl.main.api.struct;

public class DbDatatypeManager_Oracle extends DbDatatypeManager implements IDbDatatypeManager {

    @Override
    public JdxDataType dbDatatypeToJdxDatatype(String dbDataType, int columnSize, int decimalDigits) throws Exception {
        String dataType = dbDataType.toUpperCase();

        switch (dataType) {
            case "CHAR":
            case "NCHAR":
            case "VARCHAR2":
            case "NVARCHAR2": {
                return JdxDataType.STRING;
            }
            case "ROWID":
            case "UROWID ": {
                return JdxDataType.STRING;
            }
            case "DATE": {
                return JdxDataType.DATETIME;
            }
            case "RAW":
            case "BLOB":
            case "CLOB":
            case "NCLOB": {
                return JdxDataType.BLOB;
            }
            case "INTEGER":
            case "SHORTINTEGER":
            case "LONGINTEGER": {
                return JdxDataType.INTEGER;
            }
            case "FLOAT":
            case "DECIMAL":
            case "SHORTDECIMAL": {
                return JdxDataType.DOUBLE;
            }
            case "NUMBER": {
                if (decimalDigits == 0) {
                    return JdxDataType.INTEGER;
                } else {
                    return JdxDataType.DOUBLE;
                }
            }

            default: {
                throw new Exception("Неизвестный тип поля: " + dbDataType);
            }
        }
    }

}
