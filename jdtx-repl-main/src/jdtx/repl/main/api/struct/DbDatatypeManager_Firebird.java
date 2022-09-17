package jdtx.repl.main.api.struct;

public class DbDatatypeManager_Firebird extends DbDatatypeManager implements IDbDatatypeManager {

    @Override
    public JdxDataType dbDatatypeToJdxDatatype(String dbDataType, int columnSize, int decimalDigits) throws Exception {
        String dataType = dbDataType.toUpperCase();

        switch (dataType) {
            case "BLOB":
            case "BLOB SUB_TYPE 0": {
                return JdxDataType.BLOB;
            }
            case "CHAR":
            case "CHARACTER":
            case "VARCHAR":
            case "NCHAR": {
                return JdxDataType.STRING;
            }
            case "DATE":
            case "TIMESTAMP":
            case "TIME": {
                return JdxDataType.DATETIME;
            }
            case "DECIMAL":
            case "FLOAT":
            case "NUMERIC": {
                return JdxDataType.DOUBLE;
            }
            case "INT64":
            case "INTEGER":
            case "SMALLINT":
            case "BIGINT": {
                return JdxDataType.INTEGER;
            }
            // Firebird 2.5
            case "DOUBLE PRECISION": {
                return JdxDataType.DOUBLE;
            }
            default: {
                throw new Exception("Неизвестный тип поля: " + dbDataType);
            }

        }
    }

}
