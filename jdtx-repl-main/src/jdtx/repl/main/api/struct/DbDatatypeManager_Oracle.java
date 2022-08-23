package jdtx.repl.main.api.struct;

import jandcode.dbm.db.*;

public class DbDatatypeManager_Oracle implements IDbDatatypeManager {

    Db db;

    @Override
    public void setDb(Db db) throws Exception {
        this.db = db;
    }

    @Override
    public JdxDataType dbDatatypeToJdxDatatype(String dbDataType) throws Exception {
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
            case "SHORTDECIMAL":
            case "NUMBER": {
                return JdxDataType.DOUBLE;
            }
            default: {
                throw new Exception("Неизвестный тип поля: " + dbDataType);
            }
        }
    }

}