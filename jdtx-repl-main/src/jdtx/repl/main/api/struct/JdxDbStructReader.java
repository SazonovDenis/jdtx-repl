package jdtx.repl.main.api.struct;


import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;

import java.sql.*;

public class JdxDbStructReader implements IJdxDbStructReader {

    Db db;

    public void setDb(Db db) {
        this.db = db;
    }


    public IJdxDbStruct readDbStruct() throws Exception {
        return readDbStruct(true);
    }


    public IJdxDbStruct readDbStruct(boolean skipReplObj) throws Exception {
        //создаем экземпляр класса JdxDbStruct
        JdxDbStruct struct = new JdxDbStruct();


        DatabaseMetaData dbm = db.getConnection().getMetaData();
        String[] types = {"TABLE"};
        ResultSet rs = dbm.getTables(null, null, "%", types);
        while (rs.next()) {
            if (skipReplObj && rs.getString("TABLE_NAME").toLowerCase().startsWith(JdxUtils.prefix.toLowerCase())) {
                continue;
            }

            //для очередной таблицы
            //создаем экземпляр класса
            JdxTableStruct table = new JdxTableStruct();
            //добавляем экземпляр в список
            struct.getTables().add(table);

            //
            table.setName(rs.getString("TABLE_NAME"));

            //String tableMy = table.getName();
            ResultSet rsColumns = dbm.getColumns(null, null, table.getName(), null);
            ResultSet rsPK = dbm.getPrimaryKeys(null, null, table.getName());

            // --- столбцы
            while (rsColumns.next()) {
                JdxFieldStruct field = new JdxFieldStruct();
                table.getFields().add(field);

                String columnName = rsColumns.getString("COLUMN_NAME");
                String columnType = rsColumns.getString("TYPE_NAME");
                int columnSize = rsColumns.getInt("COLUMN_SIZE");

                field.setName(columnName);
                field.setDbDatatype(columnType);
                field.setJdxDatatype(dbDatatypeToJdxDatatype(columnType));
                field.setSize(columnSize);
            }

            // --- первичные ключи
            while (rsPK.next()) {
                String columnName = rsPK.getString("COLUMN_NAME");
                JdxFieldStruct fieldPK = (JdxFieldStruct) table.getField(columnName);
                fieldPK.setIsPrimaryKey(true);
                table.getPrimaryKey().add(fieldPK);
            }
        }

        // --- внешние ключи
        for (IJdxTableStruct table : struct.getTables()) {
            ResultSet rsFK = dbm.getImportedKeys(db.getConnection().getCatalog(), null, table.getName());
            while (rsFK.next()) {
                JdxForeignKey foreignKey = new JdxForeignKey();
                table.getForeignKeys().add(foreignKey);

                JdxTableStruct tableFK = (JdxTableStruct) struct.getTable(rsFK.getString("PKTABLE_NAME"));
                IJdxFieldStruct tableFieldFK = tableFK.getField(rsFK.getString("PKCOLUMN_NAME"));
                IJdxFieldStruct fieldFK = ((JdxTableStruct) table).getField(rsFK.getString("FKCOLUMN_NAME"));

                foreignKey.setField(fieldFK);
                foreignKey.setTable(tableFK);
                foreignKey.setTableField(tableFieldFK);
            }
        }

        //
        return struct;
    }


    public static boolean isStringField(String dbDataType) {
        if (dbDataType.compareToIgnoreCase("CHAR") == 0 || dbDataType.compareToIgnoreCase("CHARACTER") == 0 || dbDataType.compareToIgnoreCase("VARCHAR") == 0) {
            return true;
        } else {
            return false;
        }
    }

    public static JdxDataType dbDatatypeToJdxDatatype(String dbDataType) throws Exception {
        String dataType = dbDataType.toUpperCase();
        if (dataType.compareToIgnoreCase("BLOB") == 0) {
            return JdxDataType.BLOB;
        } else if (dataType.compareToIgnoreCase("BLOB SUB_TYPE 0") == 0) {
            return JdxDataType.BLOB;
        } else if (dataType.compareToIgnoreCase("CHAR") == 0) {
            return JdxDataType.STRING;
        } else if (dataType.compareToIgnoreCase("CHARACTER") == 0) {
            return JdxDataType.STRING;
        } else if (dataType.compareToIgnoreCase("VARCHAR") == 0) {
            return JdxDataType.STRING;
        } else if (dataType.compareToIgnoreCase("DATE") == 0) {
            return JdxDataType.DATETIME;
        } else if (dataType.compareToIgnoreCase("DECIMAL") == 0) {
            return JdxDataType.DOUBLE;
        } else if (dataType.compareToIgnoreCase("FLOAT") == 0) {
            return JdxDataType.DOUBLE;
        } else if (dataType.compareToIgnoreCase("INT64") == 0) {
            return JdxDataType.INTEGER;
        } else if (dataType.compareToIgnoreCase("INTEGER") == 0) {
            return JdxDataType.INTEGER;
        } else if (dataType.compareToIgnoreCase("NCHAR") == 0) {
            return JdxDataType.STRING;
        } else if (dataType.compareToIgnoreCase("NUMERIC") == 0) {
            return JdxDataType.DOUBLE;
        } else if (dataType.compareToIgnoreCase("SMALLINT") == 0) {
            return JdxDataType.INTEGER;
        } else if (dataType.compareToIgnoreCase("BIGINT") == 0) {
            return JdxDataType.INTEGER;
        } else if (dataType.compareToIgnoreCase("TIME") == 0) {
            return JdxDataType.DATETIME;
        } else if (dataType.compareToIgnoreCase("TIMESTAMP") == 0) {
            return JdxDataType.DATETIME;
        } else {
            throw new Exception("Неизвестный тип поля: " + dbDataType);
        }
    }


}