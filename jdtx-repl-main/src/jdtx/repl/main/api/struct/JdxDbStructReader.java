package jdtx.repl.main.api.struct;


import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;

import java.sql.*;
import java.util.*;

public class JdxDbStructReader implements IJdxDbStructReader {

    Db db;

    public void setDb(Db db) {
        this.db = db;
    }


    public IJdxDbStruct readDbStruct() throws Exception {
        return readDbStruct(true);
    }


    public IJdxDbStruct readDbStruct(boolean skipReplObj) throws Exception {
        List<IJdxTableStruct> structTables = new ArrayList<>();

        //
        DatabaseMetaData metaData = db.getConnection().getMetaData();
        String[] types = {"TABLE"};
        ResultSet rs = metaData.getTables(null, null, "%", types);
        try {
            while (rs.next()) {
                if (skipReplObj && rs.getString("TABLE_NAME").toLowerCase().startsWith(JdxUtils.audit_table_prefix.toLowerCase())) {
                    continue;
                }

                //для очередной таблицы
                //создаем экземпляр класса
                JdxTableStruct table = new JdxTableStruct();
                //добавляем экземпляр в список
                structTables.add(table);

                //
                table.setName(rs.getString("TABLE_NAME"));

                // --- столбцы
                ResultSet rsColumns = metaData.getColumns(null, null, table.getName(), null);
                try {
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
                } finally {
                    rsColumns.close();
                }

                // --- первичные ключи
                ResultSet rsPK = metaData.getPrimaryKeys(null, null, table.getName());
                try {
                    while (rsPK.next()) {
                        String columnName = rsPK.getString("COLUMN_NAME");
                        JdxFieldStruct fieldPK = (JdxFieldStruct) table.getField(columnName);
                        fieldPK.setIsPrimaryKey(true);
                        table.getPrimaryKey().add(fieldPK);
                    }
                } finally {
                    rsPK.close();
                }
            }
        } finally {
            rs.close();
        }

        // --- внешние ключи
        for (IJdxTableStruct table : structTables) {
            ResultSet rsFK = metaData.getImportedKeys(db.getConnection().getCatalog(), null, table.getName());
            try {
                while (rsFK.next()) {
                    // Пополняем список ForeignKey для таблицы
                    JdxForeignKey foreignKey = new JdxForeignKey();
                    table.getForeignKeys().add(foreignKey);

                    JdxTableStruct tableFK = (JdxTableStruct) findTable(structTables, rsFK.getString("PKTABLE_NAME"));
                    IJdxFieldStruct tableFieldFK = tableFK.getField(rsFK.getString("PKCOLUMN_NAME"));
                    JdxFieldStruct fieldFK = (JdxFieldStruct) table.getField(rsFK.getString("FKCOLUMN_NAME"));
                    String name = rsFK.getString("FK_NAME");

                    foreignKey.setName(name);
                    foreignKey.setField(fieldFK);
                    foreignKey.setTable(tableFK);
                    foreignKey.setTableField(tableFieldFK);

                    // Прставляем данные, на какую таблицу смотрит ссылочное поле
                    fieldFK.setRefTable(tableFK);
                }
            } finally {
                rsFK.close();
            }

        }

        // Сортируем
        List<IJdxTableStruct> structTablesSorted = JdxUtils.sortTables(structTables);


        // Создаем и возвращаем экземпляр класса JdxDbStruct
        JdxDbStruct struct = new JdxDbStruct();
        struct.getTables().addAll(structTablesSorted);

        //
        return struct;
    }

    private IJdxTableStruct findTable(List<IJdxTableStruct> tables, String tableName) {
        for (IJdxTableStruct t : tables) {
            if (t.getName().compareToIgnoreCase(tableName) == 0) {
                return t;
            }
        }
        return null;
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