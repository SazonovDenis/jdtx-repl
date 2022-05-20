package jdtx.repl.main.api.struct;


import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;

import java.sql.*;
import java.util.*;

/**
 * Читаем структуру базы в IJdxDbStruct
 */
public class JdxDbStructReader implements IJdxDbStructReader {

    Db db;

    public void setDb(Db db) {
        this.db = db;
    }


    public IJdxDbStruct readDbStruct() throws Exception {
        return readDbStruct(true);
    }


    public IJdxDbStruct readDbStruct(boolean skipReplObj) throws Exception {
        List<IJdxTable> structTables = new ArrayList<>();

        //
        DatabaseMetaData metaData = db.getConnection().getMetaData();
        String[] types = {"TABLE"};
        ResultSet rs = metaData.getTables(null, null, "%", types);
        try {
            while (rs.next()) {
                if (skipReplObj && rs.getString("TABLE_NAME").toLowerCase().startsWith(UtJdx.AUDIT_TABLE_PREFIX.toLowerCase())) {
                    continue;
                }

                //для очередной таблицы
                //создаем экземпляр класса
                JdxTable table = new JdxTable();
                //добавляем экземпляр в список
                structTables.add(table);

                //
                table.setName(rs.getString("TABLE_NAME"));

                // --- столбцы
                ResultSet rsColumns = metaData.getColumns(null, null, table.getName(), null);
                try {
                    while (rsColumns.next()) {
                        JdxField field = new JdxField();
                        table.getFields().add(field);

                        String columnName = rsColumns.getString("COLUMN_NAME");
                        String columnType = rsColumns.getString("TYPE_NAME");
                        int columnSize = rsColumns.getInt("COLUMN_SIZE");
                        boolean isNullable = rsColumns.getBoolean("NULLABLE");

                        field.setName(columnName);
                        field.setDbDatatype(columnType);
                        field.setJdxDatatype(dbDatatypeToJdxDatatype(columnType));
                        field.setSize(columnSize);
                        field.setIsNullable(isNullable);
                    }
                } finally {
                    rsColumns.close();
                }

                // --- первичные ключи
                ResultSet rsPK = metaData.getPrimaryKeys(null, null, table.getName());
                try {
                    while (rsPK.next()) {
                        String columnName = rsPK.getString("COLUMN_NAME");
                        String pkName = rsPK.getString("PK_NAME");
                        JdxField fieldPK = (JdxField) table.getField(columnName);
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
        for (IJdxTable table : structTables) {
            ResultSet rsFK = metaData.getImportedKeys(db.getConnection().getCatalog(), null, table.getName());
            try {
                while (rsFK.next()) {
                    // Пополняем список ForeignKey для таблицы
                    JdxForeignKey foreignKey = new JdxForeignKey();
                    table.getForeignKeys().add(foreignKey);

                    JdxTable tableFK = (JdxTable) findTable(structTables, rsFK.getString("PKTABLE_NAME"));
                    IJdxField tableFieldFK = tableFK.getField(rsFK.getString("PKCOLUMN_NAME"));
                    JdxField fieldFK = (JdxField) table.getField(rsFK.getString("FKCOLUMN_NAME"));
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

        // Сортируем таблицы по зависимостям
        List<IJdxTable> structTablesSorted = UtJdx.sortTablesByReference(structTables);


        // Создаем и возвращаем экземпляр класса JdxDbStruct
        JdxDbStruct struct = new JdxDbStruct();
        struct.getTables().addAll(structTablesSorted);

        //
        return struct;
    }

    private IJdxTable findTable(List<IJdxTable> tables, String tableName) {
        for (IJdxTable t : tables) {
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
            // Firebird 2.5
        } else if (dataType.compareToIgnoreCase("DOUBLE PRECISION") == 0) {
            return JdxDataType.DOUBLE;
        } else {
            throw new Exception("Неизвестный тип поля: " + dbDataType);
        }
    }


}