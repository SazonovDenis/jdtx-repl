package jdtx.repl.main.api.struct;


import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;

import java.sql.*;
import java.util.*;

/**
 * Читаем структуру базы в IJdxDbStruct
 */
public class JdxDbStructReader implements IJdxDbStructReader {

    private Db db;

    private IDbDatatypeManager dbDatatypeManager;

    public void setDb(Db db) throws Exception {
        this.db = db;

        String dbDriver = db.getDbSource().getDbDriver().getName();
        switch (dbDriver) {
            //case "firebird": {
            case "jdbc": {
                dbDatatypeManager = new DbDatatypeManager_Firebird();
                break;
            }
            case "oracle": {
                dbDatatypeManager = new DbDatatypeManager_Oracle();
                break;
            }
            default: {
                throw new Exception("Неизвестный тип базы: " + dbDriver);
            }
        }
        dbDatatypeManager.setDb(db);
    }


    public IJdxDbStruct readDbStruct() throws Exception {
        return readDbStruct(true);
    }


    public IJdxDbStruct readDbStruct(boolean skipReplObj) throws Exception {
        List<IJdxTable> structTables = new ArrayList<>();

        //
        DatabaseMetaData metaData = db.getConnection().getMetaData();
        String schema = db.getDbSource().getUsername();

        // ---
        // Таблицы

        //
        //System.out.println("tables");

        String[] types = {"TABLE"};
        ResultSet rs = metaData.getTables(null, schema, "%", types);
        try {
            while (rs.next()) {
                if (skipReplObj && isReplTable(rs)) {
                    continue;
                }

                //
                //System.out.println(rs.getString("TABLE_SCHEM") + " / " + rs.getString("TABLE_NAME") + ", " + rs.getString("TABLE_TYPE"));

                //
                String tableName = rs.getString("TABLE_NAME");

                // Создаем экземпляр
                JdxTable table = new JdxTable();
                structTables.add(table);

                //
                table.setName(tableName);
            }
        } finally {
            rs.close();
        }

        //
        JdxDbStruct struct = new JdxDbStruct();
        struct.getTables().addAll(structTables);


        // ---
        // Столбцы

        //
        //System.out.println("table cols");

        ResultSet rsColumns = metaData.getColumns(null, schema, null, null);
        try {
            while (rsColumns.next()) {
                //System.out.println(rsColumns.getString("TABLE_NAME") + "." + rsColumns.getString("COLUMN_NAME"));

                //
                String tableName = rsColumns.getString("TABLE_NAME");
                String columnName = rsColumns.getString("COLUMN_NAME");
                String columnType = rsColumns.getString("TYPE_NAME");
                int columnSize = rsColumns.getInt("COLUMN_SIZE");
                int decimalDigits = rsColumns.getInt("DECIMAL_DIGITS");
                boolean isNullable = rsColumns.getBoolean("NULLABLE");

                // Пополняем список полей для таблицы (table == null если tableName это не таблица, а view или что-то аналогичное)
                IJdxTable table = struct.getTable(tableName);
                if (table != null) {
                    JdxField field = new JdxField();
                    table.getFields().add(field);
                    //
                    field.setName(columnName);
                    field.setDbDatatype(columnType);
                    field.setJdxDatatype(dbDatatypeManager.dbDatatypeToJdxDatatype(columnType, columnSize, decimalDigits));
                    field.setSize(columnSize);
                    field.setIsNullable(isNullable);
                }
            }
        } finally {
            rsColumns.close();
        }

        //
        //System.out.println("primary keys");

        // --- Первичные ключи
        for (IJdxTable table : structTables) {
            ResultSet rsPK = metaData.getPrimaryKeys(null, schema, table.getName());
            try {
                while (rsPK.next()) {
                    String tableName = rsPK.getString("TABLE_NAME");
                    String columnName = rsPK.getString("COLUMN_NAME");
                    String pkName = rsPK.getString("PK_NAME");

                    // Пополняем список PK для таблицы
                    JdxField fieldPK = (JdxField) table.getField(columnName);
                    fieldPK.setIsPrimaryKey(true);
                    table.getPrimaryKey().add(fieldPK);
                }
            } finally {
                rsPK.close();
            }
        }

        //
        //System.out.println("foreign keys");

        // --- внешние ключи
        for (IJdxTable table : structTables) {
            ResultSet rsFK = metaData.getImportedKeys(null, schema, table.getName());
            try {
                while (rsFK.next()) {
                    //System.out.println(table.getName() + "." + rsFK.getString("FKCOLUMN_NAME") + " > " + rsFK.getString("PKTABLE_NAME"));

                    //
                    JdxTable tableFK = (JdxTable) findTable(structTables, rsFK.getString("PKTABLE_NAME"));
                    IJdxField tableFieldFK = tableFK.getField(rsFK.getString("PKCOLUMN_NAME"));
                    JdxField fieldFK = (JdxField) table.getField(rsFK.getString("FKCOLUMN_NAME"));
                    String name = rsFK.getString("FK_NAME");

                    // Пополняем список ForeignKey для таблицы
                    JdxForeignKey foreignKey = new JdxForeignKey();
                    table.getForeignKeys().add(foreignKey);
                    //
                    foreignKey.setName(name);
                    foreignKey.setField(fieldFK);
                    foreignKey.setTable(tableFK);
                    foreignKey.setTableField(tableFieldFK);

                    // Проставляем, на какую таблицу смотрит ссылочное поле
                    fieldFK.setRefTable(tableFK);
                }
            } finally {
                rsFK.close();
            }

        }

        // Сортируем таблицы по зависимостям
        List<IJdxTable> structTablesSorted = UtJdx.sortTablesByReference(structTables);

        // Создаем и возвращаем экземпляр JdxDbStruct
        JdxDbStruct structRes = new JdxDbStruct();
        structRes.getTables().addAll(structTablesSorted);


        //
        return structRes;
    }

    private boolean isReplTable(ResultSet rs) throws SQLException {
        return rs.getString("TABLE_NAME").toLowerCase().startsWith(UtJdx.AUDIT_TABLE_PREFIX.toLowerCase());
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


}