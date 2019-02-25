package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.sql.*;
import java.util.*;

/**
 * Манипуляции с базой - CRUD
 */
public class DbUtils {

    Db db;
    IJdxDbStruct struct;

    static String ID_FIELD = "ID";

    public DbUtils(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    /**
     * Возвращает очередную id для генератора generatorName
     */
    public long getNextGenerator(String generatorName) throws SQLException {
        return getNextGenerator(generatorName, 1);
    }

    /**
     * Возвращает текущую id для генератора generatorName
     */
    public long getCurrId(String generatorName) throws SQLException {
        return getNextGenerator(generatorName, 0);
    }

    private long getNextGenerator(String generatorName, int increment) throws SQLException {
        Statement st = db.getConnection().createStatement();
        ResultSet rs = null;
        try {
            String sql = "select gen_id(" + generatorName + ", " + increment + ") as id from dual";
            rs = st.executeQuery(sql);
            if (rs.next()) {
                return rs.getLong("id");
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception ex) {
            }
            st.close();
        }
        return 0;
    }

    /**
     * Генерация sql на вставку
     *
     * @param tableName    таблица в базе
     * @param ignoreFields список игнорируемых полей
     * @return текст sql
     */
    public String generateSqlInsert(String tableName, Object fields, Object ignoreFields) {
        List<String> ilist = null;
        if (ignoreFields != null) {
            ilist = makeFieldsList(ignoreFields);
        }
        //
        IJdxTableStruct table = struct.getTable(tableName);
        if (fields == null) {
            fields = table.getFields();
        }
        List<String> fieldsList = makeFieldsList(fields);
        //
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ");
        sb.append(table.getName());
        sb.append("(");
        int cnt = 0;
        for (String fieldName : fieldsList) {
            if (containsInList(ilist, fieldName)) {
                continue;
            }
            if (fieldName.equalsIgnoreCase(ID_FIELD)) {
                continue;
            }
            if (cnt != 0) {
                sb.append(",");
            }
            sb.append(fieldName);
            //
            cnt++;
        }
        //
        if (cnt != 0) {
            sb.append(",");
        }
        sb.append(ID_FIELD);
        //
        cnt = 0;
        sb.append(") values (");
        for (String fieldName : fieldsList) {
            if (containsInList(ilist, fieldName)) {
                continue;
            }
            if (fieldName.equalsIgnoreCase(ID_FIELD)) {
                continue;
            }
            if (cnt != 0) {
                sb.append(",");
            }
            sb.append(":");
            sb.append(fieldName);
            //
            cnt++;
        }
        //
        if (cnt != 0) {
            sb.append(",");
        }
        sb.append(":");
        sb.append(ID_FIELD);
        //
        sb.append(")");
        //
        return sb.toString();
    }

    /**
     * Генерация sql для update
     *
     * @param tableName     таблица в базе
     * @param excludeFields список игнорируемых полей
     * @return текст sql
     */
    public String generateSqlUpdate(String tableName, Object fields, Object excludeFields) {
        List<String> excludeList = null;
        if (excludeFields != null) {
            excludeList = makeFieldsList(excludeFields);
        }
        IJdxTableStruct table = struct.getTable(tableName);
        if (fields == null) {
            fields = table.getFields();
        }
        List<String> fieldsList = makeFieldsList(fields);

        //
        StringBuilder sb = new StringBuilder();
        sb.append("update ");
        sb.append(table.getName());
        sb.append(" set ");
        int cnt = 0;
        for (String fieldName : fieldsList) {
            if (containsInList(excludeList, fieldName)) {
                continue;
            }
            if (fieldName.equalsIgnoreCase(ID_FIELD)) {
                continue;
            }
            if (cnt != 0) {
                sb.append(",");
            }
            sb.append(fieldName);
            sb.append("=");
            sb.append(":");
            sb.append(fieldName);

            cnt++;
        }

        sb.append(" where ").append(ID_FIELD).append("=").append(":").append(ID_FIELD);
        return sb.toString();
    }

    /**
     * Генерация sql для delete
     *
     * @param tableName таблица в базе
     * @return текст sql
     */
    public String generateSqlDelete(String tableName) {
        IJdxTableStruct table = struct.getTable(tableName);
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(table.getName());
        sb.append(" where ").append(ID_FIELD).append("=").append(":").append(ID_FIELD);
        return sb.toString();
    }


    /**
     * Делаем список полей из объекта
     *
     * @param fields поля. Может быть строкой через ',', Map
     * @return список полей
     */
    private static List<String> makeFieldsList(Object fields) {
        List<String> res = new ArrayList<String>();
        if (fields instanceof Map) {
            Map t = (Map) fields;
            for (Object f : t.keySet()) {
                res.add(toString(f));
            }
        } else {
            List t = toList(fields, ",");
            for (Object f : t) {
                res.add(toString(f));
            }
        }
        return res;
    }

    private static boolean containsInList(List<String> lst, String s) {
        if (lst == null) {
            return false;
        }
        for (String s1 : lst) {
            if (s1.equalsIgnoreCase(s)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Конвертация в string
     */
    public static String toString(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String)
            return (String) value;
        else if (value instanceof IJdxFieldStruct)
            return ((IJdxFieldStruct) value).getName();
        else
            return value.toString();
    }

    /**
     * Преобразование объекта в список строк.
     * Для null - возвращает пустой список
     * Для String - разбирает ее на части через delimiter и делает список из строк
     * Для остальных - список из одного переданного значения преобразованного в строку
     */
    public static List<String> toList(Object source, String delimiter) {
        ArrayList<String> res = new ArrayList<String>();
        if (source instanceof Collection) {
            for (Object re : (Collection) source) {
                if (re != null) {
                    String s = toString(re);
                    if (!empty(s)) {
                        res.add(s);
                    }
                }
            }
        } else if (source instanceof CharSequence) {
            String v = source.toString();
            if (v.length() > 0) {
                String[] ar = v.split(delimiter);
                for (String a : ar) {
                    String s = a.trim();
                    if (!empty(s)) {
                        res.add(s);
                    }
                }
            }
        } else if (source != null) {
            res.add(source.toString());
        }
        return res;
    }

    /**
     * Проверка на пустую строку
     *
     * @param value строка
     * @return true, если value пустая строка или null
     */
    public static boolean empty(String value) {
        return (value == null) || (value.length() == 0);
    }


    public void deleteRec(String tableName, long id) throws Exception {
        String sql = generateSqlDelete(tableName);
        Map p = new HashMapNoCase();
        p.put(ID_FIELD, id);
        db.execSql(sql, p);
    }

    public void updateRec(String tableName, Map params, String includeFields, String excludeFields) throws Exception {
        String sql = generateSqlUpdate(tableName, includeFields, excludeFields);
        Map p = new HashMapNoCase();
        p.putAll(params);
        db.execSql(sql, p);
    }

    public void updateRec(String tableName, Map params, String includeFields) throws Exception {
        String sql = generateSqlUpdate(tableName, includeFields, null);
        Map p = new HashMapNoCase();
        p.putAll(params);
        db.execSql(sql, p);
    }

    public void updateRec(String tableName, Map params) throws Exception {
        updateRec(tableName, params, null, null);
    }

    public long insertRec(String tableName, Map params, String includeFields, String excludeFields) throws Exception {
        Map p = new HashMapNoCase();
        p.putAll(params);
        long id = UtCnv.toLong(p.get(ID_FIELD));
        if (id == 0) {
            id = getTableNextId(tableName);
            p.put(ID_FIELD, id);
        }
        //
        String sql = generateSqlInsert(tableName, includeFields, excludeFields);
        db.execSql(sql, p);
        return id;
    }

    public long insertRec(String tableName, Map params, String includeFields) throws Exception {
        return insertRec(tableName, params, includeFields, null);
    }

    public long insertRec(String tableName, Map params) throws Exception {
        return insertRec(tableName, params, null, null);
    }

    /**
     * Генерация Id по правилам базы
     *
     * @param tableName
     * @return
     * @throws SQLException
     */
    private long getTableNextId(String tableName) throws SQLException {
        return getNextGenerator("g_" + tableName);
    }
}
