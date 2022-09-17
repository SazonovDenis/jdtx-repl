package jdtx.repl.main.api.util;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;


/**
 * Манипуляции с базой - CRUD
 */
public class JdxDbUtils {

    Db db;
    IJdxDbStruct struct;
    IDbErrors dbErrors;
    IDbGenerators dbGenerators;
    IAppPkRules pkRules;

    public JdxDbUtils(Db db, IJdxDbStruct struct) throws Exception {
        this.db = db;
        this.struct = struct;
        this.dbErrors = db.service(DbErrorsService.class);
        this.dbGenerators = db.service(DbGeneratorsService.class);
        this.pkRules = db.getApp().service(AppPkRulesService.class);
    }

    /**
     * Возвращает очередную id для генератора generatorName
     */
    public long getNextGenerator(String generatorName) throws Exception {
        return dbGenerators.genNextValue(generatorName);
    }

    public static String getPkFieldName(IJdxTable table) {
        List<IJdxField> pkList = table.getPrimaryKey();
        if (pkList.size() == 0) {
            throw new XError("No PK field in table: " + table.getName());
        }
        if (pkList.size() > 1) {
            // todo разобраться, почему два одинаковых PK в этой базе читается
            //////////////////////
            // Костыль для PawnShop_alg/DB/LOMBARD2.FDB
            //////////////////////
            if (pkList.get(0).getName().compareToIgnoreCase(pkList.get(0).getName()) == 0) {
                //////////////////////
            } else {
                throw new XError("Not one PK field in table: " + table.getName());
            }
        }
        //
        String pkFieldName = table.getPrimaryKey().get(0).getName();
        return pkFieldName;
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
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = JdxDbUtils.getPkFieldName(table);
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
            if (fieldName.equalsIgnoreCase(pkFieldName)) {
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
        sb.append(pkFieldName);
        //
        cnt = 0;
        sb.append(") values (");
        for (String fieldName : fieldsList) {
            if (containsInList(ilist, fieldName)) {
                continue;
            }
            if (fieldName.equalsIgnoreCase(pkFieldName)) {
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
        sb.append(pkFieldName);
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
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = JdxDbUtils.getPkFieldName(table);
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
            if (fieldName.equalsIgnoreCase(pkFieldName)) {
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

        sb.append(" where ").append(pkFieldName).append("=").append(":").append(pkFieldName);
        return sb.toString();
    }

    /**
     * Генерация sql для delete
     *
     * @param tableName таблица в базе
     * @return текст sql
     */
    public String generateSqlDelete(String tableName) {
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = JdxDbUtils.getPkFieldName(table);
        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");
        sb.append(table.getName());
        sb.append(" where ").append(pkFieldName).append("=").append(":").append(pkFieldName);
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
        else if (value instanceof IJdxField)
            return ((IJdxField) value).getName();
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
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = JdxDbUtils.getPkFieldName(table);
        String sql = generateSqlDelete(tableName);
        Map p = new HashMapNoCase();
        p.put(pkFieldName, id);
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
        IJdxTable table = struct.getTable(tableName);
        String pkFieldName = JdxDbUtils.getPkFieldName(table);
        Map p = new HashMapNoCase();
        p.putAll(params);
        Long id = UtJdxData.longValueOf(p.get(pkFieldName));
        if (id == null) {
            String generatorName = pkRules.getGeneratorName(tableName);
            id = dbGenerators.genNextValue(generatorName);
            p.put(pkFieldName, id);
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

    public Long insertOrUpdate(String tableName, Map<String, Object> recParams, String publicationFields) throws Exception {
        Long id = null;

        try {
            id = insertRec(tableName, recParams, publicationFields, null);
        } catch (Exception e) {
            if (dbErrors.errorIs_PrimaryKeyError(e)) {
                updateRec(tableName, recParams, publicationFields, null);
            } else {
                throw e;
            }
        }

        return id;
    }

    public DataRecord loadSqlRec(String sql, Map params) throws Exception {
        DataStore res = db.loadSql(sql, params);
        checkOneLoadSqlRec(res);
        return res.getCurRec();
    }

    protected void checkOneLoadSqlRec(DataStore st) {
        if (st.size() == 0) {
            throw new XError("No result in sqlrec");
        }
        if (st.size() > 10) {
            throw new XError("Many result in sqlrec");
        }
    }

    public static void execScript(String sqls, Db db) throws Exception {
        String[] sqlArr = sqls.split(";");
        for (String sql : sqlArr) {
            if (sql.trim().length() != 0) {
                db.execSql(sql);
            }
        }
    }

}
