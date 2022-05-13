package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

import java.util.*;


// todo: сделать и  проверить сравнение 2LO2
public class UtDbComparer {

    private static Log log = LogFactory.getLog("jdtx.UtDbComparer");

    public static boolean dbStructIsEqual(IJdxDbStruct struct1, IJdxDbStruct struct2) throws Exception {
        IJdxDbStruct structCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNewIn1 = new JdxDbStruct();
        IJdxDbStruct structDiffNewIn2 = new JdxDbStruct();
        return UtDbComparer.getStructDiff(struct1, struct2, structCommon, structDiffNewIn1, structDiffNewIn2);
    }

    public static boolean dbStructIsEqualTables(IJdxDbStruct struct1, IJdxDbStruct struct2) throws Exception {
        IJdxDbStruct structEqual = new JdxDbStruct();
        IJdxDbStruct structDiffNewIn1 = new JdxDbStruct();
        IJdxDbStruct structDiffNewIn2 = new JdxDbStruct();
        return UtDbComparer.getStructDiffTables(struct1, struct2, structEqual, structDiffNewIn1, structDiffNewIn2);
    }

    /**
     * Вычисляет разницу между двумя структурами БД (таблицы и их поля)
     *
     * @param struct1          первая структура для сравнения
     * @param struct2          вторая структура для сравнения
     * @param structCommon     возвращает общие объекты в обеих структурах
     * @param structDiffNewIn1 возвращает объекты (таблицы и их поля) в первой структуре, которых нет во второй
     * @param structDiffNewIn2 возвращает объекты во второй структуре, которых нет впервой
     * @return =true, если структуры БД одинаковые, иначе в structDiff*** возвращается разница
     */
    public static boolean getStructDiff(IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct structCommon, IJdxDbStruct structDiffNewIn1, IJdxDbStruct structDiffNewIn2) throws Exception {
        // сравниваем таблицы из первой базы с таблицами со второй базы
        for (IJdxTable t1 : struct1.getTables()) {
            // таблица первой базы - находим такую же таблицу во второй базе
            IJdxTable t2 = struct2.getTable(t1.getName());
            // если она существует
            if (t2 != null) {
                JdxTable t = null;
                // поля из первой таблицы ищем во второй таблице
                for (IJdxField f1 : t1.getFields()) {
                    IJdxField f2 = t2.getField(f1.getName());
                    // если поле не существует, создаем новый экземпляр таблицы и добавляем его туда
                    if (f2 == null) {
                        if (t == null) {
                            t = new JdxTable();
                            t.setName(t1.getName());
                        }
                        t.getFields().add(f1);
                    } else {
                        // сравниваем характеристики поля
                        if (!compareField(f1, f2)) {
                            // если поля разные по свойствам, создаем новый экземпляр таблицы и добавляем его туда
                            if (t == null) {
                                t = new JdxTable();
                                t.setName(t1.getName());
                            }
                            t.getFields().add(f1);
                        }
                    }
                }
                if (t != null) {
                    structDiffNewIn1.getTables().add(t);
                } else {
                    structCommon.getTables().add(t1);
                }
            } else {
                structDiffNewIn1.getTables().add(t1);
            }
        }

        // сравниваем таблицы из второй базы с таблицами из первой базы
        for (IJdxTable t2 : struct2.getTables()) {
            // таблица второй базы - находим такую же таблицу в первой базе
            IJdxTable t1 = struct1.getTable(t2.getName());
            // если она существует
            if (t1 != null) {
                JdxTable t = null;
                // поля из второй таблицы ищем в первой таблице
                for (IJdxField f2 : t2.getFields()) {
                    IJdxField f1 = t1.getField(f2.getName());
                    // если поле не существует, создаем новый экземпляр таблицы и добавляем его туда
                    if (f1 == null) {
                        if (t == null) {
                            t = new JdxTable();
                            t.setName(t2.getName());
                        }
                        t.getFields().add(f2);
                    } else {
                        // сравниваем характеристики поля
                        if (!compareField(f1, f2)) {
                            // если поля разные по свойствам, создаем новый экземпляр таблицы и добавляем его туда
                            if (t == null) {
                                t = new JdxTable();
                                t.setName(t1.getName());
                            }
                            t.getFields().add(f1);
                        }
                    }
                }
                if (t != null) {
                    structDiffNewIn2.getTables().add(t);
                }

            } else {
                structDiffNewIn2.getTables().add(t2);
            }
        }

        // если структуры обеих баз не отличаются возвращаем true
        if (structDiffNewIn1.getTables().size() == 0 && structDiffNewIn2.getTables().size() == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Вычисляет разницу между двумя структурами БД (только таблицы)
     *
     * @param struct1          первая структура для сравнения
     * @param struct2          вторая структура для сравнения
     * @param structEqual      возвращает общие объекты в обеих структурах
     * @param structDiffNewIn1 возвращает таблицы в первой структуре, которых нет во второй
     * @param structDiffNewIn2 возвращает таблицы во второй структуре, которых нет впервой
     * @return =true, если структуры БД одинаковые, иначе в structDiff*** возвращается разница
     */
    public static boolean getStructDiffTables(IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct structEqual, IJdxDbStruct structDiffNewIn1, IJdxDbStruct structDiffNewIn2) throws Exception {
        // сравниваем таблицы из первой базы с таблицами со второй базы
        for (IJdxTable t1 : struct1.getTables()) {
            // таблица первой базы - находим такую же таблицу во второй базе
            IJdxTable t2 = struct2.getTable(t1.getName());
            //
            if (t2 == null) {
                structDiffNewIn1.getTables().add(t1);
            } else {
                structEqual.getTables().add(t1);
            }
        }

        // сравниваем таблицы из второй базы с таблицами из первой базы
        for (IJdxTable t2 : struct2.getTables()) {
            // таблица второй базы - находим такую же таблицу в первой базе
            IJdxTable t1 = struct1.getTable(t2.getName());
            //
            if (t1 == null) {
                structDiffNewIn2.getTables().add(t2);
            }
        }

        // если структуры обеих баз не отличаются возвращаем true
        if (structDiffNewIn1.getTables().size() == 0 && structDiffNewIn2.getTables().size() == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Возвращает контрольную сумму структуры БД (таблицы и структура ссылок)
     *
     * @param struct структура БД
     * @return контрольная сумма структуры БД
     */
    public static String getDbStructCrc(IJdxDbStruct struct) {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        return UtString.md5Str(struct_rw.toString(struct, false));
    }

    /**
     * Возвращает контрольную сумму структуры БД (только таблицы, без структуры ссылок)
     *
     * @param struct структура БД
     * @return контрольная сумма структуры таблиц БД
     */
    public static String getDbStructCrcTables(IJdxDbStruct struct) {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        return UtString.md5Str(struct_rw.toString(struct, false));
    }

    private static boolean compareField(IJdxField f1, IJdxField f2) {
        if (f1.getDbDatatype().compareToIgnoreCase(f2.getDbDatatype()) != 0) {
            return false;
        }
        if (f1.getSize() != f2.getSize()) {
            return false;
        }
        return true;
    }


    /**
     * Читает контрольную сумму записей в БД
     *
     * @param db     база для анализа
     * @param struct структура базы. Чтение идет только для таблиц и полей, указанных в struct
     * @return Map, где ключ - имя таблицы, значение - CRC каждой записи
     */
    public static Map<String, Map<String, String>> getDbDataCrc(Db db, IJdxDbStruct struct, IJdxDataSerializer dataSerializer) throws Exception {
        Map<String, Map<String, String>> res = new HashMap<>();

        //
        for (IJdxTable table : struct.getTables()) {
            String tableName = table.getName();

            //
            if (table.getPrimaryKey().size() == 0) {
                //log.info("table: " + tableName + " has no PK");
                continue;
            }

            //
            dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));

            //
            IJdxField pkField = table.getPrimaryKey().get(0);
            String pkFieldName = pkField.getName();
            String tableFieldNamesStr = UtJdx.fieldsToString(table.getFields());


            //
            Map<String, String> resTable = new HashMap<>();
            res.put(tableName, resTable);

            //
            DbQuery query = db.openSql("select " + tableFieldNamesStr + " from " + tableName + " order by " + pkFieldName);
            try {
                long count = 0;
                while (!query.eof()) {
                    Map<String, Object> values = query.getValues();
                    Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
                    String recCrc = UtString.md5Str(valuesStr.toString());

                    //
                    String pkFieldValue = dataSerializer.prepareValueStr(query.getValueString(pkFieldName), pkField);
                    resTable.put(pkFieldValue, recCrc.substring(0, 16));

                    //
                    query.next();

                    //
                    count++;
                    if (count % 1000 == 0) {
                        log.info("readData: " + tableName + ", " + count);
                    }
                }

                //
                //log.info("readData done: " + tableName + ", " + count);
            } finally {
                query.close();
            }

        }

        return res;
    }

    public static void compareDbDataCrc(Map<String, Map<String, String>> dbCrc1, Map<String, Map<String, String>> dbCrc2, Map<String, Set<String>> diffCrc, Map<String, Set<String>> diffNewIn1, Map<String, Set<String>> diffNewIn2) {
        diffCrc.clear();
        diffNewIn1.clear();
        diffNewIn2.clear();

        for (String tableName : dbCrc1.keySet()) {
            Map<String, String> resTable1 = dbCrc1.get(tableName);
            Map<String, String> resTable2;
            if (!dbCrc2.containsKey(tableName)) {
                log.info("Table: " + tableName + " not found in db2");
                resTable2 = new HashMap<>();
            } else {
                resTable2 = dbCrc2.get(tableName);
            }

            Set<String> diffCrcTable = new HashSet<>();
            Set<String> diffNewIn1Table = new HashSet<>();
            Set<String> diffNewIn2Table = new HashSet<>();

            // Сравниваем crc записей, встречающихся в ОБОИХ наборах
            for (String pkFieldValue : resTable1.keySet()) {
                if (resTable2.containsKey(pkFieldValue)) {
                    String crc1 = resTable1.get(pkFieldValue);
                    String crc2 = resTable2.get(pkFieldValue);
                    if (!crc1.equalsIgnoreCase(crc2)) {
                        //log.info("Table: " + tableName + ", record: [" + pkFieldValue + "] db1.crc " + crc1 + " <> db2.crc: " + crc2);
                        diffCrcTable.add(pkFieldValue);
                    }
                }
            }

            // Формируем записи, встречающиеся ТОЛЬКО в базе 1
            for (String pkFieldValue1 : resTable1.keySet()) {
                if (!resTable2.containsKey(pkFieldValue1)) {
                    //log.info("Table: " + tableName + ", record: [" + pkFieldValue1 + "] not found in db2");
                    diffNewIn1Table.add(pkFieldValue1);
                }
            }

            // Формируем записи, встречающиеся ТОЛЬКО в базе 2
            for (String pkFieldValue2 : resTable2.keySet()) {
                if (!resTable1.containsKey(pkFieldValue2)) {
                    //log.info("Table: " + tableName + ", record: [" + pkFieldValue2 + "] not found in db1");
                    diffNewIn2Table.add(pkFieldValue2);
                }
            }

            // Результат сравнения таблицы - в общий список
            diffCrc.put(tableName, diffCrcTable);
            diffNewIn1.put(tableName, diffNewIn1Table);
            diffNewIn2.put(tableName, diffNewIn2Table);
        }
    }

}
