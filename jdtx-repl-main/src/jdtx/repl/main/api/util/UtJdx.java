package jdtx.repl.main.api.util;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;

import java.io.*;
import java.security.*;
import java.util.*;

/**
 * Утилиты (разные, не обязательно связанные именно с репликацией)
 */
public class UtJdx {


    public static final String PREFIX = "Z_";

    public static final String AUDIT_TABLE_PREFIX = PREFIX;

    public static final String SYS_TABLE_PREFIX = PREFIX + "Z_";

    public static final String AUDIT_GEN_PREFIX = PREFIX + "G_";

    public static final String SYS_GEN_PREFIX = SYS_TABLE_PREFIX + "G_";

    public static final String TRIGER_PREFIX = PREFIX + "T_";

    public static final String XML_FIELD_OPR_TYPE = PREFIX + "OPR";

    public static final String SQL_FIELD_OPR_TYPE = PREFIX + "opr_type";


    /**
     * Сортирует список таблиц с учетом foreign key и по алфавиту.
     * <p>
     * В начале списка оказываются таблицы, которые не ссылаются на другие таблицы (например, справочники).
     * Результат можно применять для определения порядка таблиц при INS|DEL, для избежания проблем со ссылочной целостностью,
     * удалять данные можно с конца списка, а добавлять - с начала.
     * <p>
     * Сортировка по алфавиту может пригодится для предсказуемого порядка, если таблицы не зависят друг от друга.
     *
     * @param lst исходный список
     * @return отсортированный список
     */
    public static List<IJdxTable> sortTablesByReference(List<IJdxTable> lst) throws Exception {
        // отсортированный список таблиц
        List<IJdxTable> sortedLst = new ArrayList<>();

        // список таблиц, которые уже вошли в sortedLst
        List<IJdxTable> usedLst = new ArrayList<>();

        // список таблиц, которые еще не вошли в sortedLst (пока все таблицы)
        List<IJdxTable> restLst = new ArrayList<>();
        restLst.addAll(lst);

        // В первую итерацию в sortedLst помещаем таблицы, не ссылающиеся на другие таблицы
        List<IJdxTable> curLst = new ArrayList<>();
        int i = 0;
        while (i < restLst.size()) {
            IJdxTable table = restLst.get(i);
            // если нет внешних ключей
            if (table.getForeignKeys().size() == 0) {
                restLst.remove(i);
                curLst.add(table);
            } else {
                i++;
            }
        }

        //
        JdxTableComparator tableComparator = new JdxTableComparator();

        // Отсортируем по алфавиту первую итерацию
        curLst.sort(tableComparator);

        // К списку отсортированнных и использованных таблиц прибавляем список таблиц первой итерации
        sortedLst.addAll(curLst);
        usedLst.addAll(curLst);


        // Для всех добавленных (и отсортированных) таблиц ищем зависимые таблицы и добавляем их в sortedLst до тех пор,
        // пока в sortedLst не окажутся все таблицы
        while (restLst.size() != 0) {
            // Список таблиц, добавленных на данной итерации
            curLst.clear();

            // Ищем таблицы, ссылающиеся на уже имеющиеся в usedLst таблицы
            i = 0;
            while (i < restLst.size()) {
                IJdxTable table = restLst.get(i);

                // Перебираем все внешние ключи таблицы table и пытаемся выяснить,
                // ссылается ли table на таблицы из уже отсортированных (usedLst)
                boolean willAdd = true;
                for (IJdxForeignKey fk : table.getForeignKeys()) {
                    // Если ссылка в таблице ссылается на саму себя, то эту ссылку пропустим
                    if (fk.getTable().getName().equalsIgnoreCase(table.getName())) {
                        continue;
                    }

                    // Если целевая таблица была в исходном списке,
                    // и целевая таблица пока отсутствует в usedLst,
                    // то таблицу пока пропускаем
                    if (lst.contains(fk.getTable()) && !usedLst.contains(fk.getTable())) {
                        willAdd = false;
                        break;
                    }
                }

                // Таблица ссылается только на кого-либо из уже отсортированных (usedLst)
                if (willAdd) {
                    restLst.remove(i);
                    curLst.add(table);
                } else {
                    i++;
                }
            }

            // Отсортируем по алфавиту список таблиц, добавленных на данной итерации
            curLst.sort(tableComparator);

            // К списку отсортированнных и использованных таблиц прибавляем список таблиц, добавленных на данной итерации
            sortedLst.addAll(curLst);
            usedLst.addAll(curLst);

            //
            if (curLst.size() == 0) {
                throw new Exception("sortTablesByReference: больше невозможно добавить таблиц");
            }
        }

        // Отсортированный список таблиц
        return sortedLst;
    }


    /**
     * Возвращает список таблиц, на которые (по foreign key) ссылается table (от которых зависит table).
     * Таблицы в результате отсортированы по зависимостям (по foreign key).
     *
     * @param table главная таблица (напр. Abn)
     * @param all   найти все зависимости рекурсивно
     * @return спискок таблиц, в которых есть ссылка на главную (используемые справочники, напр. Ulz)
     */
    public static List<IJdxTable> getTablesDependsOn(IJdxTable table, boolean all) throws Exception {
        // Список таблиц
        List<IJdxTable> res = new ArrayList<>();

        //
        getTablesDependsOnInternal(table, all, res);

        //
        res = UtJdx.sortTablesByReference(res);

        //
        return res;
    }

    /**
     * Возвращает список таблиц, которые (по foreign key) ссылаются на table (которые зависят от table).
     * Таблицы в результате отсортированы по зависимостям (по foreign key).
     *
     * @param table таблица - справочник (напр. Ulz)
     * @param all   найти все зависимости рекурсивно
     * @return спискок таблиц, в которых есть ссылка на главную (пользователи справочника, напр. Abn)
     */
    public static List<IJdxTable> getDependTables(List<IJdxTable> structTables, IJdxTable table, boolean all) throws Exception {
        // Список таблиц
        List<IJdxTable> res = new ArrayList<>();

        //
        getDependTablesInternal(structTables, table, all, res);

        //
        res = UtJdx.sortTablesByReference(res);

        //
        return res;
    }

    /**
     * Возвращает все ссылки на таблицу (справочник).
     * Учитывает, что ссылок ИЗ одной таблицы на другую таблицу бывает более одной, например Abn.Ulz -> Ulz и Abn.UlzReg -> Ulz.
     *
     * @param table таблица - справочник (напр. Ulz)
     * @param all   найти все зависимости рекурсивно
     * @return Map, где для каждой зависимой таблицы (key) имеется список ссылок (value), которые ссылаются на таблицу tableName (пользователи справочника, напр. Abn)
     */
    public static Map<String, Collection<IJdxForeignKey>> getRefsToTable(List<IJdxTable> structTables, IJdxTable table, boolean all) {
        Map<String, Collection<IJdxForeignKey>> res = new HashMap<>();

        //
        getRefsToTableInternal(structTables, table, all, res);

        //
        return res;
    }

    /**
     * @return Возвращает список значений из tables, отсртированных по порядку как таблицы в structTables
     */
    public static List<String> getSortedKeys(List<IJdxTable> structTables, Collection<String> tables) {

        // Отсортируем как в structTables
        List<String> tablesSorted = new ArrayList<>();
        for (IJdxTable tableStruct : structTables) {
            String tableName = tableStruct.getName();
            if (tables.contains(tableName)) {
                tablesSorted.add(tableName);
            }
        }

        return tablesSorted;
    }

    private static void getRefsToTableInternal(List<IJdxTable> structTables, IJdxTable table, boolean all, Map<String, Collection<IJdxForeignKey>> res) {
        for (IJdxTable refTable : structTables) {
            List<IJdxForeignKey> refTableFkList = new ArrayList<>();
            //
            for (IJdxForeignKey refTableFk : refTable.getForeignKeys()) {
                if (refTableFk.getTable().getName().equals(table.getName())) {
                    refTableFkList.add(refTableFk);
                }
            }
            //
            if (refTableFkList.size() != 0) {
                res.put(refTable.getName(), refTableFkList);

                //
                if (all) {
                    getRefsToTableInternal(structTables, refTable, all, res);
                }
            }

        }
    }

    private static void getDependTablesInternal(List<IJdxTable> structTables, IJdxTable table, boolean all, List<IJdxTable> res) {
        //
        for (IJdxTable tableRef : structTables) {

            for (IJdxForeignKey fieldFk : tableRef.getForeignKeys()) {

                if (table == fieldFk.getTable()) {

                    //
                    if (res.contains(tableRef)) {
                        continue;
                    }

                    //
                    res.add(tableRef);

                    //
                    if (all) {
                        getDependTablesInternal(structTables, tableRef, all, res);
                    }
                }
            }
        }
    }

    private static void getTablesDependsOnInternal(IJdxTable table, boolean all, List<IJdxTable> res) {
        for (IJdxForeignKey fieldFk : table.getForeignKeys()) {
            IJdxTable tableRef = fieldFk.getTable();

            //
            if (res.contains(tableRef)) {
                continue;
            }

            //
            res.add(tableRef);

            //
            if (all) {
                getTablesDependsOnInternal(tableRef, all, res);
            }
        }
    }

    public static String getMd5File(File file) throws Exception {
        int buffSize = 1024 * 10;
        byte[] buffer = new byte[buffSize];

        try (
                FileInputStream inputStream = new FileInputStream(file)
        ) {
            MessageDigest md = MessageDigest.getInstance("MD5"); //NON-NLS

            //
            md.reset();
            //
            while (inputStream.available() > 0) {
                int n = inputStream.read(buffer);
                md.update(buffer, 0, n);
            }
            //
            byte[] a = md.digest();

            //
            return UtString.toHexString(a);
        }
    }

    public static String getMd5Buffer(byte[] buffer) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5"); //NON-NLS

        //
        md.reset();
        //
        md.update(buffer, 0, buffer.length);
        //
        byte[] a = md.digest();

        //
        return UtString.toHexString(a);
    }

    /**
     * Проверки реплики, правильность полей.
     * Защита от дурака, в отлаженнном коде - не нужна
     */
    public static void validateReplicaFields(IReplica replica) {
        // Проверки: правильность типа реплики
        if (replica.getInfo().getReplicaType() <= 0) {
            throw new XError("invalid replica.replicaType");
        }

        // Реплика - системная команда?
        if (JdxReplicaType.isSysReplica(replica.getInfo().getReplicaType())         ) {
            // Для системных команд мы не делаем других проверок
            return;
        }

        // Проверки: указан код рабочей станции
        if (replica.getInfo().getWsId() <= 0) {
            throw new XError("invalid replica.wsId: " + replica.getInfo().getWsId());
        }

        // Проверки: указан номер реплики, если он там нужен
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE && replica.getInfo().getNo() <= 0) {
            throw new XError("replica info.no is not set or invalid, no: " + replica.getInfo().getNo());
        }

        // Проверки: указан возраст данных age в реплике типа IDE
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE && replica.getInfo().getAge() <= 0) {
            throw new XError("invalid replica.age: " + replica.getInfo().getAge());
        }

        // Проверки: обязательность файла
        File replicaFile = replica.getData();
        if (replicaFile == null && replica.getInfo().getReplicaType() != JdxReplicaType.SNAPSHOT) {
            // Разрещаем только SNAPSHOT быть без файла, т.к. свои собственные snapshot-реплики, поступающие в queIn,
            // можно не скачивать (и в дальнейшем не применять), вот их могли и не скачачать и файла нет
            throw new XError("invalid replica.file: is null");
        }

    }

    /**
     * Проверяет целостность файла в реплике, сравнивая crc самого файла с указанным crc
     * Выкидывет exception если crc файла не совпадают
     */
    public static void checkReplicaCrc(IReplica replica, String crc) throws Exception {
        String crcFile = UtJdx.getMd5File(replica.getData());
        if (!equalCrc(crcFile, crc)) {
            // Неправильно скачанный файл - удаляем, чтобы потом начать снова
            replica.getData().delete();
            // Ошибка
            throw new XError("receive.replica.crc <> info.crc, file.crc: " + crcFile + ", crc: " + crc + ", file: " + replica.getData());
        }
    }

    /**
     * Проверяет целостность файла в реплике, сравнивая crc самого файла с указанным crc
     *
     * @return true, если crc совпадают.
     */
    public static boolean equalReplicaCrc(IReplica replica, String crc) throws Exception {
        String crcFile = UtJdx.getMd5File(replica.getData());
        return equalCrc(crcFile, crc);
    }

    /**
     * @return true, если crc пустой или crc == null или crcFile совпадает c crc.
     */
    public static boolean equalCrc(String crcFile, String crc) {
        // crcFile не может быть null, т.к. он берется из файла, но crc разрешаем быть пустым или null -
        // так бывает при различных переходных процессах (смене версии, приготовлении реплики вручную и т.п.)
        return crc == null || crc.length() == 0 || crcFile.compareToIgnoreCase(crc) == 0;
    }

    /**
     * Набор полей в строку с разделителями.
     * Работает аналогично  String.join()
     */
    public static String fieldsToString(Collection<IJdxField> fields) {
        return fieldsToString(fields, null);
    }

    /**
     * Набор полей в строку с разделителями, но перед каждым полем добавляется префикс.
     * Работает аналогично  String.join()
     */
    public static String fieldsToString(Collection<IJdxField> fields, String fieldPrefix) {
        StringBuilder sb = new StringBuilder();

        //
        for (IJdxField field : fields) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            if (fieldPrefix != null) {
                sb.append(fieldPrefix);
            }
            sb.append(field.getName());
        }

        //
        return sb.toString();
    }


    /**
     * Разложим строку tableNames в список IJdxTable
     */
    public static List<IJdxTable> stringToTables(String tableNames, IJdxDbStruct struct) {
        List<IJdxTable> tableList = new ArrayList<>();
        //
        String[] tableNamesArr = tableNames.split(",");
        for (String tableName : tableNamesArr) {
            IJdxTable table = struct.getTable(tableName);
            if (table == null) {
                throw new XError("Table not found in struct: " + tableName);
            }
            tableList.add(table);
        }
        //
        return tableList;
    }

}
