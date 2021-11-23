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
     * В начале списка оказываются таблицы, которые не ссылаются на другие таблицы (например, справочники).
     * Сортировка по алфавиту может пригодится для предсказуемого порядка, если таблицы не зависят друг от друга.
     * Результат можно применять для определения порядка таблиц при ins, для избежания проблем со ссылочной целостностью.
     *
     * @param lst исходный список
     * @return отсортированный список
     */
    public static List<IJdxTable> sortTablesByReference(List<IJdxTable> lst) throws Exception {
        // отсортированный список таблиц
        List<IJdxTable> sortLst = new ArrayList<>();

        // список таблиц, которые уже вошли в sortLst
        List<IJdxTable> usedLst = new ArrayList<IJdxTable>();

        // список таблиц, которые еще не вошли в sortLst (пока все таблицы)
        List<IJdxTable> restLst = new ArrayList<IJdxTable>();
        restLst.addAll(lst);

        // В первую итерацию в sortLst помещаем таблицы, не ссылающиеся на другие таблицы
        List<IJdxTable> curLst = new ArrayList<IJdxTable>();
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
        sortLst.addAll(curLst);
        usedLst.addAll(curLst);


        // Для всех добавленных (и отсортированных) таблиц ищем зависимые таблицы и добавляем их в sortLst до тех пор,
        // пока в sortLst не окажутся все таблицы
        while (restLst.size() != 0) {
            // список таблиц, добавленных на данной итерации
            curLst.clear();

            // Ищем таблицы, ссылающиеся на уже имеющиеся в usedLst таблицы
            i = 0;
            while (i < restLst.size()) {
                IJdxTable table = restLst.get(i);

                // перебираем все внешние ключи таблицы table и пытаемся выяснить,
                // ссылается ли table на таблицы из уже отсортированных (usedLst)
                boolean willAdd = true;
                for (IJdxForeignKey fk : table.getForeignKeys()) {
                    // Если ссылка в таблице ссылается не на эту же таблицу,
                    // и целевая таблица была в исходном списке,
                    // и целевая таблица пока отсутствует в usedLst,
                    // то таблицу пока пропускаем
                    if (!fk.getTable().getName().equalsIgnoreCase(table.getName())
                            && usedLst.indexOf(fk.getTable()) == -1
                            && lst.indexOf(fk.getTable()) != -1) {
                        willAdd = false;
                        break;
                    }
                }

                //
                if (willAdd) {
                    // Таблица ссылается только на кого-либо из уже отсортированных (usedLst)
                    restLst.remove(i);
                    curLst.add(table);
                } else {
                    i++;
                }
            }

            // Отсортируем по алфавиту список таблиц, добавленных на данной итерации
            curLst.sort(tableComparator);

            // К списку отсортированнных и использованных таблиц прибавляем список таблиц, добавленных на данной итерации
            sortLst.addAll(curLst);
            usedLst.addAll(curLst);

            //
            if (curLst.size() == 0) {
                throw new Exception("sortTablesByReference: больше невозможно добавить таблиц");
            }
        }

        // Отсортированный список таблиц
        return sortLst;
    }


    /**
     * Возвращает список таблиц, от которых зависит tableMain (по foreign key).
     */
    public static List<IJdxTable> getTablesDependsOn(IJdxTable tableMain, boolean all) throws Exception {
        // Список таблиц
        List<IJdxTable> res = new ArrayList<>();

        //
        getTablesDependsOnInternal(tableMain, all, res);

        //
        return res;
    }

    /**
     * Возвращает список таблиц, которые зависят от tableMain (по foreign key).
     */
    public static List<IJdxTable> getDependTables(IJdxDbStruct struct, IJdxTable tableMain, boolean all) throws Exception {
        // Список таблиц
        List<IJdxTable> res = new ArrayList<>();

        //
        getDependTablesInternal(struct, tableMain, all, res);

        //
        return res;
    }

    private static void getDependTablesInternal(IJdxDbStruct struct, IJdxTable tableMain, boolean all, List<IJdxTable> res) {
        //
        for (IJdxTable tableRef : struct.getTables()) {

            for (IJdxForeignKey fieldFk : tableRef.getForeignKeys()) {

                if (tableMain == fieldFk.getTable()) {

                    //
                    if (res.contains(tableRef)) {
                        continue;
                    }

                    //
                    res.add(tableRef);

                    //
                    if (all) {
                        getDependTablesInternal(struct, tableRef, all, res);
                    }
                }
            }
        }
    }

    private static void getTablesDependsOnInternal(IJdxTable tableMain, boolean all, List<IJdxTable> res) {
        for (IJdxForeignKey fieldFk : tableMain.getForeignKeys()) {
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
        if (replica.getInfo().getReplicaType() == JdxReplicaType.MUTE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.MUTE_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.UNMUTE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.UNMUTE_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SET_DB_STRUCT ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SET_DB_STRUCT_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.UPDATE_APP ||
                replica.getInfo().getReplicaType() == JdxReplicaType.UPDATE_APP_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SET_CFG ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SET_CFG_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SET_STATE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.REPAIR_GENERATORS ||
                replica.getInfo().getReplicaType() == JdxReplicaType.REPAIR_GENERATORS_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SEND_SNAPSHOT ||
                replica.getInfo().getReplicaType() == JdxReplicaType.SEND_SNAPSHOT_DONE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.MERGE ||
                replica.getInfo().getReplicaType() == JdxReplicaType.IDE_MERGE

        ) {
            // Для системных команд мы не делаем других проверок
            return;
        }

        // Проверки: указан возраст данных в реплике IDE
        if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE && replica.getInfo().getAge() <= -1) {
            throw new XError("invalid replica.age");
        }

        // Проверки: правильность кода рабочей станции
        if (replica.getInfo().getWsId() <= 0) {
            throw new XError("invalid replica.wsId");
        }

        // Проверки: обязательность файла
        File replicaFile = replica.getData();
        if (replicaFile == null && replica.getInfo().getReplicaType() != JdxReplicaType.SNAPSHOT) {
            // Разрещаем SNAPSHOT быть без файла, т.к. свои собственные snapshot-реплики, поступающие в queIn,
            // можно не скачивать (и в дальнейшем не применять)
            throw new XError("invalid replica.file is null");
        }

    }

    /**
     * Проверяет целостность файла в реплике, сравнивая CRC файла, с CRC в info
     */
    public static void checkReplicaCrc(IReplica replica, String crc) throws Exception {
        if (!equalReplicaCrc(replica, crc)) {
            // Неправильно скачанный файл - удаляем, чтобы потом начать снова
            replica.getData().delete();
            // Ошибка
            throw new XError("receive.replica.md5 <> info.crc, file: " + replica.getData());
        }
    }

    /**
     * @return true, если CRC файла в реплике совпадает с crc
     */
    public static boolean equalReplicaCrc(IReplica replica, String crc) throws Exception {
        String md5file = UtJdx.getMd5File(replica.getData());
        return md5file.compareToIgnoreCase(crc) == 0;
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
            tableList.add(table);
        }
        //
        return tableList;
    }

}
