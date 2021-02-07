package jdtx.repl.main.api;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;

import java.io.*;
import java.security.*;
import java.util.*;

public class JdxUtils {


    public static final String prefix = "Z_";

    public static final String audit_table_prefix = prefix;

    public static final String sys_table_prefix = prefix + "Z_";

    public static final String audit_gen_prefix = prefix + "G_";

    public static final String sys_gen_prefix = sys_table_prefix + "G_";

    public static final String trig_pref = prefix + "T_";


    /**
     * Сортирует список таблиц с учетом foreign key и по алфавиту.
     * Результат можно применять для определения порядка таблиц при ins.
     *
     * @param lst исходный список
     * @return отсортированный список
     */
    public static List<IJdxTable> sortTablesByReference(List<IJdxTable> lst) throws Exception {
        // отсортированный список таблиц
        List<IJdxTable> sortLst = new ArrayList<IJdxTable>();

        // список таблиц, которые уже вошли в sortLst
        List<IJdxTable> usedLst = new ArrayList<IJdxTable>();

        // список таблиц, которые еще не вошли в sortLst (пока все таблицы)
        List<IJdxTable> restLst = new ArrayList<IJdxTable>();
        restLst.addAll(lst);

        //
        JdxTableComparator tableComparator = new JdxTableComparator();

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
                // ссылается-ли table на таблицы из уже отсортированных (usedLst)
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
                replica.getInfo().getReplicaType() == JdxReplicaType.SET_QUE_IN_NO
        ) {
            // Для системных команд мы не делаем других проверок
            return;
        }

        // Проверки: правильность возраста реплики
        if (replica.getInfo().getAge() <= -1) {
            throw new XError("invalid replica.age");
        }

        // Проверки: правильность кода рабочей станции
        if (replica.getInfo().getWsId() <= 0) {
            throw new XError("invalid replica.wsId");
        }

        // Проверки: обязательность файла
        File replicaFile = replica.getFile();
        if (replicaFile == null && replica.getInfo().getReplicaType() != JdxReplicaType.SNAPSHOT) {
            // Разрещаем SNAPSHOT быть без файла, т.к. свои собственные snapshot-реплики, поступающие в queIn,
            // можно не скачивать (и в дальнейшем не применять)
            throw new XError("invalid replica.file is null");
        }

    }

    public static String collectExceptionText(Exception e) {
        String errText = e.toString();
        if (e.getCause() != null) {
            errText = errText + "\n" + e.getCause().toString();
        }
        return errText;
    }

    public static boolean errorIs_PrimaryKeyError(Exception e) {
        String errText = collectExceptionText(e);
        return errText.contains("violation of PRIMARY or UNIQUE KEY constraint");
    }

    public static boolean errorIs_ForeignKeyViolation(Exception e) {
        String errText = collectExceptionText(e);
        if (errText.contains("violation of FOREIGN KEY constraint") && errText.contains("on table")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean errorIs_TableNotExists(Exception e) {
        String errText = collectExceptionText(e);
        if ((errText.contains("table/view") && errText.contains("does not exist")) ||
                errText.contains("Table unknown")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean errorIs_GeneratorNotExists(Exception e) {
        if (collectExceptionText(e).contains("Generator not found")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean errorIs_TriggerNotExists(Exception e) {
        if (collectExceptionText(e).contains("Trigger not found")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean errorIs_TableAlreadyExists(Exception e) {
        String errText = collectExceptionText(e);
        if (errText.contains("Table") && errText.contains("already exists")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean errorIs_GeneratorAlreadyExists(Exception e) {
        String errText = collectExceptionText(e);
        if (errText.contains("DEFINE GENERATOR failed") && errText.contains("attempt to store duplicate value")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean errorIs_TriggerAlreadyExists(Exception e) {
        String errText = collectExceptionText(e);
        if (errText.contains("DEFINE TRIGGER failed") && errText.contains("attempt to store duplicate value")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * По тексту ошибки возвращает таблицу, в которой содержится неправильная ссылка
     *
     * @param e Exception, например: violation of FOREIGN KEY constraint "FK_LIC_ULZ" on table "LIC"
     * @return IJdxTable - таблица, в которой содержится неправильная ссылка, например: Lic
     */
    public static IJdxTable get_ForeignKeyViolation_tableInfo(JdxForeignKeyViolationException e, IJdxDbStruct struct) {
        //
        String errText = e.getMessage();
        String[] sa = errText.split("on table");
        //
        String thisTableName = sa[1];
        thisTableName = thisTableName.replace("\"", "").replace(" ", "");
        IJdxTable thisTable = struct.getTable(thisTableName);
        //
        return thisTable;
    }

    /**
     * По тексту ошибки возвращает поле, которое содержит неправильную ссылку
     *
     * @param e Exception, например: violation of FOREIGN KEY constraint "FK_LIC_ULZ" on table "LIC"
     * @return IJdxForeignKey - ссылочное поле, которое привело к ошибке, например: Lic.Ulz
     */
    public static IJdxForeignKey get_ForeignKeyViolation_refInfo(JdxForeignKeyViolationException e, IJdxDbStruct struct) {
        //
        String errText = e.getMessage();
        String[] sa = errText.split("on table");
        //
        String foreignKeyName = sa[0].split("FOREIGN KEY constraint")[1];
        foreignKeyName = foreignKeyName.replace("\"", "").replace(" ", "");
        //
        String thisTableName = sa[1];
        thisTableName = thisTableName.replace("\"", "").replace(" ", "");
        IJdxTable thisTable = struct.getTable(thisTableName);
        //
        for (IJdxForeignKey foreignKey : thisTable.getForeignKeys()) {
            if (foreignKey.getName().compareToIgnoreCase(foreignKeyName) == 0) {
                return foreignKey;
            }
        }
        //
        return null;
    }

    /**
     * Проверяет целостность файцла в реплике по crc
     */
    public static void checkReplicaCrc(IReplica replica, ReplicaInfo info) throws Exception {
        String md5file = JdxUtils.getMd5File(replica.getFile());
        if (!md5file.equals(info.getCrc())) {
            // Неправильно скачанный файл - удаляем, чтобы потом начать снова
            replica.getFile().delete();
            // Ошибка
            throw new XError("receive.replica.md5 <> info.crc, file: " + replica.getFile());
        }
    }

    private static class JdxTableComparator implements Comparator {
        public int compare(Object o1, Object o2) {
            IJdxTable table1 = (IJdxTable) o1;
            IJdxTable table2 = (IJdxTable) o2;
            return table1.getName().compareToIgnoreCase(table2.getName());
        }
    }

    public static Long longValueOf(Object idValue) {
        Long id;
        if (idValue == null) {
            id = null;
        } else if (idValue instanceof Long) {
            id = (Long) idValue;
        } else if (idValue instanceof Integer) {
            id = Long.valueOf((Integer) idValue);
        } else {
            id = Long.valueOf(idValue.toString());
        }
        return id;
    }

}
