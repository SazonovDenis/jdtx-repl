package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public class JdxUtils {

    public static final String prefix = "Z_";

    public static final String audit_table_prefix = prefix;

    public static final String sys_table_prefix = prefix + "Z_";

    public static final String audit_gen_prefix = prefix + "G_";

    public static final String sys_gen_prefix = sys_table_prefix + "G_";

    public static final String trig_pref = prefix + "T_";

    /**
     * Сортирует lst с учетом foreign key.
     * Результат можно применять для определения порядка таблиц при ins.
     *
     * @param lst исходный список
     * @return отсортированный список
     */
    public static ArrayList<IJdxTableStruct> sortTables(ArrayList<IJdxTableStruct> lst) {
        // отсортированный список таблиц
        ArrayList<IJdxTableStruct> sortLst = new ArrayList<IJdxTableStruct>();

        // список таблиц, которые уже вошли в sortLst
        ArrayList<IJdxTableStruct> usedLst = new ArrayList<IJdxTableStruct>();

        // список таблиц, которые еще не вошли в sortLst (пока все таблицы)
        ArrayList<IJdxTableStruct> restLst = new ArrayList<IJdxTableStruct>();
        restLst.addAll(lst);

        int i = 0;
        // в первую очередь в sortLst помещаем таблицы, не ссылающиеся на другие таблицы
        while (i < restLst.size()) {
            // если нет внешних ключей
            if (restLst.get(i).getForeignKeys().size() == 0) {
                sortLst.add(restLst.get(i));
                usedLst.add(restLst.get(i));
                restLst.remove(i);
            } else {
                i++;
            }
        }

        // для всех добавленных таблиц ищем зависимые таблицы и добавляем их в sortLst до тех пор, пока в sortLst не будут все таблицы
        while (restLst.size() != 0) {
            // список таблиц, добавленных на данной итерации
            ArrayList<IJdxTableStruct> curLst = new ArrayList<IJdxTableStruct>();

            // ищем таблицы, ссылающиеся на уже имеющиеся в usedtLst таблицы
            i = 0;
            while (i < restLst.size()) {
                IJdxTableStruct table = restLst.get(i);

                // перебираем все внешние ключи таблицы table и пытаемся найти, ссылается-ли она на таблицы из уже отсортированных (usedLst)
                boolean willAdd = true;
                for (IJdxForeignKey fk : table.getForeignKeys()) {
                    // если таблица ссылается на не имеющиеся в usedLst таблицы (и эта ссылка не на саму себя), она не будет добавлена
                    if (!fk.getTable().getName().equalsIgnoreCase(table.getName()) && usedLst.indexOf(fk.getTable()) == -1) {
                        willAdd = false;
                        break;
                    }
                }

                //
                if (willAdd) {
                    // таблица ссылается только на кого-либо из уже отсортированных (usedLst)
                    sortLst.add(table);
                    restLst.remove(i);
                    curLst.add(table);
                } else {
                    i++;
                }
            }

            // к списку использованных таблиц прибавляем список таблиц, добавленных на данной итерации
            usedLst.addAll(curLst);
        }

        // отсортированный список таблиц
        return sortLst;
    }


}
