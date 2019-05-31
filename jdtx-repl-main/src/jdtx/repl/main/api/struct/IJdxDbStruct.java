package jdtx.repl.main.api.struct;

import java.util.*;

public interface IJdxDbStruct {

    /**
     * @return Список таблиц
     */
    ArrayList<IJdxTableStruct> getTables();

    /**
     * Возвращает таблицу по имени
     *
     * @param tableName имя таблицы
     * @return таблица или null, если такой таблицы нет
     */
    IJdxTableStruct getTable(String tableName);

}
