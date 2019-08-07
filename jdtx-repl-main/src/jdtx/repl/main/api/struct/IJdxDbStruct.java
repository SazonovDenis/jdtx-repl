package jdtx.repl.main.api.struct;

import java.util.*;

public interface IJdxDbStruct {

    /**
     * @return Список таблиц
     */
    List<IJdxTable> getTables();

    /**
     * Возвращает таблицу по имени
     *
     * @param tableName имя таблицы
     * @return таблица или null, если такой таблицы нет
     */
    IJdxTable getTable(String tableName);

}
