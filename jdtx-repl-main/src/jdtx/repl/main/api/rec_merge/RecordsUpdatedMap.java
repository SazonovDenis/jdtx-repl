package jdtx.repl.main.api.rec_merge;

import java.util.*;

public class RecordsUpdatedMap extends HashMap<String, RecordsUpdated> {

    /**
     * Возвращает RecordsUpdated для таблицы tableName и поля fieldName.
     * Добавляет новый, если такого нет.
     */
    public RecordsUpdated addForTable(String tableName, String fieldName) {
        String key = tableName + "_" + fieldName;
        //
        RecordsUpdated item = get(key);
        //
        if (item == null) {
            item = new RecordsUpdated();
            item.refTableName = tableName;
            item.refFieldName = fieldName;
            put(key, item);
        }
        //
        return item;
    }


}
