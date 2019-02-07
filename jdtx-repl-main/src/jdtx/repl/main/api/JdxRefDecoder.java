package jdtx.repl.main.api;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;

import java.util.*;

/**
 * Перекодировщик ссылок.
 */
public class JdxRefDecoder {

    long SLOT_SIZE = 1000000;

    long db_code = -1;
    Db db = null;

    protected Map<String, Map> tablesDecodeSlots;
    protected Map<String, Long> tablesSlotMax;

    public JdxRefDecoder(Db db, long db_code) throws Exception {
        this.db = db;
        this.db_code = db_code;

        // Загрузим слоты для db_code
        tablesDecodeSlots = new HashMap<>();
        DataStore st = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "decode where db_code = " + db_code);
        for (DataRecord rec : st) {
            String tableName = rec.getValueString("table_name").toUpperCase();
            Map tableSlots = getTableSlots(tableName);
            long own_slot = rec.getValueLong("own_slot");
            tableSlots.put(rec.getValueLong("db_slot"), own_slot);
        }

        // Свободные слоты
        tablesSlotMax = new HashMap<>();
        st = db.loadSql("select table_name, max(own_slot) as own_slot_max from " + JdxUtils.sys_table_prefix + "decode group by table_name");
        for (DataRecord rec : st) {
            String tableName = rec.getValueString("table_name").toUpperCase();
            tablesSlotMax.put(tableName, rec.getValueLong("own_slot_max"));
        }
    }

    private Map getTableSlots(String tableName) {
        Map tableSlots = tablesDecodeSlots.get(tableName);
        if (tableSlots == null) {
            tableSlots = new HashMap<>();
            tablesDecodeSlots.put(tableName, tableSlots);
        }
        return tableSlots;
    }

    //
    public long getOrCreate_id_own(long db_id, String tableName) throws Exception {
        // Пробуем перекодировку имеющемися слотами
        long own_id = get_id_own(db_id, tableName);
        if (own_id != -1) {
            return own_id;
        }

        // Создаем новый слот
        long db_slot = db_id / SLOT_SIZE;
        //
        long own_slot = incTableSlotsMax(tableName);
        //
        Map tableSlots = getTableSlots(tableName);
        tableSlots.put(db_slot, own_slot);

        // Записываем новый слот
        Map params = UtCnv.toMap("db_code", db_code, "table_name", tableName, "db_slot", db_slot, "own_slot", own_slot);
        db.execSql("insert into " + JdxUtils.sys_table_prefix + "decode (db_code, table_name, db_slot, own_slot) values (:db_code, :table_name, :db_slot, :own_slot)", params);

        // Перекодировка через слот
        own_id = own_slot * SLOT_SIZE + db_id % SLOT_SIZE;

        //
        return own_id;
    }

    private long incTableSlotsMax(String tableName) {
        Long own_slot_max = tablesSlotMax.get(tableName);
        if (own_slot_max == null) {
            own_slot_max = -1L;
        }
        long own_slot = own_slot_max + 1;
        tablesSlotMax.put(tableName, own_slot);
        return own_slot;
    }

    public long get_id_own(long db_id, String tableName) {
        // Пробуем перекодировку имеющемися слотами
        long db_slot = db_id / SLOT_SIZE;
        Map tableSlots = getTableSlots(tableName);
        Long own_slot = (Long) tableSlots.get(db_slot);
        if (own_slot == null) {
            // Нет слота
            return -1;
        }

        // Перекодировка через слот
        long own_id = own_slot * SLOT_SIZE + db_id % SLOT_SIZE;

        //
        return own_id;
    }


}
