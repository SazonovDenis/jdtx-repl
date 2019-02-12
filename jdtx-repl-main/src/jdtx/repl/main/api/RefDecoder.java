package jdtx.repl.main.api;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;

import java.util.*;

/**
 * Перекодировщик ссылок.
 */
public class RefDecoder implements IRefDecoder {

    long SLOT_SIZE = 1000000;
    long IN_SLOT_START_NUMBER = 100;  //todo: политика назначения диапазонов

    long ws_id = -1;
    Db db = null;

    protected Map<String, Map> tablesDecodeSlots;
    protected Map<String, Long> tablesSlotMax;

    public RefDecoder(Db db, long ws_id) throws Exception {
        this.db = db;
        this.ws_id = ws_id;

        // Загрузим слоты для ws_id
        tablesDecodeSlots = new HashMap<>();
        DataStore st = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "decode where ws_id = " + ws_id);
        for (DataRecord rec : st) {
            String tableName = rec.getValueString("table_name").toUpperCase();
            Map tableSlots = getTableSlots(tableName);
            long own_slot = rec.getValueLong("own_slot");
            tableSlots.put(rec.getValueLong("ws_slot"), own_slot);
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
        // Id находится в нерегулируемом диапазоне
        if (db_id <= 0) {
            return db_id;
        }

        // Пробуем перекодировку имеющемися слотами
        long own_id = get_id_own(db_id, tableName);
        if (own_id != -1) {
            return own_id;
        }

        // Создаем новый слот
        long ws_slot = db_id / SLOT_SIZE;
        //
        long own_slot = incTableSlotsMax(tableName);
        //
        Map tableSlots = getTableSlots(tableName);
        tableSlots.put(ws_slot, own_slot);

        // Записываем новый слот
        Map params = UtCnv.toMap("ws_id", ws_id, "table_name", tableName, "ws_slot", ws_slot, "own_slot", own_slot);
        db.execSql("insert into " + JdxUtils.sys_table_prefix + "decode (ws_id, table_name, ws_slot, own_slot) values (:ws_id, :table_name, :ws_slot, :own_slot)", params);

        // Перекодировка через слот
        own_id = own_slot * SLOT_SIZE + db_id % SLOT_SIZE;

        //
        return own_id;
    }

    private long incTableSlotsMax(String tableName) {
        Long own_slot_max = tablesSlotMax.get(tableName);
        if (own_slot_max == null) {
            own_slot_max = IN_SLOT_START_NUMBER;
        }
        long own_slot = own_slot_max + 1;
        tablesSlotMax.put(tableName, own_slot);
        return own_slot;
    }

    public long get_id_own(long db_id, String tableName) {
        // Пробуем перекодировку имеющемися слотами
        long ws_slot = db_id / SLOT_SIZE;
        Map tableSlots = getTableSlots(tableName);
        Long own_slot = (Long) tableSlots.get(ws_slot);
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
