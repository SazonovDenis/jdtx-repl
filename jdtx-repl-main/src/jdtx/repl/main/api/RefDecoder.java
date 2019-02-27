package jdtx.repl.main.api;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;

import java.util.*;

/**
 * Перекодировщик ссылок.
 */
public class RefDecoder implements IRefDecoder {

    long SLOT_SIZE = 1000000;
    long SLOT_START_NUMBER = 100;  //todo: политика назначения диапазонов

    Db db = null;
    long ws_id = -1;

    protected Map<String, Map> allSlots;

    public RefDecoder(Db db, long ws_id) throws Exception {
        if (ws_id <= 0) {
            throw new XError("invalid ws_id <= 0");
        }

        //
        this.db = db;
        this.ws_id = ws_id;

        // Слоты
        allSlots = new HashMap<>();
    }

    // ------------------------------------------
    // IRefDecoder
    // ------------------------------------------

    public JdxRef get_ref(long own_id, String tableName) throws Exception {
        JdxRef ref = new JdxRef();

        if (own_id < SLOT_SIZE * SLOT_START_NUMBER) {
            ref.ws_id = this.ws_id;
            ref.id = own_id;
            return ref;
        }

        // Номер слота у нас
        long own_slot_no = own_id / SLOT_SIZE;
        // По нашему номеру слота определяем ws_id и ws_slot_no
        // todo: каждый раз делать select - дорого!!!
        DataRecord rec = db.loadSql("select * from z_z_decode where table_name ='" + tableName + "' and own_slot = " + own_slot_no).getCurRec();
        if (rec.getValueLong("own_slot") == 0) {
            throw new XError("Не найден слот " + own_slot_no);
        }
        long ws_slot_no = rec.getValueLong("ws_slot");
        long ws_id = rec.getValueLong("ws_id");
        long id = ws_slot_no * SLOT_SIZE + own_id % SLOT_SIZE;

        //
        ref.ws_id = ws_id;
        ref.id = id;
        return ref;
    }

    public long get_id_own(long db_id, long ws_id, String tableName) throws Exception {
        if (!needDecode(db_id, ws_id, tableName)) {
            return db_id;
        }

        // Пробуем перекодировку имеющемися слотами
        long own_id = find_id_own_internal(db_id, ws_id, tableName);
        if (own_id != -1) {
            return own_id;
        }

        // ---
        // Создаем новый слот
        long ws_slot_no = db_id / SLOT_SIZE;
        long own_slot_no = calcNextSlotNo(tableName);
        // Записываем новый слот в свой кэш
        Map slots = loadSlotsFor(ws_id, tableName);
        slots.put(ws_slot_no, own_slot_no);
        // Записываем новый слот в БД
        writeSlotToDb(ws_id, tableName, ws_slot_no, own_slot_no);


        // ---
        // Перекодировка через слот
        own_id = own_slot_no * SLOT_SIZE + db_id % SLOT_SIZE;


        // ---
        return own_id;
    }

    // ------------------------------------------
    //
    // ------------------------------------------


    /**
     * @return Нужно ли перекодировать
     */
    protected boolean needDecode(long db_id, long ws_id, String tableName) {
        // Свои id не перекодируем
        if (ws_id == this.ws_id) {
            return false;
        }

        // Для PS: id находится в нерегулируемом диапазоне.
        if (db_id <= 0) {
            // todo: возможно есть более сложное поведение, например "id до 1000",
            // или зависит от таблицы. Это может быть перекрыто в кастомных обработчиках
            return false;
        }

        //
        return true;
    }


    private long find_id_own_internal(long db_id, long ws_id, String tableName) throws Exception {
        if (!needDecode(db_id, ws_id, tableName)) {
            return db_id;
        }

        //
        long own_id = -1;

        // Берем слоты в кэше
        Map tableSlots = loadSlotsFor(ws_id, tableName);
        // Пробуем перекодировку имеющемися слотами
        long ws_slot_no = db_id / SLOT_SIZE;
        Long own_slot_no = (Long) tableSlots.get(ws_slot_no);
        // Уже есть слот в кэше?
        if (own_slot_no != null) {
            // Перекодировка через слот
            own_id = own_slot_no * SLOT_SIZE + db_id % SLOT_SIZE;
        }

        //
        return own_id;
    }

    // Записываем новый слот
    private void writeSlotToDb(long ws_id, String tableName, long ws_slot_no, long own_slot_no) throws Exception {
        Map params = UtCnv.toMap("ws_id", ws_id, "table_name", tableName, "ws_slot", ws_slot_no, "own_slot", own_slot_no);
        String sql = "insert into " + JdxUtils.sys_table_prefix + "decode (ws_id, table_name, ws_slot, own_slot) values (:ws_id, :table_name, :ws_slot, :own_slot)";
        db.execSql(sql, params);
    }


    private Map loadSlotsFor(long ws_id, String tableName) throws Exception {
        String key = ws_id + "|" + tableName;
        Map slots = allSlots.get(key);
        if (slots == null) {
            // Слоты для этой ws_id+tableName еще не кешировали
            slots = new HashMap<>();
            allSlots.put(key, slots);
            // Загружаем из БД
            DataStore st = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "decode where ws_id = " + ws_id + " and table_name = '" + tableName + "'");
            for (DataRecord rec : st) {
                slots.put(rec.getValueLong("ws_slot"), rec.getValueLong("own_slot"));
            }
        }
        return slots;
    }

    private long calcNextSlotNo(String tableName) throws Exception {
        DataRecord rec = db.loadSql("select max(own_slot) as own_slot_max, count(*) as cnt from " + JdxUtils.sys_table_prefix + "decode where table_name = '" + tableName + "'").getCurRec();
        long own_slot_max;
        if (rec.getValueLong("cnt") == 0) {
            own_slot_max = SLOT_START_NUMBER;
        } else {
            own_slot_max = rec.getValueLong("own_slot_max");
        }

        //
        return own_slot_max + 1;
    }


}
