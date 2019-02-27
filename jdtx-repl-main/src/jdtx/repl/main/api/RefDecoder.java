package jdtx.repl.main.api;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;

import java.util.*;

/**
 * �������������� ������.
 */
public class RefDecoder implements IRefDecoder {

    long SLOT_SIZE = 1000000;
    long IN_SLOT_START_NUMBER = 100;  //todo: �������� ���������� ����������

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

        // �����
        allSlots = new HashMap<>();
    }

    // ------------------------------------------
    // IRefDecoder
    // ------------------------------------------


    public long get_id_own(long db_id, long ws_id, String tableName) throws Exception {
        if (!needDecode(db_id, ws_id, tableName)) {
            return db_id;
        }

        // ������� ������������� ���������� �������
        long own_id = find_id_own_internal(db_id, ws_id, tableName);
        if (own_id != -1) {
            return own_id;
        }

        // ---
        // ������� ����� ����
        long ws_slot_no = db_id / SLOT_SIZE;
        long own_slot_no = calcNextSlotNo(tableName);
        // ���������� ����� ���� � ���� ���
        Map slots = loadSlotsFor(ws_id, tableName);
        slots.put(ws_slot_no, own_slot_no);
        // ���������� ����� ���� � ��
        writeSlotToDb(ws_id, tableName, ws_slot_no, own_slot_no);


        // ---
        // ������������� ����� ����
        own_id = own_slot_no * SLOT_SIZE + db_id % SLOT_SIZE;


        // ---
        return own_id;
    }

    // ------------------------------------------
    //
    // ------------------------------------------


    /**
     * @return ����� �� ��������������
     */
    protected boolean needDecode(long db_id, long ws_id, String tableName) {
        // ���� id �� ������������
        if (ws_id == this.ws_id) {
            return false;
        }

        // ��� PS: id ��������� � �������������� ���������.
        if (db_id <= 0) {
            // todo: �������� ���� ����� ������� ���������, �������� "id �� 1000",
            // ��� ������� �� �������. ��� ����� ���� ��������� � ��������� ������������
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

        // ����� ����� � ����
        Map tableSlots = loadSlotsFor(ws_id, tableName);
        // ������� ������������� ���������� �������
        long ws_slot_no = db_id / SLOT_SIZE;
        Long own_slot_no = (Long) tableSlots.get(ws_slot_no);
        // ��� ���� ���� � ����?
        if (own_slot_no != null) {
            // ������������� ����� ����
            own_id = own_slot_no * SLOT_SIZE + db_id % SLOT_SIZE;
        }

        //
        return own_id;
    }

    // ���������� ����� ����
    private void writeSlotToDb(long ws_id, String tableName, long ws_slot_no, long own_slot_no) throws Exception {
        Map params = UtCnv.toMap("ws_id", ws_id, "table_name", tableName, "ws_slot", ws_slot_no, "own_slot", own_slot_no);
        String sql = "insert into " + JdxUtils.sys_table_prefix + "decode (ws_id, table_name, ws_slot, own_slot) values (:ws_id, :table_name, :ws_slot, :own_slot)";
        db.execSql(sql, params);
    }


    private Map loadSlotsFor(long ws_id, String tableName) throws Exception {
        String key = ws_id + "|" + tableName;
        Map slots = allSlots.get(key);
        if (slots == null) {
            // ����� ��� ���� ws_id+tableName ��� �� ����������
            slots = new HashMap<>();
            allSlots.put(key, slots);
            // ��������� �� ��
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
            own_slot_max = IN_SLOT_START_NUMBER;
        } else {
            own_slot_max = rec.getValueLong("own_slot_max");
        }

        //
        return own_slot_max + 1;
    }


}
