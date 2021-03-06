package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.db.*;
import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class GroupStrategy_Test extends DbmTestCase {

    Db db;
    IJdxDbStruct struct;


    @Override
    public void setUp() throws Exception {
        super.setUp();
        //
        db = dbm.getDb();
        System.out.println("db: " + db.getDbSource().getDatabase());
        //
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
    }


    @Test
    public void test_assignNotEmptyFields() throws Exception {
        logOn();
        //
        UtRecMerge utRecMerge = new UtRecMerge(db, struct);

        Map<String, Object> record = new HashMap<>();
        Map<String, Object> recordRes = new HashMap<>();
        GroupsStrategyStorage groupsStrategyStorage = new GroupsStrategyStorage();

        JSONObject cfg = UtRepl.loadAndValidateJsonFile("test/etalon/field_groups.json");
        groupsStrategyStorage.loadStrategy(cfg, struct);
        //
        GroupStrategy tableGroups = groupsStrategyStorage.getForTable("LIC");

        // Проверяем
        record.put("NAMEF", "NameF_V1");
        record.put("NAMEI", "NameI_V1");
        record.put("NAMEO", "NameI_V1");
        record.put("DOM", "Dom_V1");
        record.put("RNN", "Rnn_V1");
        //record.put("KV", "Kv_V1");
        //
        recordRes.put("NAMEF", "NameF_V2");
        recordRes.put("NAMEI", "NameI_V2");
        recordRes.put("DOM", "Dom_V2");
        recordRes.put("KV", "Kv_V2");
        recordRes.put("DOCNO", "DocNo_V2");
        //
        printFields(struct.getTable("LIC"));
        printValues(record, struct.getTable("LIC"));
        printValues(recordRes, struct.getTable("LIC"));
        //
        utRecMerge.assignNotEmptyFields(record, recordRes, tableGroups);
        //
        printValues(recordRes, struct.getTable("LIC"));
        //
        assertEquals("NameF_V1", recordRes.get("NAMEF"));
        assertEquals("NameI_V1", recordRes.get("NAMEI"));
        assertEquals("NameI_V1", recordRes.get("NAMEO"));
        assertEquals("Dom_V2", recordRes.get("DOM"));
        assertEquals("Kv_V2", recordRes.get("KV"));
        assertEquals("Rnn_V1", recordRes.get("RNN"));
        assertEquals("DocNo_V2", recordRes.get("DOCNO"));
    }

    void printFields(IJdxTable struct) {
        for (IJdxField field : struct.getFields()) {
            System.out.print("| " + UtString.padLeft(field.getName(), 10));
        }
        System.out.print(" |");
        System.out.println();
    }

    void printValues(Map<String, Object> record, IJdxTable struct) {
        for (IJdxField field : struct.getFields()) {
            System.out.print("| " + UtString.padLeft((String) record.get(field.getName()), 10));
        }
        System.out.print(" |");
        System.out.println();
    }


}