package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.test.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 */
public class UtTest extends UtilsTestCase {

    Db db;

    public UtTest(Db db) {
        this.db = db;
    }

    public void compareStruct(IJdxDbStruct struct_1, IJdxDbStruct struct_2) {
        assertEquals("Количество таблиц", struct_1.getTables().size(), struct_2.getTables().size());
        for (int t = 0; t < struct_1.getTables().size(); t++) {
            assertEquals("Количество полей в таблице " + struct_1.getTables().get(t).getName(), struct_1.getTables().get(t).getFields().size(), struct_2.getTables().get(t).getFields().size());
            for (int f = 0; f < struct_1.getTables().get(t).getFields().size(); f++) {
                assertEquals("Поля в таблице " + struct_1.getTables().get(t).getName(), struct_1.getTables().get(t).getFields().get(f).getName(), struct_2.getTables().get(t).getFields().get(f).getName());
            }
        }
    }

    public void dumpTable(String tableName, String outFileName, String sortBy) throws Exception {
        UtFile.mkdirs(outFileName.substring(0, outFileName.length() - UtFile.filename(outFileName).length()));
        //
        String sql = "select * from " + tableName;
        if (sortBy != null) {
            sql = sql + " order by " + sortBy;
        }
        DataStore st = db.loadSql(sql);
        //
        OutTableSaver svr = new OutTableSaver(st);
        svr.save().toFile(outFileName);
    }

    public void makeChange(IJdxDbStruct struct, long ws_id) throws Exception {
        Random rnd = new Random();
        DbUtils dbu = new DbUtils(db, struct);


        //
        long id0 = dbu.getNextGenerator("g_region");
        dbu.insertRec("region", UtCnv.toMap(
                "id", id0,
                "regionTip", 1,
                "parent", 0,
                "name", "Name-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ));

        long id1 = dbu.getNextGenerator("g_ulz");
        dbu.insertRec("ulz", UtCnv.toMap(
                "id", id1,
                "region", id0,
                "ulzTip", 2,
                "name", "Name-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ));

        long id2 = dbu.getNextGenerator("g_lic");
        dbu.insertRec("lic", UtCnv.toMap(
                "lic", id2,
                "ulz", id1,
                "NameF", "NameF-ins-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "NameI-ins-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "NameO-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");


        //
        long id01 = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        long id02 = db.loadSql("select max(id) id from lic where id > " + (id01 + 100)).getCurRec().getValueLong("id");

        dbu.updateRec("lic", UtCnv.toMap(
                "id", id01,
                "NameF", "NameF-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "NameI-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "NameO-upd-ws:" + ws_id + "-" + rnd.nextInt()
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,ulz,dom,kv,tel,info");

        dbu.updateRec("lic", UtCnv.toMap(
                "id", id02,
                "NameF", "NameF-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "NameI-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "NameO-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "Ulz", id1
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");
    }


}
