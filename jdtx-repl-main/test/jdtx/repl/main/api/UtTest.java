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
                "NameF", "Name-F-ins-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "Name-I-ins-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "NameO-ins-ws:" + ws_id + "-" + rnd.nextInt()
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");


        //
        if (rnd.nextInt(5) == 0) {
            long id_del = db.loadSql("select min(id) id from lic where id > 1360").getCurRec().getValueLong("id");
            if (id_del > 0) {
                dbu.deleteRec("lic", id_del);
            }
        }
        if (rnd.nextInt(5) == 0) {
            long id_del = db.loadSql("select min(id) id from lic where id > 101001360").getCurRec().getValueLong("id");
            if (id_del > 0) {
                dbu.deleteRec("lic", id_del);
            }
        }

        //
        long id01 = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        long id02 = db.loadSql("select max(id) id from lic where id > " + (id01 + 100)).getCurRec().getValueLong("id");

        dbu.updateRec("lic", UtCnv.toMap(
                "id", id01,
                "NameF", "Name-F-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "Name-I-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "Name-O-upd-ws:" + ws_id + "-" + rnd.nextInt()
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,ulz,dom,kv,tel,info");

        dbu.updateRec("lic", UtCnv.toMap(
                "id", id02,
                "NameF", "NameF-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameI", "NameI-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "NameO", "NameO-upd-ws:" + ws_id + "-" + rnd.nextInt(),
                "Ulz", id1
        ), null, "bornDt,rnn,licDocTip,docNo,docSer,liCdocVid,docDt,region,dom,kv,tel,info");

        // Апдейт общей записи
        dbu.db.execSql("update Lic set NameI = :NameI where Dom = :Dom", UtCnv.toMap(
                "Dom", "12",
                "NameI", "NameI-upd-com-ws:" + ws_id + "-" + rnd.nextInt()
        ));
    }


}
