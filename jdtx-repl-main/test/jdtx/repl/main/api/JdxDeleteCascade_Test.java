package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.junit.*;

import java.util.*;

public class JdxDeleteCascade_Test extends JdxReplWsSrv_Test {


    public JdxDeleteCascade_Test() throws Exception {
        super();
        //
        cfg_json_publication_srv = "test/etalon/publication_regionTip_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_regionTip_152_ws.json";
    }


    @Test
    public void prepareEtalon_TestAll() throws Exception {
        DbUtils dbuSrv = new DbUtils(db, struct);
        DbUtils dbu2 = new DbUtils(db2, struct2);
        DbUtils dbu3 = new DbUtils(db3, struct3);

        // Первичная инициализация
        allSetUp();
        //
        sync_http();
        sync_http();
        sync_http();
        sync_http();
        //
        printRegionTip();

        //
        assertEquals(15, get_regionTip_cnt(dbuSrv));
        assertEquals(15, get_regionTip_cnt(dbu2));
        assertEquals(15, get_regionTip_cnt(dbu3));
    }


    @Test
    public void testCnt() throws Exception {
        DbUtils dbuSrv = new DbUtils(db, struct);
        DbUtils dbu2 = new DbUtils(db2, struct2);
        DbUtils dbu3 = new DbUtils(db3, struct3);
        //
        System.out.println(get_regionTip_cnt(dbuSrv));
        System.out.println(get_regionTip_cnt(dbu2));
        System.out.println(get_regionTip_cnt(dbu3));
    }


    @Test
    public void test_CascadeDel() throws Exception {
        DbUtils dbuSrv = new DbUtils(db, struct);
        DbUtils dbu2 = new DbUtils(db2, struct2);
        DbUtils dbu3 = new DbUtils(db3, struct3);
        Random rnd = new Random();
        rnd.setSeed(0);


        // ---
        printRegionTip();

        //
        assertEquals(15, get_regionTip_cnt(dbuSrv));
        assertEquals(15, get_regionTip_cnt(dbu2));
        assertEquals(15, get_regionTip_cnt(dbu3));


        // ---
        // ws2: вставка regionTip + region
        String ws2_regionTip = "regionTip-ws:2-" + rnd.nextInt();
        long ws2_id0_regionTip = dbu2.getNextGenerator("g_regionTip");
        dbu2.insertRec("regionTip", UtCnv.toMap(
                "id", ws2_id0_regionTip,
                "deleted", 0,
                "name", ws2_regionTip,
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        long ws2_id0_region = dbu2.getNextGenerator("g_region");
        dbu2.insertRec("region", UtCnv.toMap(
                "id", ws2_id0_region,
                "parent", 0,
                "regionTip", ws2_id0_regionTip,
                "deleted", 0,
                "name", "region-ws:2-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        //System.out.println("ws3_id0_regionTip: " + ws2_id0_regionTip);


        // ---
        // ws3: вставка regionTip + region
        String ws3_regionTip = "regionTip-ws:3-" + rnd.nextInt();
        long ws3_id0_regionTip = dbu3.getNextGenerator("g_regionTip");
        dbu3.insertRec("regionTip", UtCnv.toMap(
                "id", ws3_id0_regionTip,
                "deleted", 0,
                "name", ws3_regionTip,
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        long ws3_id0_region = dbu3.getNextGenerator("g_region");
        dbu3.insertRec("region", UtCnv.toMap(
                "id", ws3_id0_region,
                "parent", 0,
                "regionTip", ws3_id0_regionTip,
                "deleted", 0,
                "name", "region-ws:3-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        //System.out.println("ws3_id0_regionTip: " + ws3_id0_regionTip);

        //
        assertEquals(15, get_regionTip_cnt(dbuSrv));
        assertEquals(16, get_regionTip_cnt(dbu2));
        assertEquals(16, get_regionTip_cnt(dbu3));


        // ---
        // Синхронизация после вставки
        sync_http();
        sync_http();


        // ---
        System.out.println("После вставки и синхронизации");
        //
        printRegionTip();

        //
        assertEquals(17, get_regionTip_cnt(dbuSrv));
        assertEquals(17, get_regionTip_cnt(dbu2));
        assertEquals(17, get_regionTip_cnt(dbu3));


        // ---
        // ws2: Удаление чужих записей (ws3:regionTip)
        dbu2.db.execSql("delete from regionTip where Name = :Name'", UtCnv.toMap("Name", ws3_regionTip));

        // ws3: Удаление чужих записей (ws2:regionTip)
        dbu3.db.execSql("delete from regionTip where Name = :Name'", UtCnv.toMap("Name", ws2_regionTip));


        // ---
        System.out.println("После удаления, до синхронизации");
        //
        printRegionTip();

        //
        assertEquals(17, get_regionTip_cnt(dbuSrv));
        assertEquals(16, get_regionTip_cnt(dbu2));
        assertEquals(16, get_regionTip_cnt(dbu3));


        // ---
        // Синхронизация после удаления
        sync_http();
        sync_http();


        // ---
        System.out.println("Окончательно, после синхронизации");
        //
        printRegionTip();

        //
        assertEquals(17, get_regionTip_cnt(dbuSrv));
        assertEquals(17, get_regionTip_cnt(dbu2));
        assertEquals(17, get_regionTip_cnt(dbu3));
    }

    @Test
    public void printRegionTip() throws Exception {
        //String sql = "select Region.id, Region.Name, Region.RegionTip, RegionTip.Name RegionTipName from Region full join RegionTip on Region.RegionTip = RegionTip.id where Region.id <> 0 order by Region.id";
        String sql = "select * from RegionTip where RegionTip.id <> 0 order by RegionTip.id";

        //
        DbUtils dbu1 = new DbUtils(db, struct);
        DbUtils dbu2 = new DbUtils(db2, struct2);
        DbUtils dbu3 = new DbUtils(db3, struct3);

        //
        System.out.println("srv");
        UtData.outTable(dbu1.db.loadSql(sql));
        System.out.println("ws2");
        UtData.outTable(dbu2.db.loadSql(sql));
        System.out.println("ws3");
        UtData.outTable(dbu3.db.loadSql(sql));
    }

    private long get_regionTip_cnt(DbUtils dbu) throws Exception {
        return dbu.db.loadSql("select count(*) cnt from regionTip where id <> 0").getCurRec().getValueLong("cnt");
    }


}

