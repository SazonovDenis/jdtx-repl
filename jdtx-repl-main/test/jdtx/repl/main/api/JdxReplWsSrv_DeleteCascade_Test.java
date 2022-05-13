package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

import java.util.*;

public class JdxReplWsSrv_DeleteCascade_Test extends JdxReplWsSrv_Test {


    public JdxReplWsSrv_DeleteCascade_Test() throws Exception {
        super();
        //
        cfg_json_publication_srv = "test/etalon/publication_regionTip_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_regionTip_152_ws.json";
    }


    /**
     * Корректность удаления своих и чужих ссылок
     */
    @Test
    public void test_allSetUp_CascadeDel() throws Exception {
        JdxDbUtils dbuSrv = new JdxDbUtils(db, struct);
        JdxDbUtils dbu2 = new JdxDbUtils(db2, struct2);
        JdxDbUtils dbu3 = new JdxDbUtils(db3, struct3);

        // Создание репликации
        allSetUp();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        printRegionTip();

        //
        assertEquals(15, get_regionTip_cnt(dbuSrv));
        assertEquals(15, get_regionTip_cnt(dbu2));
        assertEquals(15, get_regionTip_cnt(dbu3));

        //
        doCascadeDel(15);
    }

    @Test
    public void test_CascadeDel() throws Exception {
        doCascadeDel(16);
    }

    void doCascadeDel(long recordCount) throws Exception {
        JdxDbUtils dbuSrv = new JdxDbUtils(db, struct);
        JdxDbUtils dbu2 = new JdxDbUtils(db2, struct2);
        JdxDbUtils dbu3 = new JdxDbUtils(db3, struct3);
        Random rnd = new Random();
        rnd.setSeed(0);


        // ---
        printRegionTip();

        //
        assertEquals(recordCount, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount, get_regionTip_cnt(dbu2));
        assertEquals(recordCount, get_regionTip_cnt(dbu3));


        // ---
        // ws2: вставка regionTip + region
        String ws2_regionTip_Name = "regionTip-ws:2-" + rnd.nextInt();
        long ws2_regionTip_id = dbu2.getNextGenerator("g_regionTip");
        dbu2.insertRec("regionTip", UtCnv.toMap(
                "id", ws2_regionTip_id,
                "deleted", 0,
                "name", ws2_regionTip_Name,
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        long ws2_id0_region = dbu2.getNextGenerator("g_region");
        dbu2.insertRec("region", UtCnv.toMap(
                "id", ws2_id0_region,
                "parent", 0,
                "regionTip", ws2_regionTip_id,
                "deleted", 0,
                "name", "region-ws:2-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));


        // ---
        // ws3: вставка regionTip + region
        String ws3_regionTip_Name = "regionTip-ws:3-" + rnd.nextInt();
        long ws3_regionTip_id = dbu3.getNextGenerator("g_regionTip");
        dbu3.insertRec("regionTip", UtCnv.toMap(
                "id", ws3_regionTip_id,
                "deleted", 0,
                "name", ws3_regionTip_Name,
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        long ws3_id0_region = dbu3.getNextGenerator("g_region");
        dbu3.insertRec("region", UtCnv.toMap(
                "id", ws3_id0_region,
                "parent", 0,
                "regionTip", ws3_regionTip_id,
                "deleted", 0,
                "name", "region-ws:3-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));

        //
        assertEquals(recordCount, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu2));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu3));

        // ---
        System.out.println("После вставки, до синхронизации");
        //
        printRegionTip();


        // ---
        // Синхронизация после вставки
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        System.out.println("После вставки и синхронизации");
        //
        printRegionTip();

        //
        assertEquals(recordCount + 2, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount + 2, get_regionTip_cnt(dbu2));
        assertEquals(recordCount + 2, get_regionTip_cnt(dbu3));


        // ---
        // ws2: Удаление чужих записей (ws3:regionTip)
        db2.execSql("delete from regionTip where Name = :Name'", UtCnv.toMap("Name", ws3_regionTip_Name));

        // ws3: Удаление чужих записей (ws2:regionTip)
        db3.execSql("delete from regionTip where Name = :Name'", UtCnv.toMap("Name", ws2_regionTip_Name));


        // ---
        System.out.println("После удаления чужих на ws2 и ws3, до синхронизации");
        //
        printRegionTip();

        //
        assertEquals(recordCount + 2, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu2));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu3));


        // ---
        // Синхронизация после удаления чужих на ws2 и ws3
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        System.out.println("После удаления чужих на ws2 и ws3, после синхронизации");
        //
        printRegionTip();

        //
        assertEquals(recordCount + 2, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount + 2, get_regionTip_cnt(dbu2));
        assertEquals(recordCount + 2, get_regionTip_cnt(dbu3));


        // ---
        // ws2: Удале СВОИХ записей (ws2:regionTip)
        db2.execSql("delete from region where regionTip = :regionTip'", UtCnv.toMap("regionTip", ws2_regionTip_id));
        db2.execSql("delete from regionTip where id = :id'", UtCnv.toMap("id", ws2_regionTip_id));

        //
        assertEquals(recordCount + 2, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu2));
        assertEquals(recordCount + 2, get_regionTip_cnt(dbu3));


        // ---
        System.out.println("После удаления своих на ws2, до синхронизации");
        //
        printRegionTip();


        // ---
        // Синхронизация после удаления своих на ws2
        sync_http_1_2_3();
        sync_http_1_2_3();


        // ---
        System.out.println("После удаления своих на ws2, после синхронизации");
        //
        printRegionTip();

        //
        assertEquals(recordCount + 1, get_regionTip_cnt(dbuSrv));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu2));
        assertEquals(recordCount + 1, get_regionTip_cnt(dbu3));
    }

    @Test
    public void testCnt() throws Exception {
        JdxDbUtils dbuSrv = new JdxDbUtils(db, struct);
        JdxDbUtils dbu2 = new JdxDbUtils(db2, struct2);
        JdxDbUtils dbu3 = new JdxDbUtils(db3, struct3);
        //
        System.out.println(get_regionTip_cnt(dbuSrv));
        System.out.println(get_regionTip_cnt(dbu2));
        System.out.println(get_regionTip_cnt(dbu3));
    }


    @Test
    public void printRegionTip() throws Exception {
        //String sql = "select Region.id, Region.Name, Region.RegionTip, RegionTip.Name RegionTipName from Region full join RegionTip on Region.RegionTip = RegionTip.id where Region.id <> 0 order by Region.id";
        String sql = "select * from RegionTip where RegionTip.id <> 0 order by RegionTip.id";

        //
        System.out.println("srv");
        UtData.outTable(db.loadSql(sql));
        System.out.println("ws2");
        UtData.outTable(db2.loadSql(sql));
        System.out.println("ws3");
        UtData.outTable(db3.loadSql(sql));
    }

    long get_regionTip_cnt(JdxDbUtils dbu) throws Exception {
        return dbu.loadSqlRec("select count(*) cnt from regionTip where id <> 0", null).getValueLong("cnt");
    }


}

