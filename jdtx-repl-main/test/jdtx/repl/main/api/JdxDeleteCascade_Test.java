package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.junit.*;

import java.util.*;

public class JdxDeleteCascade_Test extends JdxReplWsSrv_Test {


    public JdxDeleteCascade_Test() {
        super();
        cfg_json_publication_srv = "test/etalon/publication_regionTip_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_regionTip_152_ws.json";
    }

    @Test
    public void prepareEtalon_TestAll() throws Exception {
        // Первичная инициализация
        super.allSetUp();
        sync_http();
        sync_http();
        //
        printRegionTip();
    }


    @Test
    public void test1() throws Exception {
        logOff();

        //
        DbUtils dbu2 = new DbUtils(db2, struct2);
        DbUtils dbu3 = new DbUtils(db3, struct3);

        //
        Random rnd = new Random();
        rnd.setSeed(0);


        // ---
        // Первичная синхронизация
        sync_http();
        sync_http();


        // ---
        printRegionTip();


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
        System.out.println("ws3_id0_regionTip: " + ws2_id0_regionTip);


        // ---
        // ws3: вставка regionTip + region
        String ws3_regionTip = "regionTip-ws:3-" + rnd.nextInt();
        long ws3_id0_regionTip = dbu2.getNextGenerator("g_regionTip");
        dbu3.insertRec("regionTip", UtCnv.toMap(
                "id", ws3_id0_regionTip,
                "deleted", 0,
                "name", ws3_regionTip,
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        long ws3_id0_region = dbu2.getNextGenerator("g_region");
        dbu3.insertRec("region", UtCnv.toMap(
                "id", ws3_id0_region,
                "parent", 0,
                "regionTip", ws3_id0_regionTip,
                "deleted", 0,
                "name", "region-ws:3-" + rnd.nextInt(),
                "shortName", "sn-" + rnd.nextInt()
        ));
        //
        System.out.println("ws3_id0_regionTip: " + ws3_id0_regionTip);


        // ---
        // Синхронизация после вставки
        sync_http();
        sync_http();


        // ---
        System.out.println("После вставки");
        //
        printRegionTip();


        // ---
        // ws2: Удаление чужих записей (ws3:regionTip)
        dbu2.db.execSql("delete from regionTip where Name = :Name'", UtCnv.toMap("Name", ws3_regionTip));

        // ws3: Удаление чужих записей (ws2:regionTip)
        dbu3.db.execSql("delete from regionTip where Name = :Name'", UtCnv.toMap("Name", ws2_regionTip));


        // ---
        System.out.println("После удаления");
        //
        printRegionTip();


        // ---
        // Синхронизация после удаления
        sync_http();
        sync_http();


        // ---
        System.out.println("Окончательно");
        //
        printRegionTip();
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


}

