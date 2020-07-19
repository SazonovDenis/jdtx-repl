package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

public class JdxReplWsSrv_FailedInsertUpdate_Test extends JdxReplWsSrv_Test {


    long UlzTip = 5; //Тупик
    String UlzTipName = "Тупик";
    long Region = 117; //Астана
    String RegionName = "Астана";


    @Test
    public void test() throws Exception {
        logOn();

        // Первичная инициализация
        allSetUp();
        //
        select("temp/0.txt");
        System.out.println("---");
        //
        sync_http();
        sync_http();
        sync_http();
        sync_http();

        // 
        select("temp/1.txt");
        System.out.println("---");

        //
        deleteRef_db2();

        //
        select("temp/2.txt");
        System.out.println("---");

        //
        sync_http_1_2();
        sync_http_1_2();

        //
        select("temp/3.txt");
        System.out.println("---");

        //
        sync_http_3_noSend();
        //
        useDeletedRef_db3();

        //
        select("temp/4.txt");
        System.out.println("---");

        //
        sync_http();
        sync_http();
        sync_http();
        sync_http();

        //
        select("temp/5.txt");
    }

    private void sync_http_1_2() throws Exception {
        test_ws1_doReplSession();
        test_ws2_doReplSession();

        test_srv_doReplSession();

        test_ws1_doReplSession();
        test_ws2_doReplSession();
    }

    private void sync_http_3_noSend() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();
        ws.handleSelfAudit();
        ws.handleQueIn();
    }

    @Test
    public void test_select() throws Exception {
        select(null);
    }

    void select(String fileName) throws Exception {
        String sql = "select\n" +
                "  Ulz.id,\n" +
                "  Ulz.name,\n" +
                "  UlzTip.id as UlzTip,\n" +
                "  UlzTip.name as UlzTipName\n" +
                "from\n" +
                "  Ulz\n" +
                "  full join UlzTip on (Ulz.UlzTip = UlzTip.id)\n" +
                "where\n" +
                "  UlzTip.Name in ('Переулок','Тупик')\n" +
                "order by\n" +
                "  Ulz.id,\n" +
                "  UlzTip.id";
        String s = "";
        // srv
        DataStore st1 = db.loadSql(sql);
        s = s + "\n" +outTable(st1);
        // db2
        DataStore st2 = db2.loadSql(sql);
        s = s + "\n" + outTable(st2);
        // db3
        DataStore st3 = db3.loadSql(sql);
        s = s + "\n" + outTable(st3);
        //
        if (fileName != null) {
            FileUtils.writeStringToFile(new File(fileName), s);
        }
    }

    @Test
    public void test_deleteRef() throws Exception {
        test_select();
        deleteRef_db2();
        test_select();
    }

    @Test
    public void test_useDeletedRef_db3() throws Exception {
        useDeletedRef_db3();
        test_select();
    }

    void useDeletedRef_db3() throws Exception {
        DbUtils dbu = new DbUtils(db3, struct3);
        dbu.insertRec("Ulz", UtCnv.toMap("Name", "Новая ХХХ", "UlzTip", UlzTip, "Region", Region));
    }

    void deleteRef_db2() throws Exception {
        db2.execSql("delete from UlzTip where UlzTip.Name in ('" + UlzTipName + "')");
    }

    String outTable(DataStore t) {
        OutTableSaver sv = new OutTableSaver(t);
        String s = sv.save().toString();
        System.out.println(s);
        return s;
    }

}

