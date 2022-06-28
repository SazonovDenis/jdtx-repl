package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Асинхронное удаление записи справочника:
 * - удаление записи из справочника на одной рабочей станции (А)
 * - использование этой записи на другой рабочей станции (Б)
 * - приход реплики с использованием этой записи со станции Б на станцию А
 * Тренируемся на паре таблиц "Ulz" -> "UlzTip"
 */
public class JdxReplWsSrv_FailedInsertUpdate_Test extends JdxReplWsSrv_Test {


    long UlzTip = 5; //Тупик
    String UlzTipName = "Тупик";
    long Region = 117; //Астана
    String RegionName = "Астана";


    @Test
    public void test_failedInsertUpdate() throws Exception {
        // ---
        // Есть проблема
        test_ProblemDelete_Show();


        // ---
        // Исправляем проблему
        test_ProblemDelete_ManualRestoreDeleted();
        //
        select("temp/6.txt");


        // ---
        // Окончательная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Станции синхронизированы
        System.out.println("Станции синхронизированы");
        select("temp/x.txt");
    }

    @Test
    public void test_ProblemDelete_Show() throws Exception {
        logOn();

        // Создание репликации
        allSetUp();
        //
        select("temp/0.txt");
        System.out.println();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Состояние полной синхронности
        System.out.println("Состояние полной синхронности");
        select("temp/1.txt");
        System.out.println();

        //
        deleteRef_db2();

        // На db2 удалены якобы "лишние" записи в справочнике UlzTip
        System.out.println("На db2 удалены якобы 'лишние' записи в справочнике UlzTip");
        select("temp/2.txt");
        System.out.println();

        //
        sync_http_1_2();
        sync_http_1_2();

        // Станция db2 синхронна с сервером db1
        System.out.println("Станция db2 синхронна с сервером db1");
        select("temp/3.txt");
        System.out.println();

        //
        useDeletedRef_db3();

        // На db3 использовали "спорную" запись в справочнике UlzTip
        System.out.println("На db3 использовали 'спорную' запись в справочнике UlzTip");
        select("temp/4.txt");
        System.out.println();

        //
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Станциям не удалось синхронизироваться
        System.out.println("Станциям не удалось синхронизироваться");
        select("temp/5.txt");
    }

    @Test
    public void test_ProblemDelete_ManualRestoreDeleted() throws Exception {
        // Восстановим ошибочно удаленные записи на сервере
        test_manualRestoreDeleted_db(db);
        System.out.println("srv: ошибочно удаленные записи восстановлены в справочнике UlzTip");
        System.out.println();

        // Восстановим ошибочно удаленные записи на db2
        test_manualRestoreDeleted_db(db2);
        System.out.println("db2: ошибочно удаленные записи восстановлены в справочнике UlzTip");
        System.out.println();
    }

/*
    private void sync_http_3_noSend() throws Exception {
        JdxReplWs ws = new JdxReplWs(db3);
        ws.init();
        ws.handleSelfAudit();
        ws.handleQueIn();
    }
*/

    @Test
    public void test_select() throws Exception {
        select(null);
    }

    void select(String resFileName) throws Exception {
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
                "  UlzTip.Name,\n" +
                "  Ulz.Name,\n" +
                "  Ulz.id,\n" +
                "  UlzTip.id";
        String s = "";
        // srv
        DataStore st1 = db.loadSql(sql);
        s = s + "\n" + dumpDataStore(st1);
        // db2
        DataStore st2 = db2.loadSql(sql);
        s = s + "\n" + dumpDataStore(st2);
        // db3
        DataStore st3 = db3.loadSql(sql);
        s = s + "\n" + dumpDataStore(st3);
        //
        if (resFileName != null) {
            FileUtils.writeStringToFile(new File(resFileName), s);
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

    @Test
    public void test_sync_select() throws Exception {
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        select(null);
    }

    void test_manualRestoreDeleted_db(Db dbX) throws Exception {
        String sql = "select * from UlzTip where UlzTip.Name in ('Переулок','Тупик') order by Name, id";

        // ---
        DataStore st0 = dbX.loadSql(sql);
        UtData.outTable(st0);

        // ---
        JdxReplWs wsX = new JdxReplWs(dbX);
        wsX.init();
        //
        String replicaFileName = wsX.dataRoot + "temp/ULZTIP_3_5.zip";
        File replicaFile = new File(replicaFileName);
        //
        wsX.useReplicaFile(replicaFile);

        // ---
        DataStore st1 = dbX.loadSql(sql);
        UtData.outTable(st1);
    }

    void useDeletedRef_db3() throws Exception {
        JdxDbUtils dbu = new JdxDbUtils(db3, struct3);
        dbu.insertRec("Ulz", UtCnv.toMap("Name", "Новая ул ХХХ", "UlzTip", UlzTip, "Region", Region));
    }

    void deleteRef_db2() throws Exception {
        db2.execSql("delete from UlzTip where UlzTip.Name in ('" + UlzTipName + "')");
    }

    String dumpDataStore(DataStore t) {
        OutTableSaver sv = new OutTableSaver(t);
        String s = sv.save().toString();
        System.out.println(s);
        return s;
    }

}

