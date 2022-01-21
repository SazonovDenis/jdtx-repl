package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

import java.util.*;

public class JdxReplWsSrv_ApplyAudit_Test extends JdxReplWsSrv_Test {

    /**
     * Прогон сценария репликации: применение собственных сильно устаревших реплик
     */
    @Test
    public void test_Init_ApplyAudit() throws Exception {
        // Создаем репликацию (ws1, ws2, ws3)
        allSetUp();
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Формирование и применение собственных сильно устаревших реплик
        check_ApplyAudit();

        //
        test_DumpTables_1_2_3();
    }

    @Test
    public void test_ApplyAudit() throws Exception {
        // Формирование и применение собственных сильно устаревших реплик
        check_ApplyAudit();

        //
        test_DumpTables_1_2_3();
    }

    @Test
    public void test_make_change() throws Exception {
        make_change(2, db2, struct2, 201);
        //
        test_DumpTables_1_2_3();
    }

    private void check_ApplyAudit() throws Exception {
        long lic_id = getLicId(db2);

        //
        make_change(2, db2, struct2, 200);

        // Синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Синхронное состояние
        //test_DumpTables_1_2_3();

        // Делаем изменения, которые пока не обрабатываются сервером
        make_change(2, db2, struct2, 201);
        test_ws2_doReplSession();
        make_change(2, db2, struct2, 202);
        test_ws2_doReplSession();

        // Рассинхронизированное состояние
        //test_DumpTables_1_2_3();

        // Сервер проснулся
        test_srv_doReplSession();

        // Мы меняем данные, получаем состояние "val_222"
        int val_222 = 222;
        make_change(2, db2, struct2, val_222);

        // Синхронное состояние "val_222"
        DataStore st = db2.loadSql("select id,NameF,Dom from Lic where id = " + lic_id);
        System.out.println("===================");
        UtData.outTable(st);
        System.out.println("===================");
        assertEquals(val_222, st.getCurRec().getValueLong("Dom"));

        // Применяем реплики, возникшие ДО состояния "val_222",
        // реплику на наши изменения до состояния "val_222" сервер пока не прислал.
        test_ws2_doReplSession();

        // Устаревшее состояние - не должно применится при применении реплик,
        // должно сохранится состояние "val_222"
        st = db2.loadSql("select id,NameF,Dom from Lic where id = " + lic_id);
        System.out.println("===================");
        UtData.outTable(st);
        System.out.println("===================");
        assertEquals(val_222, st.getCurRec().getValueLong("Dom"));

        //// Синхронизация - не нужна, должно сохранится
        //test_ws2_doReplSession();
        //test_srv_doReplSession();
        //test_ws2_doReplSession();
        //test_srv_doReplSession();
        //
        //// Синхронное состояние "val_222"
        //st = db2.loadSql("select id,NameF,Dom from Lic where id = " + lic_id);
        //System.out.println("===================");
        //UtData.outTable(st);
        //System.out.println("===================");
        ////
        //assertEquals(val_222, st.getCurRec().getValueLong("Dom"));

        // Окончательная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Окончательное синхронное состояние "val_222"
        st = db2.loadSql("select id,NameF,Dom from Lic where id = " + lic_id);
        System.out.println("===================");
        UtData.outTable(st);
        System.out.println("===================");
        assertEquals(val_222, st.getCurRec().getValueLong("Dom"));
    }

    private long getLicId(Db db) throws Exception {
        long lic_id = db.loadSql("select min(id) id from lic where id > 0").getCurRec().getValueLong("id");
        return lic_id;
    }

    private void make_change(long ws_id, Db db, IJdxDbStruct struct, int val) throws Exception {
        UtTest.JdxRandom rnd = new UtTest.JdxRandom();
        rnd.setSeed(123456);
        JdxDbUtils dbu = new JdxDbUtils(db, struct);

        //
        long lic_id = getLicId(db);

        //
        Map values = UtCnv.toMap(
                "id", lic_id,
                "NameF", "UpdWs:" + ws_id + "-step:" + val + "-" + rnd.nextStr(5),
                "Dom", val
        );

        //
        dbu.updateRec("lic", values, "NameF,Dom");
    }

}
