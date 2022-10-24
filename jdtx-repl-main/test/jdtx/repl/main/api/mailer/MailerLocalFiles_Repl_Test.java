package jdtx.repl.main.api.mailer;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.junit.*;

public class MailerLocalFiles_Repl_Test extends JdxReplWsSrv_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_AllDir_LocalFiles() throws Exception {
        logOn();
        //
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);

        //
        sync_LocalFiles_1_2_3();
        sync_LocalFiles_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
    }

    @Test
    /**
     * Проверим, что после сеанса с LocalFiles можно продолжать через Http
     */
    public void test_Http_After_LocalFiles() throws Exception {
        logOn();

        // Внесем изменения
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);


        // Выполним сеанс для всех через http, а для ws2 - через localFiles
        ws_doReplSessionLocalFiles(db2);


        // Внесем изменения второй раз
        test_ws1_makeChange_Unimportant();
        test_ws2_makeChange();
        test_ws3_makeChange();
        //
        utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);


        // Теперь продолжаем через http
        sync_http_1_2_3();
        sync_http_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);
    }

    @Test
    public void sync_LocalFiles_1_2_3() throws Exception {
        srv_doReplSessionLocalFiles();

        ws_doReplSessionLocalFiles(db);
        ws_doReplSessionLocalFiles(db2);
        ws_doReplSessionLocalFiles(db3);

        srv_doReplSessionLocalFiles();

        ws_doReplSessionLocalFiles(db);
        ws_doReplSessionLocalFiles(db2);
        ws_doReplSessionLocalFiles(db3);

        srv_doReplSessionLocalFiles();
    }

    @Test
    public void sync_LocalFiles_1_2_3_dump() throws Exception {
        sync_LocalFiles_1_2_3();
        sync_LocalFiles_1_2_3();

        //
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
    }

/*
    @Test
    public void ws2_doReceiveDir() throws Exception {
        // Рабочая станция
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        //
        System.out.println("Забираем входящие реплики");
        String dirName = "temp/MailerLocalFiles/";
        ws.replicasReceiveDir(dirName);
    }

    @Test
    public void ws2_doReplSessionLocalFiles() throws Exception {
        ws_doReplSessionLocalFiles(db2);
    }
*/

    public void ws_doReplSessionLocalFiles(Db db) throws Exception {
        String dirName = "temp/MailerLocalFiles/";

        // Рабочая станция
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        System.out.println("Отслеживаем и обрабатываем свои изменения");
        ws.handleSelfAudit();

        //
        System.out.println("Отправляем свои реплики");
        ws.replicasSendDir(dirName);

        //
        System.out.println("Забираем входящие реплики");
        ws.replicasReceiveDir(dirName);

        //
        System.out.println("Применяем входящие реплики");
        try {
            ws.handleAllQueIn();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        //
        System.out.println("Отправляем свои реплики (ответ на входящие)");
        ws.replicasSendDir(dirName);
    }

    void srv_doReplSessionLocalFiles() throws Exception {
        String dirName = "temp/MailerLocalFiles/";

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        //
        srv.srvReplicasSendDir(dirName);
        srv.srvReplicasReceiveDir(dirName);

        //
        System.out.println("Формирование общей очереди");
        srv.srvHandleCommonQue();

        //
        System.out.println("Тиражирование реплик");
        srv.srvReplicasDispatch();

        //
        srv.srvReplicasSendDir(dirName);
        srv.srvReplicasReceiveDir(dirName);
    }

}
