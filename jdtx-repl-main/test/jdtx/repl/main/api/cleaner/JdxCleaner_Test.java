package jdtx.repl.main.api.cleaner;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.junit.*;

import java.util.*;

public class JdxCleaner_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        //
        super.setUp();
    }

    @Test
    public void test_getInfoWs() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        //
        Map<String, Object> info = ws.getInfoWs();
        System.out.println(info);
    }

    @Test
    public void readQueUsedStatus_2() throws Exception {
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        IMailer mailer = ws2.getMailer();

        // Узнаем, что использовано на ws2
        JdxCleaner cleaner = new JdxCleaner(null);
        JdxQueUsedState usedState = cleaner.readQueUsedStatus(mailer);
        System.out.println(ws2.getWsId() + ": " + usedState);
    }

    @Test
    public void sendQueUsedStatus_2() throws Exception {
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        IMailer mailer = ws2.getMailer();

        // Отправим на ws2
        JdxCleanTaskWs task = new JdxCleanTaskWs();
        task.queOutNo = 100;
        //
        JdxCleaner cleaner = new JdxCleaner(db);
        cleaner.sendQueCleanTask(mailer, task);


        // Рабочая станция - попытаемся выполнить очистку станции
        ws2.wsCleanupRepl();
    }

    @Test
    public void sendQueUsedStatus_2_big() throws Exception {
        JdxReplWs ws2 = new JdxReplWs(db2);
        ws2.init();
        IMailer mailer = ws2.getMailer();

        //
        JdxCleanTaskWs task;
        JdxCleaner cleaner = new JdxCleaner(db);


        // Отправим слишком большое задание на ws2
        task = new JdxCleanTaskWs();
        task.queOutNo = 1000000;
        task.queInNo = 10;
        task.queIn001No = 10;
        //
        cleaner.sendQueCleanTask(mailer, task);

        // Рабочая станция - попытаемся выполнить очистку станции
        try {
            ws2.wsCleanupRepl();
            throw new XError("Shoild Fail");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (!e.getMessage().contains("Нельзя удалять")) {
                throw e;
            }
        }


        // Отправим слишком большое задание на ws2
        task = new JdxCleanTaskWs();
        task.queOutNo = 10;
        task.queInNo = 1000000;
        task.queIn001No = 10;
        //
        cleaner.sendQueCleanTask(mailer, task);

        // Рабочая станция - попытаемся выполнить очистку станции
        try {
            ws2.wsCleanupRepl();
            throw new XError("Shoild Fail");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (!e.getMessage().contains("Нельзя удалять")) {
                throw e;
            }
        }


        // Отправим слишком большое задание на ws2
        task = new JdxCleanTaskWs();
        task.queOutNo = 10;
        task.queInNo = 10;
        task.queIn001No = 1000000;
        //
        cleaner.sendQueCleanTask(mailer, task);

        // Рабочая станция - попытаемся выполнить очистку станции
        try {
            ws2.wsCleanupRepl();
            throw new XError("Shoild Fail");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (!e.getMessage().contains("Нельзя удалять")) {
                throw e;
            }
        }


        // Отправим нормальное задание на ws2
        task = new JdxCleanTaskWs();
        task.queOutNo = 10;
        task.queInNo = 10;
        task.queIn001No = 10;
        //
        cleaner.sendQueCleanTask(mailer, task);

        // Рабочая станция - попытаемся выполнить очистку станции
        ws2.wsCleanupRepl();
    }

    @Test
    public void getWsQueInAges() throws Exception {
        JdxCleaner cleaner = new JdxCleaner(db);
        Map<Long, Long> res;

        //
        res = cleaner.get_WsQueOutNo_by_queCommonNo(100);
        System.out.println(res);

        //
        res = cleaner.get_WsQueOutNo_by_queCommonNo(200);
        System.out.println(res);
    }

    @Test
    public void doChanges_cleanQue() throws Exception {
        JdxReplWsSrv_Test test = new JdxReplWsSrv_Test();
        test.setUp();
        //
        test.test_ws1_makeChange_Unimportant();
        test.test_ws2_makeChange();
        test.test_ws3_makeChange();
        //
        UtTest utTest2 = new UtTest(db2);
        utTest2.make_Region_InsDel_0(struct2, 2);
        utTest2.make_Region_InsDel_1(struct2, 2);


        // Вторая станция отстает
        test.sync_http_1_3();
        test.sync_http_1_3();

        // Что покажет очистка аудита?
        System.out.println("=============");
        cleanQue();
        System.out.println("=============");


        // Все станции догнали
        test.sync_http_1_2_3();

        // Что теперь покажет очистка аудита?
        System.out.println("=============");
        cleanQue();
        System.out.println("=============");


        //
        //test.test_DumpTables_1_2_3();
    }

    @Test
    public void cleanQue() throws Exception {
        doCleanWs(db);
        doCleanWs(db2);
        doCleanWs(db3);
    }

    @Test
    public void cleanQue_2() throws Exception {
        doCleanWs(db2);
    }

    @Test
    public void cleanQue_3() throws Exception {
        doCleanWs(db3);
    }

    public void doCleanWs(Db wsDb) throws Exception {
        // Сервер
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Какие реплики больше не нужны на станциях?
        // Узнаем и отправим на станции
        srv.srvCleanupReplWs(Long.MAX_VALUE);


        // Рабочая станция
        JdxReplWs myWs = new JdxReplWs(wsDb);
        myWs.init();

        // Выполняем очистку станции myWs
        myWs.wsCleanupRepl();
    }

    @Test
    public void cleanSrv() throws Exception {
        // Сервер
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Какие реплики больше не нужны?
        // Узнаем и очистим на сервере
        srv.srvCleanupReplSrv(Long.MAX_VALUE);
    }

}
