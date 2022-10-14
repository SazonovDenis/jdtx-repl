package jdtx.repl.main.api.cleaner;

import jandcode.dbm.db.*;
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
    public void readQueUsedStatus() throws Exception {
        JdxCleaner cleaner = new JdxCleaner(db);

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        Map<Long, IMailer> mailers = srv.mailerList;

        // Узнаем, что использовано на всех станциях
        Map<Long, JdxQueUsedState> usedStates = new HashMap<>();
        for (long wsId : mailers.keySet()) {
            IMailer mailer = mailers.get(wsId);
            JdxQueUsedState state = cleaner.readQueUsedStatus(mailer);
            usedStates.put(wsId, state);
        }

        // Печатаем
        for (long wsId : usedStates.keySet()) {
            JdxQueUsedState state = usedStates.get(wsId);
            System.out.println(wsId + ": " + state);
        }
    }

    @Test
    public void readQueUsedStatus_2() throws Exception {
        long wsId = 2;

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        IMailer mailer = srv.mailerList.get(wsId);

        // Узнаем, что использовано на ws2
        JdxCleaner cleaner = new JdxCleaner(null);
        JdxQueUsedState state = cleaner.readQueUsedStatus(mailer);
        System.out.println(wsId + ": " + state);
    }

    @Test
    public void sendQueUsedStatus_2() throws Exception {
        long wsId = 2;

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        IMailer mailer = srv.mailerList.get(wsId);


        //
        JdxQueUsedState state = new JdxQueUsedState();
        state.queOutUsed = 100;
        //
        JdxCleaner cleaner = new JdxCleaner(db);
        cleaner.sendQueUsedStatus(mailer, state);
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
        cleanQue_3();
        System.out.println("=============");


        // Все станции догнали
        test.sync_http_1_2_3();

        // Что теперь покажет очистка аудита?
        System.out.println("=============");
        cleanQue_3();
        System.out.println("=============");
    }

    @Test
    public void cleanQue_2() throws Exception {
        doCleanQue(2, db2);
    }

    @Test
    public void cleanQue_3() throws Exception {
        doCleanQue(3, db3);
    }

    //после удаления при репликации  возникает ошибка
    //utils.error.XError: Invalid replica.age: 3, que.age: 0

    //////////////////
    //////////////////
    //////////////////
    //////////////////
    //////////////////
    //////////////////
    // todo Важно, что может получится так, что для станции НЕ УДАСТСЯ прочитать достоверной информации о состоянии
    // ТОгда удалять реплики будет опасно - а вдруг рано????
    // Обеспечить на сервере хранение возраста использрванных станцией queCommon.
    // ТОгда можно информацию о состоянии применени получать частями
    //////////////////
    public void doCleanQue(long myWsId, Db myDb) throws Exception {
        JdxCleaner cleanerSrv = new JdxCleaner(db);

        // ---
        // Сервер
        // ---

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();
        Map<Long, IMailer> mailers = srv.mailerList;


        // Узнаем, что использовано на всех станциях
        Map<Long, JdxQueUsedState> usedStates = new HashMap<>();
        for (long wsId : mailers.keySet()) {
            IMailer mailer = mailers.get(wsId);
            JdxQueUsedState state = cleanerSrv.readQueUsedStatus(mailer);
            usedStates.put(wsId, state);
        }
        // Печатаем
        for (long wsId : usedStates.keySet()) {
            JdxQueUsedState state = usedStates.get(wsId);
            System.out.println("ws: " + wsId + ", ws.queIn.used: " + state.queInUsed);
        }


        // Определим худший возраст использования queCommon (на самой тормозной станции он будет меньше всех),
        // и все, что ранее этого возраста, больше никому не нужно и можно удалить у всех
        long queInUsedMin = Long.MAX_VALUE;
        for (long wsId : mailers.keySet()) {
            JdxQueUsedState usedState = usedStates.get(wsId);
            long queInUsed = usedState.queInUsed;
            if (queInUsed < queInUsedMin) {
                queInUsedMin = queInUsed;
            }
        }
        System.out.println("min queIn.used: " + queInUsedMin);


        // По номеру реплики из серверной ОБЩЕЙ очереди, которую приняли и использовали все рабочие станции,
        // для каждой рабочей станции узнаем, какой номер ИСХОДЯЩЕЙ очереди рабочей станции
        // уже принят и использован всеми другими станциями.
        Map<Long, Long> allQueOutNo = cleanerSrv.get_WsQueOutNo_by_queCommonNo(queInUsedMin);
        // Печатаем
        for (long wsId : allQueOutNo.keySet()) {
            long wsQueOutNo = allQueOutNo.get(wsId);
            System.out.println("ws: " + wsId + ", ws.queOut.no: " + wsQueOutNo);
        }


        // ---
        // Типа отправляем инфу на рабочую станцию, а она принимает
        // ---

        // ...
        // Какой номер ИСХОДЯЩЕЙ очереди рабочей станции 2 уже принят всеми?
        long wsQueOutNo = allQueOutNo.get(myWsId);
        System.out.println("wsId: " + myWsId + ", queIn.used: " + queInUsedMin + " -> ws.queOut.no: " + wsQueOutNo);
        // ...


        // ---
        // Рабочая станция ws2
        // ---

        //
        JdxReplWs myWs = new JdxReplWs(myDb);
        myWs.init();

        // Чистим аудит и реплики на myWsId
        JdxCleaner cleanerWs = new JdxCleaner(myDb);
        cleanerWs.cleanQue(myWs.queOut, wsQueOutNo, myWs.struct);
    }

}
