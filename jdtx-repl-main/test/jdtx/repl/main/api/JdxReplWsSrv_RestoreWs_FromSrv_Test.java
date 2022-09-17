package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.utils.*;
import jandcode.utils.rt.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.io.*;
import org.junit.*;

import java.io.*;

/**
 * Проверка восстановления рабочей станции
 * по данным с сервера.
 */
public class JdxReplWsSrv_RestoreWs_FromSrv_Test extends JdxReplWsSrv_RestoreWs_Test {

    String cfg_json_snapshot;

    boolean doNolmalLifeBromBackup = false;

    public JdxReplWsSrv_RestoreWs_FromSrv_Test() {
        super();
        cfg_json_snapshot = "test/etalon/publication_full_152_snapshot.json";
    }

    /**
     * Проверка восстановления репликации рабочей станции
     * при полной потере базы рабочей станции и её репликационных каталогов, по данным с сервера.
     * test_DatabaseRestore_stepRuin - провоцирует ошибку,
     * test_DatabaseRestore_stepRepair - исправляет ее
     */
    @Test
    public void test_DirDB_srv() throws Exception {
        // Создание репликации, обычная работа - изменения в базах и синхронизация
        doSetUp_doNolmalLife_BeforeFail();

        // Аварийное событие
        databaseRestore_stepRuin();

        // Восстановление
        databaseRestore_stepRepair();
    }

    /**
     * Прогон сценария репликации: восстановление утраченной базы рабочей станции по данным с сервера,
     * с односторонним фильтром по LIC.
     */
    @Test
    public void test_DirDB_srv_filter() throws Exception {
        cfg_json_decode = "../install/cfg/decode_strategy_194.json";
        cfg_json_publication_srv = "test/etalon/publication_lic_152_srv.json";
        cfg_json_publication_ws = "test/etalon/publication_lic_152_ws.json";
        cfg_json_snapshot = "test/etalon/publication_lic_152_ws.json"; // <-- cfg_json_snapshot эта натройка почему то все ломае
        //cfg_json_snapshot = "test/etalon/publication_lic_152_snapshot.json";
        equalExpected = expectedEqual_filterLic;
        //
        test_DirDB_srv();
    }

    // Создание репликации,
    // обычная работа - изменения в базах и синхронизация
    // По дороге создаем две контрольных точки
    void doSetUp_doNolmalLife_BeforeFail() throws Exception {
        if (doNolmalLifeBromBackup) {
            System.out.println("-------------");
            System.out.println("Делаем doRestoreFromNolmalLife из ранее созданной копии");
            System.out.println("-------------");
            doRestoreFromNolmalLife();
            System.out.println("-------------");
            System.out.println("doRestoreFromNolmalLife - ok");
            System.out.println("-------------");
            return;
        }

        // Создание репликации
        allSetUp();

        // Первичная синхронизация
        sync_http_1_2_3();
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Удобно различать записи
        IDbGenerators pkGenerator2 = DbToolsService.getDbGenerators(db2);
        pkGenerator2.setValue("g_UsrLog", pkGenerator2.getValue("g_UsrLog") + 2000);
        IDbGenerators pkGenerator3 = DbToolsService.getDbGenerators(db3);
        pkGenerator3.setValue("g_UsrLog", pkGenerator3.getValue("g_UsrLog") + 3000);
        IDbGenerators pkGenerator5 = DbToolsService.getDbGenerators(db5);
        pkGenerator5.setValue("g_UsrLog", pkGenerator5.getValue("g_UsrLog") + 5000);


        // ---
        // Обычная работа: изменения в базах и синхронизация -
        // получаем синхронные базы
        test_AllHttp();

        //
        UtFile.cleanDir(backupDirName);
        doBackupNolmalLife();
    }

    void databaseRestore_stepRuin() throws Exception {
        // Проверим исходную синхронность
        System.out.println("Базы должны быть в синхронном состоянии");
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);


        // ---
        // Аварийное событие:
        // стираем текущую рабочую базу ws3 и ее рабочий каталог
        System.out.println("Аварийное событие");
        doDeleteDir(3);
        doDeleteDb(3);


        // ---
        // Жизнь после аварии
        // ... не делаем - ясно, что не получится
    }

    void databaseRestore_stepRepair() throws Exception {
        // Берем заготовку базы данных (на "рабочей станции")
        Rt rt = extWs3.getApp().getRt().getChild("db/default");
        String dbNameDest = rt.getValue("database").toString();
        String dbNameSour = rt.getValue("database_etalon").toString();
        //
        FileUtils.copyFile(new File(dbNameSour), new File(dbNameDest));
        //
        System.out.println();
        System.out.println("Эталонная база скопирована [" + dbNameDest + "]");
        //
        connectAll();


        // ---
        // Подаем команду для подготовки базы рабочей станции
        IVariantMap args = new VariantMap();
        args.clear();
        args.put("ws", 3);
        args.put("mail", mailUrl);
        args.put("guid", mailGuid);
        extWs3.repl_create(args);


        // ---
        // Сладко ли работается воскрешенной рабочей станции?
        sync_http_1_2_3();

        //
        System.out.println();
        System.out.println("Попытка синхронизации была неудачная");
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, expectedNotEqual_2isEmpty);


        // ---
        // Начальная попытка ремонта
        System.out.println();
        System.out.println("Начальная попытка ремонта");
        doStepRepair(db3, false);


        // тест равенства баз - с конфигом "карта равенства"

        // ---
        // Подаем команду для подготовки snaphot (на сервере)
        args.clear();
        args.put("ws", 3);
        //args.put("cfg_snapshot", cfg_json_snapshot);
        extSrv.repl_restore_ws(args);


        // ---
        // Первая попытка ремонта (ожидание от сервера)
        System.out.println();
        System.out.println("Первая попытка ремонта (ожидание от сервера)");
        doStepRepair(db3, false);

        // Сервер ответит на просьбы о повторной отправке
        System.out.println();
        System.out.println("Сервер ответит на просьбы о повторной отправке");
        test_srv_doReplSession();

        // Сейчас все готово для ремонта
        System.out.println();
        System.out.println("Последняя попытка ремонта");
        doStepRepair(db3, false);
        System.out.println();
        System.out.println("Удачная попытка ремонта");
        doStepRepair(db3, true);


        // ---
        // Сладко ли работается воскрешенной и РЕАНИМИРОВАННОЙ рабочей станции?
        sync_http_1_2_3();
        sync_http_1_2_3();

        // Проверим синхронность после восстановления
        System.out.println();
        System.out.println("Cинхронизация должна пройти нормально");
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        //
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);


        // --- Работаем после восстановления
        test_AllHttp();
        test_AllHttp();

        // Проверим синхронность после работы
        System.out.println("Cинхронизация должна пройти нормально");
        do_DumpTables(db, db2, db3, struct, struct2, struct3);
        //
        compareDb(db, db2, equalExpected);
        compareDb(db, db3, equalExpected);


        // --- Проверим работу que001
        test_mute_unmute();
    }

    @Test
    public void test_mute_unmute() throws Exception {
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(0, db.loadSql(sqlEnabledIs1MuteIs1).size());

        //
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();


        //
        srv.srvSendWsMute(3, UtQue.SRV_QUE_OUT001);
        test_AllHttp();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(1, db.loadSql(sqlEnabledIs1MuteIs1).size());


        //
        srv.srvSendWsMute(2, UtQue.SRV_QUE_OUT001);
        test_AllHttp();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(2, db.loadSql(sqlEnabledIs1MuteIs1).size());


        //
        srv.srvSendWsUnmute(2, UtQue.SRV_QUE_OUT001);
        srv.srvSendWsUnmute(3, UtQue.SRV_QUE_OUT001);
        test_AllHttp();

        // Проверяем (на сервере) ответ на сигнал - проверяем состояние MUTE
        UtData.outTable(db.loadSql(sqlEnabledIs1));
        assertEquals(0, db.loadSql(sqlEnabledIs1MuteIs1).size());
    }

    private String sqlWsList = "select\n" +
            "  WORKSTATION_LIST.ID as WS_ID,\n" +
            "  WORKSTATION_LIST.NAME,\n" +
            "  WORKSTATION_LIST.GUID,\n" +
            "  STATE__ENABLED.param_value as ENABLED,\n" +
            "  STATE__MUTE_AGE.param_value as MUTE_AGE\n" +
            "from\n" +
            "  " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST WORKSTATION_LIST\n" +
            "  left join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE STATE__ENABLED on (WORKSTATION_LIST.id = STATE__ENABLED.ws_id and STATE__ENABLED.param_name = 'enabled')\n" +
            "  left join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE STATE__MUTE_AGE on (WORKSTATION_LIST.id = STATE__MUTE_AGE.ws_id and STATE__MUTE_AGE.param_name = 'mute_age')\n" +
            "where\n" +
            "  1=1\n";

    private String sqlEnabledIs1 = sqlWsList +
            "  and STATE__ENABLED.param_value = 1";

    private String sqlEnabledIs1MuteIs1 = sqlWsList +
            "  and STATE__ENABLED.param_value = 1\n" +
            "  and STATE__MUTE_AGE.param_value <> 0";

    @Test
    public void test_restoreWorkstation_ws3_jsonCfg() throws Exception {
        JdxReplSrv srv = new JdxReplSrv(db);
        srv.init();

        // Узнаем правила для формирования snapshot
        IPublicationRuleStorage ruleSnapshot = srv.getCfgSnapshot(cfg_json_snapshot);

        //
        srv.restoreWorkstation(3, ruleSnapshot);
    }


}
