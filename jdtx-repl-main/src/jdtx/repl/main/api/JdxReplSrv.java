package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.filter.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;


/**
 * Контекст сервера
 */
public class JdxReplSrv {

    // Общая очередь на сервере
    IJdxQue queCommon;

    // Почтовые ящики для чтения/отправки сообщений (для каждой рабочей станции)
    Map<Long, IMailer> mailerList;

    // Правила публикации (для каждой рабочей станции)
    Map<Long, IPublicationRuleStorage> publicationsInList;

    //
    Db db;
    private IJdxDbStruct struct;

    //
    private String dataRoot;

    //
    protected static Log log = LogFactory.getLog("jdtx.Server");


    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;

        // Общая очередь на сервере
        queCommon = new JdxQueCommon(db, UtQue.QUE_COMMON, UtQue.STATE_AT_SRV);

        // Почтовые курьеры для чтения/отправки сообщений (для каждой рабочей станции)
        mailerList = new HashMap<>();

        // Правила публикации (для каждой рабочей станции)
        publicationsInList = new HashMap<>();
    }

    public IMailer getSelfMailer() {
        // Ошибки сервера кладем в ящик рабочей станции №1
        long wsId = 1;
        return mailerList.get(wsId);
    }

    /**
     * Сервер, запуск
     */
    public void init() throws Exception {
        MDC.put("serviceName", "srv");

        //
        dataRoot = new File(db.getApp().getRt().getChild("app").getValueString("dataRoot")).getCanonicalPath();
        dataRoot = UtFile.unnormPath(dataRoot) + "/";
        log.info("dataRoot: " + dataRoot);

        // Проверка версии служебных структур в БД
        UtDbObjectManager ut = new UtDbObjectManager(db);
        ut.checkReplVerDb();

        // Проверка, что инициализация станции прошла
        ut.checkReplDb();

        // Чтение своей конфигурации
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgWs = cfgManager.getSelfCfg(CfgType.WS);
        JSONObject cfgPublications = cfgManager.getSelfCfg(CfgType.PUBLICATIONS);


        // Чтение структуры БД
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct structActual = reader.readDbStruct();


        // Общая очередь
        String queCommon_DirLocal = dataRoot + "srv/que_common/";
        queCommon.setDataRoot(queCommon_DirLocal);


        // Почтовые курьеры, отдельные для каждой станции
        DataStore wsSt = loadWsList(0);
        for (DataRecord wsRec : wsSt) {
            long wsId = wsRec.getValueLong("id");

            // Рабочие каталоги мейлера
            String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
            String mailLocalDirTmp = dataRoot + "srv/ws_" + sWsId + "_tmp/";

            // Конфиг для мейлера
            JSONObject cfgMailer = new JSONObject();
            String guid = wsRec.getValueString("guid");
            String url = (String) cfgWs.get("url");
            cfgMailer.put("guid", guid);
            cfgMailer.put("url", url);
            cfgMailer.put("localDirTmp", mailLocalDirTmp);

            // Мейлер
            IMailer mailer = new MailerHttp();
            mailer.init(cfgMailer);
            //
            mailerList.put(wsId, mailer);


            // Правила входящих реплик для рабочей станции ("in", используем при подготовке реплик)
            JSONObject cfgPublicationsWs = CfgManager.getCfgFromDataRecord(wsRec, CfgType.PUBLICATIONS);
            IPublicationRuleStorage publicationRuleWsIn = PublicationRuleStorage.loadRules(cfgPublicationsWs, structActual, "in");
            publicationsInList.put(wsId, publicationRuleWsIn);
        }


        // Фильтрация структуры: убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut

        // Правила публикаций
        IPublicationRuleStorage publicationIn = PublicationRuleStorage.loadRules(cfgPublications, structActual, "in");
        IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfgPublications, structActual, "out");

        // Фильтрация структуры
        struct = UtRepl.getStructCommon(structActual, publicationIn, publicationOut);

        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
    }

    /**
     * Сервер, инициализация окружения
     */
    public void initFirst() {
        UtFile.mkdirs(queCommon.getBaseDir());
    }


    // Проверка версии приложения, ошибка при несовпадении
    void checkAppUpdate() throws Exception {
        String appRoot = new File(db.getApp().getRt().getChild("app").getValueString("appRoot")).getCanonicalPath();
        UtAppUpdate ut = new UtAppUpdate(db, appRoot);
        // Рабочая станция вседа обновляет приложение,
        // сервер - просто ждет пока приложение обновится.
        // Это разделение для того, чтобы на серверной базе
        // сервер и рабчая станция одновременно не кинулись обновлять.
        ut.checkAppUpdate(false);
    }

    public void addWorkstation(long wsId, String wsName, String wsGuid, String cfgPublicationsFileName, String cfgDecodeFileName) throws Exception {
        log.info("add workstation, wsId: " + wsId + ", name: " + wsName);

        // ---
        // Создадим станцию

        // SRV_WORKSTATION_LIST
        Map params = UtCnv.toMap(
                "id", wsId,
                "name", wsName,
                "guid", wsGuid
        );
        String sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST (id, name, guid) values (:id, :name, :guid)";
        db.execSql(sql, params);

        // SRV_WORKSTATION_STATE
        JdxDbUtils dbu = new JdxDbUtils(db, null);
        long id = dbu.getNextGenerator(UtJdx.SYS_GEN_PREFIX + "SRV_WORKSTATION_STATE");
        sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE (id, ws_id, que_common_dispatch_done, que_in_no_done, enabled, mute_age) values (" + id + ", " + wsId + ", 0, 0, 0, 0)";
        db.execSql(sql);


        // ---
        // Инициализационная очередь queOut001
        JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
        queOut001.setDataRoot(dataRoot);

        // Очереди и правила их нумерации, в частности out001
        // ---
        // Отправим системные команды для станции в ее очередь queOut001
        JSONObject cfgPublicationsWs = UtRepl.loadAndValidateJsonFile(cfgPublicationsFileName);
        srvSendCfgInternal(queOut001, cfgPublicationsWs, CfgType.PUBLICATIONS, wsId);
        //
        JSONObject cfgDecode = UtRepl.loadAndValidateJsonFile(cfgDecodeFileName);
        srvSendCfgInternal(queOut001, cfgDecode, CfgType.DECODE, wsId);
        //
        srvDbStructFinishInternal(queOut001);


        // ---
        // Подготовим Snapshot для станции

        // Запоминаем возраст входящей очереди.
        JdxStateManagerWs stateManagerWs = new JdxStateManagerWs(db);
        long queInNoDone = stateManagerWs.getQueNoDone("in");

        // Единственное место, где на сервере без экземпляра СЕРВЕРНОЙ рабочей станции - не обойтись
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        List<IReplica> snapshotReplicas;

        //
        db.startTran();
        try {
            // В publicationOutTables будет соблюден порядок сортировки таблиц с учетом foreign key
            // (при применении snapsot важен порядок)
            List<IJdxTable> publicationOutTables = makePublicationTables(ws.struct, ws.publicationOut);

            // Создаем реплики для snapshot (пока без фильтрации)
            snapshotReplicas = ws.SnapshotForTables(publicationOutTables, 0, false);

            //
            db.commit();

        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }


        // ---
        // Возраст snapshot рабочей станции.
        long wsSnapshotAge = queInNoDone;


        // ---
        // Обрабатываем snapshot-реплики

        // Преобразователь по фильтрам
        IReplicaFilter filter = new ReplicaFilter();

        // Правила публикаций (фильтры) для wsId.
        // В качестве фильтров на ОТПРАВКУ snapshot от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
        // Не берем сейчас publicationsInList.get(wsId), т.к. новой станции еще в этом списке нет
        IPublicationRuleStorage publicationRuleWsIn = PublicationRuleStorage.loadRules(cfgPublicationsWs, struct, "in");
        publicationsInList.put(wsId, publicationRuleWsIn);


        // Параметры (для правил публикации): получатель реплики
        filter.getFilterParams().put("wsDestination", String.valueOf(wsId));
        // Параметры (для правил публикации): автор реплики - делаем заведомо не существующее значение
        filter.getFilterParams().put("wsAuthor", String.valueOf(-1));

        // Помещаем snapshot-реплики в очередь queOut001
        for (IReplica replica : snapshotReplicas) {
            // Преобразовываем по правилам публикаций (фильтрам)
            IReplica replicaForWs = filter.convertReplicaForWs(replica, publicationRuleWsIn);

            // В очередь queOut001
            queOut001.push(replicaForWs);
        }


        // ---
        // Сообщим рабочей станции ее начальный возраст ВХОДЯЩЕЙ очереди
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaSetQueInNo(wsId, wsSnapshotAge);
        queOut001.push(replica);

        // Отмечаем возраст snapshot рабочей станции.
        JdxStateManagerSrv stateManagerSrv = new JdxStateManagerSrv(db);
        //stateManagerSrv.setSnapshotAge(wsId, wsSnapshotAge);

        // Инициализируем возраст отправленных реплик для рабочей станции.
        // Для рабочей станции ее ВХОДЯЩАЯ очередь - это отражение ИСХОДЯЩЕЙ очереди сервера.
        // Если мы начинаем готовить Snapshot в возрасте queInNoDone, то все реплики ПОСЛЕ этого возраста
        // рабочая станция должна будет получить самостоятельно через ее очередь queIn.
        // Поэтому можно взять у "серверной" рабочей станции номер обработанной ВХОДЯЩЕЙ очереди,
        // но пометить НА СЕРВЕРЕ этим возрастом номер ОТПРАВЛЕННЫХ реплик для этой станции.
        stateManagerSrv.setDispatchDoneQueCommon(wsId, wsSnapshotAge);

        // Инициализируем нумерацию реплик в очереди queOut000 этой станции.
        // Для красивой нумерации в queOut000.
        JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
        queOut000.setMaxNo(wsSnapshotAge);

        // Инициализируем нумерацию отправки реплик из очереди queOut000 на этоу станцию.
        IJdxMailStateManager stateManagerMail = new JdxMailStateManagerSrv(db, wsId, UtQue.QUE_OUT000);
        stateManagerMail.setMailSendDone(wsSnapshotAge);
    }

    private List<IJdxTable> makePublicationTables(IJdxDbStruct struct, IPublicationRuleStorage publicationStorage) {
        List<IJdxTable> publicationTables = new ArrayList<>();
        for (IJdxTable table : struct.getTables()) {
            if (publicationStorage.getPublicationRule(table.getName()) != null) {
                publicationTables.add(table);
            }
        }
        return publicationTables;
    }

    public void enableWorkstation(long wsId) throws Exception {
        log.info("enable workstation, wsId: " + wsId);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set enabled = 1 where id = " + wsId;
        db.execSql(sql);
    }

    public void disableWorkstation(long wsId) throws Exception {
        log.info("disable workstation, wsId: " + wsId);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set enabled = 0 where id = " + wsId;
        db.execSql(sql);
    }

    /**
     * Сервер: формирование общей очереди.
     * Считывание очередей рабочих станций и формирование общей очереди.
     */
    public void srvHandleCommonQue() throws Exception {
        srvHandleCommonQueInternal(mailerList, queCommon);
    }

    /**
     * Сервер: тиражирование реплик из common.
     * Распределение общей очереди по очередям рабочих станций: common -> out000
     */
    public void srvReplicasDispatch() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // Все что у нас есть на раздачу
        long commonQueMaxNo = queCommon.getMaxNo();

        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();

            //
            log.info("srvReplicasDispatch (common -> out000), to.wsId: " + wsId);

            // Исходящая очередь для станции wsId
            JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
            queOut000.setDataRoot(dataRoot);

            // Преобразователь по фильтрам
            IReplicaFilter filter = new ReplicaFilter();

            // Правила публикаций (фильтры) для wsId.
            // В качестве фильтров на ОТПРАВКУ от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
            IPublicationRuleStorage publicationRule = publicationsInList.get(wsId);

            // Параметры (для правил публикации): получатель реплики (для правил публикации)
            filter.getFilterParams().put("wsDestination", String.valueOf(wsId));

            //
            long sendFrom = stateManager.getDispatchDoneQueCommon(wsId) + 1;
            long sendTo = commonQueMaxNo;

            long count = 0;
            for (long no = sendFrom; no <= sendTo; no++) {
                // Берем реплику из queCommon
                IReplica replica = queCommon.get(no);

                // Читаем заголовок
                JdxReplicaReaderXml.readReplicaInfo(replica);

                // Параметры (для правил публикации): автор реплики
                filter.getFilterParams().put("wsAuthor", String.valueOf(replica.getInfo().getWsId()));

                // Преобразовываем по правилам публикаций (фильтрам)
                IReplica replicaForWs = filter.convertReplicaForWs(replica, publicationRule);

                //
                db.startTran();
                try {
                    // Положим реплику в очередь (физически переместим)
                    queOut000.push(replicaForWs);

                    // Отметим распределение очередного номера реплики.
                    stateManager.setDispatchDoneQueCommon(wsId, no);

                    //
                    db.commit();
                } catch (Exception e) {
                    db.rollback(e);
                    throw e;
                }

                //
                count++;
            }

            //
            if (sendFrom < sendTo) {
                log.info("srvReplicasDispatch (common -> out000) done, wsId: " + wsId + ", out001.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
            } else {
                log.info("srvReplicasDispatch (common -> out000) done, wsId: " + wsId + ", out001.no: " + sendFrom + ", nothing done");
            }

        }
    }


    /**
     * Рассылка реплик в ящики каждой рабочей станции
     */
    public void srvReplicasSendMail() throws Exception {
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Рассылаем
            try {
                // Рассылаем очередь out000 (продукт обработки очереди common -> out000) на каждую станцию
                IJdxMailStateManager stateManagerMail = new JdxMailStateManagerSrv(db, wsId, UtQue.QUE_OUT000);
                JdxQueOut001 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);
                UtMail.sendQueToMail(wsId, queOut000, mailer, "to", stateManagerMail);

                // Рассылаем queOut001 на каждую станцию
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);
                //
                stateManagerMail = new JdxMailStateManagerSrv(db, wsId, UtQue.QUE_OUT001);
                UtMail.sendQueToMail(wsId, queOut001, mailer, "to001", stateManagerMail);

                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                // todo: не переложить отметку ли в sendQueToMail?
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in SrvDispatchReplicas, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
    }


    /**
     * @deprecated Разобраться с репликацией через папку - сейчас полностью сломано
     */
    @Deprecated
    public void srvHandleCommonQueFrom(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, 0);

        // Физически забираем данные
        srvHandleCommonQueInternal(mailerListLocal, queCommon);
    }

    /**
     * @deprecated Разобраться с репликацией через папку - сейчас полностью сломано
     */
    @Deprecated
    public void srvDispatchReplicasToDir(String cfgFileName, String mailDir, SendRequiredInfo requiredInfo, long destinationWsId, boolean doMarkDone) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, destinationWsId);

        // Физически отправляем данные
        srvDispatchReplicas(queCommon, mailerListLocal, requiredInfo, doMarkDone);
    }


    public void srvSetWsMute(long destinationWsId) throws Exception {
        log.info("srvSetWs MUTE, destination.WsId: " + destinationWsId);

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(destinationWsId);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvSetWsUnmute(long destinationWsId) throws Exception {
        log.info("srvSetWs UNMUTE, destination.WsId: " + destinationWsId);

        // Системная команда "UNMUTE" ...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaUnmute(destinationWsId);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvAppUpdate(String exeFileName) throws Exception {
        log.info("srvAppUpdate, exeFileName: " + exeFileName);

        UtRepl utRepl = new UtRepl(db, struct);

        // Рассылка - в исходящую очередь реплик queOut001
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();

            // Инициализационная очередь queOut001
            JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
            queOut001.setDataRoot(dataRoot);

            // Команда на обновление
            IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

            // Системная команда - в исходящую очередь реплик
            queOut001.push(replica);

            //
            log.info("srvAppUpdate, to wd: " + wsId);
        }
    }

    public void srvRequestSnapshot(long wsId, String tableNames) throws Exception {
        log.info("srvRequestSnapshot, wsId: " + wsId + ", tables: " + tableNames);

        // Разложим в список
        String[] tableNamesArr = tableNames.split(",");
        List<IJdxTable> tableList = new ArrayList<>();
        for (String tableName : tableNamesArr) {
            IJdxTable table = struct.getTable(tableName);
            tableList.add(table);
        }

        // Сортируем список, чтобы несколько snapsot-реплик не сломали ссылки
        List<IJdxTable> tableListSorted = UtJdx.sortTablesByReference(tableList);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        for (IJdxTable table : tableListSorted) {
            log.info("srvRequestSnapshot, table: " + table.getName());

            // Реплика-запрос snapshot
            IReplica replica = utRepl.createReplicaWsSendSnapshot(wsId, table.getName());

            // Реплика-запрос snapshot - в исходящую очередь реплик
            queCommon.push(replica);
        }

    }


    public void srvDbStructStart() throws Exception {
        log.info("srvDbStructStart");

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(0);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvDbStructFinish() throws Exception {
        log.info("srvDbStructFinish");

        // Системные команды в общую исходящую очередь реплик
        srvDbStructFinishInternal(queCommon);
    }


    /**
     * Системные команды в очередь que
     */
    private void srvDbStructFinishInternal(IJdxReplicaQue que) throws Exception {
        IReplica replica;
        UtRepl utRepl = new UtRepl(db, struct);


        // Системная команда "SET_DB_STRUCT"...
        replica = utRepl.createReplicaSetDbStruct();
        // ...в очередь
        que.push(replica);


        // Системная команда "UNMUTE" ...
        replica = utRepl.createReplicaUnmute(0);
        // ...в очередь
        que.push(replica);
    }


    public void srvSendCfg(String cfgFileName, String cfgType, long destinationWsId) throws Exception {
        log.info("srvSendCfg, cfgFileName: " + new File(cfgFileName).getAbsolutePath() + ", cfgType: " + cfgType + ", destination wsId: " + destinationWsId);

        //
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfgFileName);
        srvSendCfgInternal(queCommon, cfg, cfgType, destinationWsId);
    }

    private void srvSendCfgInternal(IJdxReplicaQue que, JSONObject cfg, String cfgType, long destinationWsId) throws Exception {
        //
        db.startTran();
        try {
            // Обновляем конфиг в таблицах для рабочих станций (SRV_WORKSTATION_LIST)
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setWsCfg(cfg, cfgType, destinationWsId);

            // Системная команда ...
            UtRepl utRepl = new UtRepl(db, struct);
            IReplica replica = utRepl.createReplicaSetCfg(cfg, cfgType, destinationWsId);

            // ... в исходящую очередь реплик
            que.push(replica);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            //
            e.printStackTrace();
            //
            throw e;
        }
    }


    /**
     * Сервер: считывание очередей рабочих станций и формирование общей очереди
     * <p>
     * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
     * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
     */
    private void srvHandleCommonQueInternal(Map<Long, IMailer> mailerList, IJdxReplicaQue commonQue) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Обрабатываем каждую станцию
            try {
                log.info("srvHandleCommonQue, from.wsId: " + wsId);

                //
                long queDoneAge = stateManager.getWsQueInAgeDone(wsId);
                long queMaxAge = mailer.getBoxState("from");

                //
                long count = 0;
                for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                    log.info("receive, wsId: " + wsId + ", receiving.age: " + age);

                    // Информацмия о реплике с почтового сервера
                    IReplicaFileInfo info = mailer.getReplicaInfo("from", age);

                    // Физически забираем данные с почтового сервера
                    IReplica replica = mailer.receive("from", age);

                    // Проверяем целостность скачанного
                    UtJdx.checkReplicaCrc(replica, info);

                    // Читаем заголовок
                    JdxReplicaReaderXml.readReplicaInfo(replica);

                    // Помещаем полученные данные в общую очередь
                    db.startTran();
                    try {
                        // Помещаем в очередь
                        long commonQueAge = commonQue.push(replica);

                        // Отмечаем факт скачивания
                        stateManager.setWsQueInAgeDone(wsId, age);

                        // todo: Почему для сервера - сразу ТУТ реагируем, а для станции - потом. И почему ТУТ не проверяется адресат????
                        // Реагируем на системные реплики-сообщения
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.MUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setMuteDone(wsId, commonQueAge);
                        }
                        //
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.UNMUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setUnmuteDone(wsId);
                        }

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback();
                        throw e;
                    }

                    // Удаляем с почтового сервера
                    mailer.delete("from", age);

                    //
                    count++;
                }


                // Отметить попытку чтения (для отслеживания активности станции, когда нет данных для реальной передачи)
                mailer.setData(null, "ping.read", "from");
                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);


                //
                if (queDoneAge <= queMaxAge) {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + " .. " + queMaxAge + ", done count: " + count);
                } else {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in srvHandleCommonQue, from.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }
        }
    }

    /**
     * Сервер: распределение очереди по рабочим станциям
     *
     * @deprecated Нужно только для репликации через папку - сейчас полностью сломано
     */
    @Deprecated
    private void srvDispatchReplicas(IJdxQue que, Map<Long, IMailer> mailerList, SendRequiredInfo requiredInfo, boolean doMarkDone) throws Exception {
/*
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // Все что у нас есть на раздачу
        long commonQueMaxNo = que.getMaxNo();

        //
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Рассылаем на каждую станцию
            try {
                log.info("SrvDispatchReplicas, to.wsId: " + wsId);

                //
                long sendFrom;
                long sendTo;
                long count;

                // ---
                // queOut001 - очередь Que001

                // Сначала проверим, надо ли отправить queOut001
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);

                //
                sendFrom = stateManager.getDispatchDoneQueOut001(wsId) + 1;
                sendTo = queOut001.getMaxNoFromDir(); // todo: очень некрасиво - путаюися "физический" (getMaxNo) и "логический" (getMaxNoFromDir) возраст

                // Берем реплику - snapshot
                count = 0;
                for (long no = sendFrom; no <= sendTo; no++) {
                    IReplica replica001 = queOut001.get(no);

                    // Физически отправим реплику - snapshot
                    mailer.send(replica001, "to001", no);

                    // Отметим отправку
                    if (doMarkDone) {
                        // Отметим отправку очередного номера реплики.
                        stateManager.setDispatchDoneQueOut001(wsId, no);
                    }

                    //
                    count = count + 1;
                }

                //
                if (sendFrom < sendTo) {
                    log.info("Que001 DispatchReplicas done, wsId: " + wsId + ", queOut001.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
                } else {
                    log.info("Que001 DispatchReplicas done, wsId: " + wsId + ", queOut001.no: " + sendFrom + ", nothing done");
                }


                // ---
                // queCommon - общая очередь

                // Выясняем объем передачи
                // Если никто не просит - узнаем сколько просит станция
                SendRequiredInfo requiredInfoBox;
                if (requiredInfo == null) {
                    requiredInfoBox = mailer.getSendRequired("to");
                } else {
                    requiredInfoBox = requiredInfo;
                }

                //
                if (requiredInfoBox.requiredFrom != -1) {
                    // Попросили повторную отправку
                    log.warn("Repeat send required, from: " + requiredInfoBox.requiredFrom + ", to: " + requiredInfoBox.requiredTo + ", recreate: " + requiredInfoBox.recreate);
                    sendFrom = requiredInfoBox.requiredFrom;
                    sendTo = requiredInfoBox.requiredTo;
                } else {
                    // Не просили - зададим сами (от последней отправленной до послейдней, что у нас есть на раздачу)
                    sendFrom = stateManager.getDispatchDoneQueCommon(wsId) + 1;
                    sendTo = commonQueMaxNo;
                }

                //
                count = 0;
                for (long no = sendFrom; no <= sendTo; no++) {
                    // Берем реплику
                    IReplica replica = que.get(no);

                    //
                    //log.debug("replica.age: " + replica.getInfo().getAge() + ", replica.wsId: " + replica.getInfo().getWsId());

                    // Физически отправим реплику
                    mailer.send(replica, "to", no); // todo это тупо - вот так копировать и перекладывать файлы из папки в папку???

                    // Отметим отправку очередного номера реплики.
                    if (doMarkDone) {
                        stateManager.setMailSendDone(wsId, no);
                        stateManager.setDispatchDoneQueCommon(wsId, no);
                    }

                    //
                    count++;
                }


                // Отметить попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
                mailer.setData(null, "ping.write", "to");
                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);


                // Снимем флаг просьбы сервера
                if (requiredInfoBox.requiredFrom != -1) {
                    mailer.setSendRequired("to", new SendRequiredInfo());
                    log.warn("Repeat send done");
                }

                //
                if (sendFrom < sendTo) {
                    log.info("QueCommon DispatchReplicas, to.wsId: " + wsId + ", queCommon.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
                } else {
                    log.info("QueCommon DispatchReplicas, to.wsId: " + wsId + ", queCommon.no: " + sendFrom + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in SrvDispatchReplicas, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
*/
    }

    private Map getInfoSrv() {
        return null;
    }


    /**
     * Готовим спосок локальных (через папку) мейлеров, отдельные для каждой станции
     */
    private void fillMailerListLocal(Map<Long, IMailer> mailerListLocal, String cfgFileName, String mailDir, long destinationWsId) throws Exception {
        // Готовим курьеров
        mailDir = UtFile.unnormPath(mailDir) + "/";

        //
        JSONObject cfgData = UtRepl.loadAndValidateJsonFile(cfgFileName);

        // Список активных рабочих станций
        DataStore st = loadWsList(destinationWsId);

        //
        for (DataRecord rec : st) {
            long wdId = rec.getValueLong("id");
            String guid = rec.getValueString("guid");
            String guidPath = guid.replace("-", "/");

            // Конфиг для мейлера
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            cfgWs.put("mailRemoteDir", mailDir + guidPath);

            // Мейлер
            IMailer mailerLocal = new MailerLocalFiles();
            mailerLocal.init(cfgWs);

            //
            mailerListLocal.put(wdId, mailerLocal);
        }
    }

    /**
     * Список активных рабочих станций (или одна конкретная)
     */
    private DataStore loadWsList(long wsId) throws Exception {
        String sql;
        if (wsId != 0) {
            // Указана конкретная станция-получатель - выгружаем только ее, остальные пропускаем
            sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST where id = " + wsId;
        } else {
            // Берем только активные
            sql = "select " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST.* " +
                    "from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST " +
                    "join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE on " +
                    "(" + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST.id = " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE.ws_id) " +
                    "where " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE.enabled = 1";
        }

        //
        DataStore st = db.loadSql(sql);

        //
        return st;
    }


}


