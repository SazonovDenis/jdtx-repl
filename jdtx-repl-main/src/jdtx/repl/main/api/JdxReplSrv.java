package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.cleaner.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.filter.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.rec_merge.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.settings.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.task.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;


/**
 * Контекст сервера
 * todo рассмотреть необходимость добавлять каскадно и ВЛИЯЮШИЕ записл для вставляемой эталонной (и далее - каскадно) - их ТОЖЕ может не оказаться на филиале
 */
public class JdxReplSrv {

    public static long SERVER_WS_ID = 1L;

    // Общая очередь на сервере
    IJdxQueCommon queCommon;

    // Почтовые ящики для чтения/отправки сообщений (для каждой рабочей станции)
    Map<Long, IMailer> mailerList;

    // Правила публикации (для каждой рабочей станции)
    Map<Long, IPublicationRuleStorage> publicationsInList;

    // Входящие очереди-зеркала на сервере (для каждой рабочей станции).
    // Нужны отдельным списком ради передачи в QueCommon.
    Map<Long, IJdxQue> queInList;

    //
    private Db db;

    //
    private IJdxDbStruct struct;

    /**
     * Полная физическая структура БД
     */
    private IJdxDbStruct structFull;

    //
    private String dataRoot;
    private String guid;

    //
    public JdxErrorCollector errorCollector = null;

    public static String[] ws_param_names = {
            "que_common_dispatch_done",
            "que_out000_no",
            "que_out000_send_done",
            "que_out001_no",
            "que_out001_send_done",
            "que_in_no",
            "que_in_no_done",
            "enabled",
            "mute_age"
    };

    //
    protected static Log log = LogFactory.getLog("jdtx.Server");

    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;
    }

    public IMailer getSelfMailer() {
        // Ошибки сервера кладем в ящик рабочей станции №1
        long wsId = SERVER_WS_ID;
        return mailerList.get(wsId);
    }

    public String getDataRoot() {
        return dataRoot;
    }

    public String getSrvGuid() {
        return guid;
    }

    /**
     * Сервер, запуск
     */
    public void init() throws Exception {
        if (MDC.get("serviceName") == null) {
            MDC.put("serviceName", "srv");
        }

        // Проверка версии служебных структур в БД
        IDbObjectManager dbObjectManager = db.service(DbObjectManager.class);
        dbObjectManager.checkVerDb();

        // Проверка, что инициализация станции прошла
        dbObjectManager.checkReplicationInit();

        // В каком каталоге работаем
        initDataRoot();

        // Чтение своей конфигурации
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgWs = cfgManager.getSelfCfg(CfgType.WS);
        JSONObject cfgPublications = cfgManager.getSelfCfg(CfgType.PUBLICATIONS);


        // Чтение структуры БД
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        structFull = reader.readDbStruct();

        // Читаем и проверяем код серверной рабочей станции
        readIdGuid();

        // Правила входящих реплик, почтовые курьеры, входящие очереди-зеркала (отдельные для каждой рабочей станции)
        publicationsInList = new HashMap<>();
        mailerList = new HashMap<>();
        queInList = new HashMap<>();
        //
        DataStore wsSt = loadWsList();
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
            JSONObject cfgPublicationsWs = cfgManager.getWsCfg(CfgType.PUBLICATIONS, wsId);
            IPublicationRuleStorage publicationRuleWsIn = PublicationRuleStorage.loadRules(cfgPublicationsWs, structFull, "in");
            //
            publicationsInList.put(wsId, publicationRuleWsIn);


            // Входящие очереди-зеркала
            JdxQueInSrv queIn = new JdxQueInSrv(db, wsId);
            queIn.setDataRoot(dataRoot);
            //
            queInList.put(wsId, queIn);
        }


        // Общая очередь на сервере
        queCommon = new JdxQueCommon(db, UtQue.SRV_QUE_COMMON, UtQue.STATE_AT_SRV);
        queCommon.setDataRoot(dataRoot);
        queCommon.setSrvQueIn(queInList);


        // Фильтрация структуры: убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut
        struct = UtRepl.filterStruct(structFull, cfgPublications);


        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
    }

    /**
     * В каком каталоге работаем.
     * Оформлен как отдельный метод, чтобы можно было вызывать только его (в тестах и т.д.)
     */
    void initDataRoot() throws IOException {
        dataRoot = new File(db.getApp().getRt().getChild("app").getValueString("dataRoot")).getCanonicalPath();
        dataRoot = UtFile.unnormPath(dataRoot) + "/";
        log.info("dataRoot: " + dataRoot);
    }

    /**
     * Читаем и проверяем код и guid серверной станции.
     */
    void readIdGuid() throws Exception {
        IWsSettings wsSettings = db.getApp().service(WsSettingsService.class);
        long selfWsId = wsSettings.getWsId();
        if (selfWsId != SERVER_WS_ID) {
            throw new XError("Invalid server ws_id: " + selfWsId);
        }
        this.guid = wsSettings.getWsGuid().split("-")[0];
    }

    /**
     * Проверка версии приложения, ошибка при несовпадении.
     * <p>
     * Рабочая станция вседа обновляет приложение, а сервер - просто ждет пока приложение обновится.
     * Это разделение для того, чтобы на серверной базе
     * сервер и рабчая станция одновременно не кинулись обновлять.
     */
    public void checkAppUpdate() throws Exception {
        String appRoot = new File(db.getApp().getRt().getChild("app").getValueString("appRoot")).getCanonicalPath();
        UtAppUpdate ut = new UtAppUpdate(db, appRoot);
        ut.checkAppUpdate(false);
    }

    /**
     * Сервер, задачи по уходу за сервером.
     * Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
     * для очередей, задействованных в задаче чтения со станций.
     */
    public void srvHandleRoutineTaskIn() throws Exception {
        DataStore wsSt = loadWsList();
        Set wsList = UtData.uniqueValues(wsSt, "id");

        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // очередь queInSrv для станции wsId (входящие очереди-зеркала)
        for (Object wsIdObj : wsList) {
            long wsId = UtJdxData.longValueOf(wsIdObj);
            IJdxQue que = queInList.get(wsId);
            UtRepl.clearTrashFiles(que);
        }
    }

    /**
     * Сервер, задачи по уходу за сервером.
     * Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
     * удаление старых реплик в почтовых ящиках, задействованных в задаче чтения со станций.
     */
    public void srvCleanupMailInBox() throws Exception {
        DataStore wsSt = loadWsList();
        Set wsList = UtData.uniqueValues(wsSt, "id");

        //
        for (Object wsIdObj : wsList) {
            long wsId = UtJdxData.longValueOf(wsIdObj);
            //
            IMailer mailer = mailerList.get(wsId);
            IJdxQue que = queInList.get(wsId);
            //
            String box = "from";
            long no = que.getMaxNo();
            long deleted = mailer.deleteAll(box, no);
            if (deleted != 0) {
                log.info("mailer.deleted, no: " + no + ", box: " + box + " deleted: " + deleted + ", wsId: " + wsId);
            }
        }
    }

    /**
     * Сервер, задачи по уходу за сервером.
     * Анализ, какие реплики больше не нужны на станциях.
     */
    public Map<Long, JdxQueCleanTask> srvCleanupReplPrepareTask(long argQueInUsedLast) throws Exception {
        JdxCleaner cleanerSrv = new JdxCleaner(db);

        // Узнаем, что использовано на всех станциях.
        // Важно учесть, что если для станции не удастся прочитать достоверной информации о состоянии,
        // то удалять реплики будет опасно - вдруг рано? Вдруг кто-то еще не применил?
        // Чтобы обеспечить чтение из ВСЕХ активных станций, чтение состояния итераций идет по mailerList,
        // при этом ошибки НЕ маскируются и НЕ пропускаются.
        Map<Long, JdxQueUsedState> usedStates = new HashMap<>();
        for (long wsId : mailerList.keySet()) {
            IMailer mailer = mailerList.get(wsId);
            JdxQueUsedState usedState = cleanerSrv.readQueUsedStatus(mailer);
            usedStates.put(wsId, usedState);
        }
        // Печатаем
        log.info("Que used status:");
        for (long wsId : usedStates.keySet()) {
            JdxQueUsedState usedState = usedStates.get(wsId);
            log.info("ws: " + wsId + ", usedState: " + usedState);
        }


        // Определим худший возраст использования серверной общей очереди queCommon
        // среди всех рабочих станций (на самой отстающей станции он будет меньше всех).
        // Все реплики, что меньше этого возраста, больше никому не нужны - их можно удалить на всех станциях.
        long queInUsedMin = Long.MAX_VALUE;
        for (long wsId : mailerList.keySet()) {
            JdxQueUsedState usedState = usedStates.get(wsId);
            long queInUsed = usedState.queInUsed;
            if (queInUsed < queInUsedMin) {
                queInUsedMin = queInUsed;
            }
        }
        log.info("min ws queIn.used: " + queInUsedMin);
        if (argQueInUsedLast < queInUsedMin) {
            queInUsedMin = argQueInUsedLast;
            log.info("set min queIn.used: " + queInUsedMin);
        }

        // По номеру реплики из серверной ОБЩЕЙ очереди, которую приняли и использовали все рабочие станции,
        // для каждой рабочей станции узнаем, какой номер ИСХОДЯЩЕЙ очереди рабочей станции
        // уже принят и использован всеми другими станциями.
        Map<Long, Long> allQueOutNo = cleanerSrv.get_WsQueOutNo_by_queCommonNo(queInUsedMin);
        // Печатаем
        log.info("Ws queOut no:");
        for (long wsId : allQueOutNo.keySet()) {
            long wsQueOutNo = allQueOutNo.get(wsId);
            log.info("ws: " + wsId + ", ws.queOut.no: " + wsQueOutNo);
        }


        // Формируем команды на очистку
        Map<Long, JdxQueCleanTask> res = new HashMap<>();
        for (long wsId : mailerList.keySet()) {
            JdxQueCleanTask cleanTask = new JdxQueCleanTask();

            cleanTask.queOutNo = allQueOutNo.get(wsId);
            cleanTask.queInNo = queInUsedMin;
            cleanTask.queIn001No = usedStates.get(wsId).queIn001Used;

            res.put(wsId, cleanTask);
        }

        //
        return res;
    }

    /**
     * Сервер, задачи по уходу за сервером.
     * Анализ, какие реплики больше не нужны на станциях,
     * отправка команды удаления на рабочие станции.
     */
    public void srvCleanupRepl(long argQueInUsedLast) throws Exception {
        JdxCleaner cleanerSrv = new JdxCleaner(db);

        // Анализ, какие реплики больше не нужны на станциях
        Map<Long, JdxQueCleanTask> cleanupTasks = srvCleanupReplPrepareTask(argQueInUsedLast);
        // Печатаем
        log.info("Cleanup tasks:");
        for (long wsId : cleanupTasks.keySet()) {
            JdxQueCleanTask cleanupTask = cleanupTasks.get(wsId);
            log.info("  ws: " + wsId + ", cleanupTask: " + cleanupTask);
        }

        // Сообщаем на рабочие станции, какие реплики можно удалить
        for (long wsId : cleanupTasks.keySet()) {
            JdxQueCleanTask cleanupTask = cleanupTasks.get(wsId);

            //
            IMailer mailer = mailerList.get(wsId);
            if (mailer != null) {
                log.info("sendQueCleanTask, ws: " + wsId + ", cleanTask: " + cleanupTask);
                cleanerSrv.sendQueCleanTask(mailer, cleanupTask);
            } else {
                log.info("sendQueCleanTask ws: " + wsId + ", skipped");
            }
        }
    }

    /**
     * Сервер, задачи по уходу за сервером.
     * Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
     * для очередей, задействованных в задаче формирования исходящих очередей для станций.
     */
    public void srvHandleRoutineTaskOut() throws Exception {
        DataStore wsSt = loadWsList();
        Set wsList = UtData.uniqueValues(wsSt, "id");

        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // Общая очередь
        UtRepl.clearTrashFiles(queCommon);

        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // очередь Out000 для станции wsId (исходящая из сервера)
        for (Object wsIdObj : wsList) {
            long wsId = UtJdxData.longValueOf(wsIdObj);
            // Исходящая очередь Out000 для станции wsId
            JdxQueOut000 que = new JdxQueOut000(db, wsId);
            que.setDataRoot(dataRoot);

            //
            UtRepl.clearTrashFiles(que);
        }

        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // очередь queOut001 для станции wsId (инициализационная или для системных команд)
        for (Object wsIdObj : wsList) {
            long wsId = UtJdxData.longValueOf(wsIdObj);
            //
            JdxQueOut001 que = new JdxQueOut001(db, wsId);
            que.setDataRoot(dataRoot);

            //
            UtRepl.clearTrashFiles(que);
        }
    }

    /**
     * Сервер, инициализация окружения при создании репликации
     */
    public void firstSetup() {
        //UtFile.mkdirs(queCommon.getBaseDir());
    }


    public void addWorkstation(long wsId, String wsName) throws Exception {
        log.info("add workstation, wsId: " + wsId + ", name: " + wsName);

        String srvGuid = getSrvGuid();
        String wsGuid = UtRepl.getGuidWs(srvGuid, wsId);


        //
        db.startTran();
        try {
            // Добавление станции
            Map params = UtCnv.toMap(
                    "id", wsId,
                    "name", wsName,
                    "guid", wsGuid
            );
            String sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST (id, name, guid) values (:id, :name, :guid)";
            db.execSql(sql, params);

            //
            long queCommonNo = queCommon.getMaxNo();

            //
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setWsStruct(new JdxDbStruct(), wsId);

            // Значения параметров
            String sqlIns = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE (id, ws_id, param_name, param_value) values (:id, :ws_id, :param_name, :param_value)";
            long id = wsId * 1000;
            for (String param_name : JdxReplSrv.ws_param_names) {
                long param_value = 0;
                if (param_name.equalsIgnoreCase("enabled")) {
                    param_value = 1;
                }
                if (param_name.equalsIgnoreCase("mute_age")) {
                    param_value = -1;
                }
                Map values = UtCnv.toMap("id", id, "ws_id", wsId, "param_name", param_name, "param_value", param_value);
                db.execSql(sqlIns, values);
                //
                id = id + 1;
            }


            // Очередь queOut001 станции (инициализационная или для системных команд)
            JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
            queOut001.setDataRoot(dataRoot);


            // ---
            // Реплика на установку возрастов очередей рабочей станции (начальное состояние)
            JdxWsState wsState = new JdxWsState();
            // Возраст очереди "in" новой рабочей станции - по текущему возрасту серверной очереди "common"
            long wsSnapshotAge = queCommonNo;
            wsState.QUE_IN_NO = wsSnapshotAge;
            wsState.QUE_IN_NO_DONE = wsSnapshotAge;
            //
            wsState.QUE_IN001_NO = 0L;
            wsState.QUE_IN001_NO_DONE = 0L;
            // Новая рабочая станция пока не имела аудита, поэтому возраст ее очереди "out" будет 0, ...
            wsState.QUE_OUT_NO = 0L;
            wsState.AUDIT_AGE_DONE = 0L;
            // ... отправка тоже 0 ...
            wsState.MAIL_SEND_DONE = 0L;
            // ... и возраст аудита тоже 0.
            wsState.AGE = 0L;
            // Поехали
            wsState.MUTE = 0L;
            //
            UtRepl utRepl = new UtRepl(db, struct);
            IReplica replicaSetState = utRepl.createReplicaSetWsState(wsId, wsState);
            queOut001.push(replicaSetState);


            // ---
            // Установка (на сервере) разных возрастов для новой рабочей станции (начальное состояние)
            JdxStateManagerSrv stateManagerSrv = new JdxStateManagerSrv(db);

            // Номер отправленных реплик.
            // Более поздние в snapshot НЕ попали и рабочая станция получит их самостоятельно.
            stateManagerSrv.setDispatchDoneQueCommon(wsId, wsSnapshotAge);

            // Номер реплик в очереди queOut000 новой станции будет совпадать с остальными станциями (так удобнее и правильнее).
            JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
            queOut000.setMaxNo(wsSnapshotAge);

            // Нумерация отправки реплик из очереди queOut000 на эту станцию.
            IJdxMailSendStateManager mailStateManager = new JdxMailSendStateManagerSrv(db, wsId, UtQue.SRV_QUE_OUT000);
            mailStateManager.setMailSendDone(wsSnapshotAge);

            // Номер последней реплики ОТ новой рабочей станции
            // Станция пока не имела аудита, поэтому ничего еще не отправила на сервер, поэтому 0.
            stateManagerSrv.setWsQueInNoDone(wsId, 0);


            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }

        //
        log.info("add workstation done, wsId: " + wsId + ", name: " + wsName);
    }

    //
    public IPublicationRuleStorage getCfgSnapshot(String cfgSnapshotFileName) throws Exception {
        JSONObject cfgSnapsot = UtRepl.loadAndValidateJsonFile(cfgSnapshotFileName);
        IPublicationRuleStorage ruleSnapsot = PublicationRuleStorage.loadRules(cfgSnapsot, struct, "snapshot");
        return ruleSnapsot;
    }

    /**
     * При восстановлении станции в снимок для неё нужно отправить не только то, что станция может ПОЛУЧИТЬ по правилам "in"
     * но и то, что она сама отправила (а это может отфильтровываться по правилам in),
     * Например, если станция отправляет заказы только вверх, то при получении снимка она не должна получить
     * ВСЕ заказы без разбора - иначе там будут чужие заказы.
     * С другой стороны, СВОИ заказы (ранее отправленные, до сбоя) станция должна получить.
     * <p>
     * Формируем правила snapshot для станции
     * из правил "in", и комбнации правил "out" с фильтром по автору (условием "RECORD_OWNER_ID == наша станция")
     */
    public IPublicationRuleStorage createCfgSnapshot(long wsId) throws Exception {
        // Правила входящих и исходящих реплик для рабочей станции
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgPublicationsWs = cfgManager.getWsCfg(CfgType.PUBLICATIONS, wsId);
        IPublicationRuleStorage publicationRuleWsIn = PublicationRuleStorage.loadRules(cfgPublicationsWs, structFull, "in");
        IPublicationRuleStorage publicationRuleWsOut = PublicationRuleStorage.loadRules(cfgPublicationsWs, structFull, "out");

        // Берем все правила "in" без изменений
        IPublicationRuleStorage rulesSnapsot = new PublicationRuleStorage();
        rulesSnapsot.getPublicationRules().addAll(publicationRuleWsIn.getPublicationRules());

        // Добавим все правила "out", при этом в каждое правило добавим фильтр по автору
        // todo: да, грязно и зависимость от синтаксиса правил
        String magicFilterRule = "RECORD_OWNER_ID = PARAM_wsDestination";
        for (IPublicationRule ruleOut : publicationRuleWsOut.getPublicationRules()) {
            IPublicationRule ruleSnapsot = rulesSnapsot.getPublicationRule(ruleOut.getTableName());

            // Если для таблицы ruleOut.tableName уже есть привило (из списка "in")
            // сейчас не добавляем и не меняем выражение фильтрации
            if (ruleSnapsot != null) {
                continue;
            }

            // Для таблицы ruleOut.tableName добавим привило, взяв за основу привило из списка "out"
            ruleSnapsot = ruleOut;

            // Добавим в выражение фильтрации привило "RECORD_OWNER_ID == наша станция"
            String filterExpression = ruleSnapsot.getFilterExpression();
            if (filterExpression == null || !filterExpression.contains(magicFilterRule)) {
                if (filterExpression == null) {
                    filterExpression = magicFilterRule;
                } else {
                    filterExpression = "(" + filterExpression + ") || (" + magicFilterRule + ")";
                }
                ruleSnapsot.setFilterExpression(filterExpression);
            }

            //
            rulesSnapsot.getPublicationRules().add(ruleSnapsot);
        }

        //
        return rulesSnapsot;
    }

    /**
     * Восстановление рабочей станции при потере базы.
     * Отправляется snapshot, инициализируются счетчики очередей.
     *
     * @param wsId код ранее существующей рабочей станции
     */
    public void restoreWorkstation(long wsId, IPublicationRuleStorage rulesForSnapsot) throws Exception {
        log.info("Restore workstation, wsId: " + wsId);

        // ---
        // Читаем текущие (последние) конфиги станции
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgDecode = cfgManager.getWsCfg(CfgType.DECODE, wsId);
        JSONObject cfgPublications = cfgManager.getWsCfg(CfgType.PUBLICATIONS, wsId);

        // Очередь queOut001 для станции (инициализационная или для системных команд)
        JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
        queOut001.setDataRoot(dataRoot);

        //
        UtRepl utRepl = new UtRepl(db, struct);


        // ---
        // Узнаем у сервера текущее (последнее) состояние рабочей станции

        // Очередь queIn001 станции (равна тому, что мы успели отправить на станцию в прошлой жизни)
        long wsQueIn001 = queOut001.getMaxNo();

        // Очередь queOut станции (равна тому, что станция успела нам отправить в прошлой жизни)
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        long wsQueOutNo = stateManager.getWsQueInNoDone(wsId);

        // Запоминаем возраст входящей очереди "серверной" рабочей станции (que_in_no_done).
        // Если мы начинаем готовить snapshot из "серверной" рабочей станции в этом возрасте,
        // то все реплики ДО этого возраста будут отражены в snapshot,
        // а все реплики ПОСЛЕ этого возраста в snapshot НЕ попадут,
        // и рабочая станция должна будет получить их самостоятельно, через свою очередь queIn.
        // Поэтому можно взять у "серверной" рабочей станции номер обработанной входящей очереди
        // и считать его возрастом обработанной входящей очереди на новой рабочей станции.
        JdxStateManagerWs stateManagerWs = new JdxStateManagerWs(db);
        long wsSnapshotAge = stateManagerWs.getQueNoDone(UtQue.QUE_IN);


        // ---
        // Подготовим snapshot для станции wsId

        // Передаем тот состав таблиц, который перечислен в правилах rulesForSnapsot
        List<IJdxTable> publicationOutTables = makeOrderedFromPublicationRules(struct, rulesForSnapsot);

        // Подготовим snapshot для станции wsId, фильтруем его по правилам rulesForSnapsot
        List<IReplica> replicasSnapshot = utRepl.createSnapshotForTablesFiltered(publicationOutTables, SERVER_WS_ID, wsId, rulesForSnapsot);


        // ---
        // Отправляем конфиги на станцию (в очередь queOut001)
        IReplica replica = utRepl.createReplicaSetCfg(cfgDecode, CfgType.DECODE, wsId);
        queOut001.push(replica);

        // Отправляем команду "задать структуру БД" (в очередь queOut001)
        // Снимок ОТ СТАНЦИИ не просим - её данные на сервере и так есть
        replica = utRepl.createReplicaSetDbStruct(wsId, cfgPublications, false);
        queOut001.push(replica);


        // ---
        // Отправляем snapshot для станции (в очередь queOut001)
        utRepl.sendToQue(replicasSnapshot, queOut001);


        // ---
        // Отправим команду "починить генераторы".
        // После применения snaphot генераторы восстановленной рабочей станции будут находятся в устаревшем сосоянии.
        IReplica replicaRepairGenerators = utRepl.createReplicaRepairGenerators(wsId);
        queOut001.push(replicaRepairGenerators);


        // ---
        // Установим рабочей станции её начальное состояние (возрасты очередей рабочей станции)

        // Реплика на установку возрастов очередей рабочей станции (начальное состояние)
        JdxWsState wsState = new JdxWsState();
        // Возраст очереди "in" новой рабочей станции - по возрасту очереди "in"
        // "серверной" рабочей станции (que_in_no_done), запомненному при формировании snapshot
        wsState.QUE_IN_NO = wsSnapshotAge;
        wsState.QUE_IN_NO_DONE = wsSnapshotAge;
        //
        wsState.QUE_IN001_NO = wsQueIn001;
        wsState.QUE_IN001_NO_DONE = wsQueIn001;
        //
        wsState.QUE_OUT_NO = wsQueOutNo;
        //
        wsState.MAIL_SEND_DONE = wsQueOutNo;
        // Возраст аудита станции из ее прошлой жизни
        long wsQueOutAge = ((JdxQueCommon) queCommon).getMaxAgeForAuthorWs(wsId);
        wsState.AGE = wsQueOutAge;
        wsState.AUDIT_AGE_DONE = wsQueOutAge;
        // Поехали
        wsState.MUTE = 0L;
        //
        IReplica replicaSetState = utRepl.createReplicaSetWsState(wsId, wsState);


        // ---
        // Реплика на установку возрастов - отправка.
        // Самым наглым образом, минуя все очереди, тупо под номером 1
        // Станция сейчас в таком состоянии, что ждет тменно этот номер в своей очереди in001
        // Это нужно, чтобы станция при выполнении команды "repair restore from backup" получила нашу первую реплику.
        // В этой первой реплике возраст QUE_OUT_NO и AUDIT_AGE_DONE будет возвращен на место, вместе с остальными возрастами.
        IMailer mailerWs = mailerList.get(wsId);
        mailerWs.send(replicaSetState, "to001", 1);


        // ---
        // Установка (на сервере) всяких возрастов для новой рабочей станции (начальное состояние)
        // не требуется - на сервере состояние станции известно и не нужно его менять
    }

    private List<IReplica> genSnapshotForChanged(long destinationWsId, JSONObject cfgPublicationRulesNew) throws Exception {
        // ---
        // Правила публикации для станции

        // Читаем новые правила публикации для станции
        IPublicationRuleStorage publicationRulesInNew = PublicationRuleStorage.loadRules(cfgPublicationRulesNew, structFull, "in");
        IPublicationRuleStorage publicationRulesOutNew = PublicationRuleStorage.loadRules(cfgPublicationRulesNew, structFull, "out");

        // Читаем текущие (старые) правила публикации для станции
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgPublicationsWsOld = cfgManager.getWsCfg(CfgType.PUBLICATIONS, destinationWsId);
        IPublicationRuleStorage publicationRulesInOld = PublicationRuleStorage.loadRules(cfgPublicationsWsOld, structFull, "in");


        // ---
        // Структура для станции

        // Берем текущую (старую) структуру для станции
        IJdxDbStruct structOld = cfgManager.getWsStruct(destinationWsId);

        // Формирование новой (будущей) рабочей структуры рабочей станции:
        // убирание того, чего нет ни в одном из правил публикаций publicationRulesInNew и publicationRulesOutNew
        IJdxDbStruct structNew = UtRepl.getStructCommon(structFull, publicationRulesInNew, publicationRulesOutNew);


        // ---
        // Определяем разницу между старым и новым составом таблиц, отправляемых с сервера на станцию
        List<IJdxTable> tablesAdded = new ArrayList<>();
        List<IJdxTable> tablesRemoved = new ArrayList<>();
        List<IJdxTable> tablesChanged = new ArrayList<>();

        // Находим разницу между старыми и новыми правилами публикации (правило "in" рабочей станции)
        UtPublicationRule.getPublicationRulesDiff(structNew, publicationRulesInOld, publicationRulesInNew, tablesAdded, tablesRemoved, tablesChanged);

        // Находим разницу между старой и новой структурой
        UtDbComparer.getStructDiff(structOld, structNew, tablesAdded, tablesRemoved, tablesChanged);

        // В списках tablesAdded, tablesRemoved, tablesChanged могут встретиться повторения (мы же два раза сравнивали).
        // Исправим, т.к. это важно при создании триггеров.
        tablesAdded = UtJdx.selectTablesByName(tablesAdded, structFull);
        tablesChanged = UtJdx.selectTablesByName(tablesChanged, structFull);

        // ---
        // Будем делать snapshot для таблиц, добавленных в правила и таблиц с измененными правилами
        List<IJdxTable> tables = new ArrayList<>();
        tables.addAll(tablesAdded);
        tables.addAll(tablesChanged);

        // Делаем snapshot (по новым правилам publicationRulesInNew)
        UtRepl utRepl = new UtRepl(db, structNew);
        List<IReplica> replicasRes = utRepl.createSnapshotForTablesFiltered(tables, SERVER_WS_ID, destinationWsId, publicationRulesInNew);


        // ---
        return replicasRes;
    }


    /**
     * Возвращает список таблиц из правил publicationStorage,
     * на основе порядка, в котором они расположены в описании структуры struct (там список таблиц отсортирован по зависимостям).
     */
    private List<IJdxTable> makeOrderedFromPublicationRules(IJdxDbStruct struct, IPublicationRuleStorage publicationStorage) {
        List<IJdxTable> tablesOrdered = new ArrayList<>();
        //
        for (IJdxTable tableOrderedSample : struct.getTables()) {
            if (publicationStorage.getPublicationRule(tableOrderedSample.getName()) != null) {
                tablesOrdered.add(tableOrderedSample);
            }
        }
        //
        return tablesOrdered;
    }

    public void enableWorkstation(long wsId) throws Exception {
        log.info("enable workstation, wsId: " + wsId);
        //
        SrvWorkstationStateManager stateManager = new SrvWorkstationStateManager(db);
        stateManager.setValue(wsId, "enabled", 1);
    }

    public void disableWorkstation(long wsId) throws Exception {
        log.info("disable workstation, wsId: " + wsId);
        //
        SrvWorkstationStateManager stateManager = new SrvWorkstationStateManager(db);
        stateManager.setValue(wsId, "enabled", 0);
    }

    /**
     * Сервер: считывание из mailer очередей рабочих станций и помещение их в очередь-зеркало на сервере
     */
    public void srvHandleQueIn() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (long wsId : mailerList.keySet()) {
            IMailer mailerWs = mailerList.get(wsId);
            IJdxQue queInSrv = queInList.get(wsId);

            // Обрабатываем каждую станцию
            try {
                log.info("srvHandleQueIn, from.wsId: " + wsId);

                //
                long queDoneNo = stateManager.getWsQueInNoReceived(wsId);
                long mailMaxNo = mailerWs.getBoxState("from");

                //
                long count = 0;
                for (long no = queDoneNo + 1; no <= mailMaxNo; no++) {
                    log.info("receive, wsId: " + wsId + ", receiving.no: " + no);

                    // Физически забираем данные с почтового сервера
                    IReplica replica = UtMail.receiveOrRequestReplica(mailerWs, "from", no, RequiredInfo.EXECUTOR_WS);

                    // Помещаем полученные данные в общую очередь
                    db.startTran();
                    try {
                        // Помещаем в очередь
                        queInSrv.push(replica);

                        // Отмечаем факт скачивания
                        stateManager.setWsQueInNoReceived(wsId, no);

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback();
                        throw e;
                    }

                    //
                    count++;
                }


                // Отметить попытку чтения (для отслеживания активности сервера, когда нет данных для реальной передачи)
                mailerWs.setData(null, "ping.read", "from");


                //
                if (count > 0) {
                    log.info("srvHandleQueIn, from.wsId: " + wsId + ", que.no: " + (queDoneNo + 1) + " .. " + mailMaxNo + ", done count: " + count);
                } else {
                    log.info("srvHandleQueIn, from.wsId: " + wsId + ", que.no: " + mailMaxNo + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvHandleQueIn", e);
                //
                log.error("Error in srvHandleQueIn, from.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }
        }
    }

    /**
     * Сервер: считывание очередей рабочих станций и формирование общей очереди
     * <p>
     * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
     * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
     */
    public void srvHandleCommonQue() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (long wsId : queInList.keySet()) {
            IJdxQue queInSrv = queInList.get(wsId);

            // Обрабатываем каждую станцию
            try {
                log.info("srvHandleCommonQue, from.wsId: " + wsId);

                //
                long queDoneNo = stateManager.getWsQueInNoDone(wsId);
                long queMaxNo = queInSrv.getMaxNo();

                //
                long count = 0;
                for (long no = queDoneNo + 1; no <= queMaxNo; no++) {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", queInSrv.no: " + no);

                    // Обработка входящих очередей
                    db.startTran();
                    try {
                        // --- Формируем общую очередь queCommon

                        // Берем реплику из входящей очереди queInSrv
                        IReplica replica = queInSrv.get(no);

                        // Помещаем в очередь queCommon
                        long queCommonNo = queCommon.push(replica);


                        // --- Специальные реакции на реплики

                        // Станция прислала отчет об изменении своего состояния - отмечаем состояние станции в серверных таблицах
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.MUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setMuteDone(wsId, queCommonNo);
                        }
                        //
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.UNMUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setUnmuteDone(wsId);
                        }


                        // --- Отмечаем, что реплика обработана
                        stateManager.setWsQueInNoDone(wsId, no);

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback();

                        //
                        if (UtJdxErrors.errorIs_replicaFile(e)) {
                            // Пробуем что-то сделать с проблемой реплики в очереди
                            log.error("srvHandleCommonQue, wsId: " + wsId + ", error: " + e.getMessage());

                            // Выясним, у кого куда просить
                            IMailer mailerWs = mailerList.get(wsId);
                            String box = "from";
                            String executor = RequiredInfo.EXECUTOR_WS;

                            // Попросим автора реплики прислать её в ящик, когда дождемся ответа - починим очередь
                            log.info("receiveOrRequestReplica, try replica receive, box: " + box + ", replica.no: " + no + ", executor: " + executor);
                            IReplica replicaNew = UtMail.receiveOrRequestReplica(mailerWs, box, no, executor);
                            log.info("receiveOrRequestReplica, replica receive done");

                            // Обновим "битую" реплику в очереди - заменим на нормальную
                            queInSrv.remove(no);
                            queInSrv.put(replicaNew, no);

                            // Ждем следующего цикла, а пока - ошибка
                            throw new XError("srvHandleCommonQue, requestReplica done, wait for next iteration, wsId: " + wsId + ", queName: " + queInSrv.getQueName() + " , replica.no: " + no);
                        } else {
                            throw e;
                        }
                    }

                    //
                    count++;
                }


                //
                if (count > 0) {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.no: " + queDoneNo + " .. " + queMaxNo + ", done count: " + count);
                } else {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.no: " + queMaxNo + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvHandleCommonQue, from.wsId: " + wsId, e);
                //
                log.error("Error in srvHandleCommonQue, from.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }
        }
    }

    /**
     * Сервер: тиражирование реплик из common.
     * Распределение общей очереди по очередям рабочих станций: common -> out000
     */
    public void srvReplicasDispatch() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // Все что у нас есть на раздачу
        long commonQueMaxNo = queCommon.getMaxNo();

        for (long wsId : mailerList.keySet()) {

            //
            log.info("srvReplicasDispatch, to.wsId: " + wsId);

            try {

                // Исходящая очередь для станции wsId
                JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);

                // Преобразователь по фильтрам
                IReplicaFilter filter = new ReplicaFilter();

                // Правила публикаций (фильтры) для станции wsId.
                // В качестве фильтров на ОТПРАВКУ от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
                IPublicationRuleStorage publicationRule = publicationsInList.get(wsId);

                // Параметры (для правил публикации): получатель реплики (для правил публикации)
                filter.getFilterParams().put("wsDestination", String.valueOf(wsId));

                //
                long sendFrom = stateManager.getDispatchDoneQueCommon(wsId) + 1;
                long sendTo = commonQueMaxNo;

                long countToDo = sendTo - sendFrom + 1;
                long count = 0;
                for (long no = sendFrom; no <= sendTo; no++) {
                    log.info("srvReplicasDispatch, to.wsId: " + wsId + ", no: " + no + ", " + count + "/" + countToDo);

                    // Фильтруем данные из реплики
                    IReplica replicaForWs;
                    IReplica replica = null;
                    try {
                        // Берем реплику из queCommon
                        replica = queCommon.get(no);

                        // Читаем заголовок
                        JdxReplicaReaderXml.readReplicaInfo(replica);

                        // Параметры (для правил публикации): автор реплики
                        filter.getFilterParams().put("wsAuthor", String.valueOf(replica.getInfo().getWsId()));

                        // Преобразовываем по правилам публикаций (фильтрам)
                        replicaForWs = filter.convertReplicaForWs(replica, publicationRule);

                    } catch (Exception e) {

                        if (UtJdxErrors.errorIs_replicaFile(e)) {
                            log.error("srvReplicasDispatch, error: " + e.getMessage());
                            // Сможем  что-то сделать с проблемой реплики в очереди, когда сделаем
                            // https://github.com/SazonovDenis/jdtx-repl/issues/22
                            // А пока - ошибка
                            String replicaInfo;
                            if (replica != null) {
                                replicaInfo = "replica.wsId: " + replica.getInfo().getWsId() + ", replica.age: " + replica.getInfo().getAge() + ", replica.no: " + replica.getInfo().getNo();
                            } else {
                                replicaInfo = "no replica.info";
                            }
                            throw new XError("srvReplicasDispatch, queCommon.no: " + no + ", " + replicaInfo + ", error: " + e.getMessage());
                        } else {
                            throw e;
                        }
                    }

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
                if (count > 0) {
                    log.info("srvReplicasDispatch done, to.wsId: " + wsId + ", out001.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
                } else {
                    log.info("srvReplicasDispatch done, to.wsId: " + wsId + ", out001.no: " + sendTo + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvReplicasDispatch, to.wsId: " + wsId, e);
                //
                log.error("Error in srvReplicasDispatch, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
    }


    /**
     * Рассылка реплик в ящики каждой рабочей станции, штатная
     */
    public void srvReplicasSend() throws Exception {
        for (Map.Entry<Long, IMailer> en : mailerList.entrySet()) {
            long wsId = en.getKey();
            IMailer wsMailer = en.getValue();

            // Рассылаем
            try {
                // Рассылаем очередь out000 (продукт обработки очереди common -> out000) на каждую станцию
                IJdxMailSendStateManager mailStateManager = new JdxMailSendStateManagerSrv(db, wsId, UtQue.SRV_QUE_OUT000);
                JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);

                // Проверка и ремонт отметки "отправлено в ящик на почтовый сервер"
                UtMail.repairSendMarkedBySendDone(queOut000, wsMailer, "to", mailStateManager);

                // Рассылаем очередь
                UtMail.sendQueToMail_State(wsId, queOut000, wsMailer, "to", mailStateManager);


                // Рассылаем очередь queOut001 на каждую станцию
                mailStateManager = new JdxMailSendStateManagerSrv(db, wsId, UtQue.SRV_QUE_OUT001);
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);

                // Проверка и ремонт отметки "отправлено в ящик на почтовый сервер"
                UtMail.repairSendMarkedBySendDone(queOut001, wsMailer, "to001", mailStateManager);

                // Рассылаем очередь
                UtMail.sendQueToMail_State(wsId, queOut001, wsMailer, "to001", mailStateManager);
            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvReplicasSendMail, to.wsId: " + wsId, e);
                //
                log.error("Error in srvReplicasSendMail, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }
        }
    }


    /**
     * Рассылка реплик с сервера, по требованию
     */
    public void replicasSend_Requied() throws Exception {
        for (Map.Entry<Long, IMailer> en : mailerList.entrySet()) {
            long wsId = en.getKey();
            IMailer wsMailer = en.getValue();

            try {
                // ws.from <- srv.common (станция потеряла свою собственную почту)
                String box = "from";
                // Выясняем, что запросили передать
                RequiredInfo requiredInfo = wsMailer.getSendRequired(box);
                MailSendTask sendTask = UtMail.getRequiredSendTask(null, requiredInfo, RequiredInfo.EXECUTOR_SRV);
                // Отправляем из очереди, что запросили
                UtMail.sendQueToMail_Required_QueCommon(sendTask, wsId, queCommon, wsMailer, box);

                //
                IJdxMailSendStateManager mailStateManager;

                // ws.to <- srv.out (станция пропустила передачу с сервера)
                box = "to";
                // Выясняем, что запросили передать
                mailStateManager = new JdxMailSendStateManagerSrv(db, wsId, UtQue.SRV_QUE_OUT000);
                RequiredInfo requiredInfo000 = wsMailer.getSendRequired(box);
                MailSendTask sendTask000 = UtMail.getRequiredSendTask(mailStateManager, requiredInfo000, RequiredInfo.EXECUTOR_SRV);
                // Отправляем из очереди, что запросили
                JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);
                UtMail.sendQueToMail_Required(sendTask000, wsId, queOut000, wsMailer, box, mailStateManager);

                // ws.to001 <- srv.out001 (станция пропустила передачу с сервера)
                box = "to001";
                // Выясняем, что запросили передать
                mailStateManager = new JdxMailSendStateManagerSrv(db, wsId, UtQue.SRV_QUE_OUT001);
                RequiredInfo requiredInfo001 = wsMailer.getSendRequired(box);
                MailSendTask sendTask001 = UtMail.getRequiredSendTask(mailStateManager, requiredInfo001, RequiredInfo.EXECUTOR_SRV);
                // Отправляем из очереди, что запросили
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);
                UtMail.sendQueToMail_Required(sendTask001, wsId, queOut001, wsMailer, box, mailStateManager);

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvMailRequest, to.wsId: " + wsId, e);
                //
                log.error("Error in srvMailRequest, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
    }


    public void srvSendWsMute(long destinationWsId, String queName) throws Exception {
        log.info("srvSendWs MUTE, destination.WsId: " + destinationWsId + ", que: " + queName);

        // Выбор очереди
        IJdxReplicaQue que = getQueByName(destinationWsId, queName);

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(destinationWsId);

        // ... в выбранную очередь реплик
        que.push(replica);
    }


    public void srvSendWsUnmute(long destinationWsId, String queName) throws Exception {
        log.info("srvSendWs UNMUTE, destination.WsId: " + destinationWsId + ", que: " + queName);

        // Выбор очереди
        IJdxReplicaQue que = getQueByName(destinationWsId, queName);

        // Системная команда "UNMUTE" ...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaUnmute(destinationWsId);

        // ... в выбранную очередь реплик
        que.push(replica);
    }


    public void srvMuteAll() throws Exception {
        log.info("srvMuteAll");

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(0);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvUnmuteAll() throws Exception {
        log.info("srvUnmuteAll");

        // Системная команда "UNMUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaUnmute(0);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public long srvMuteState(boolean doWaitMute, boolean doWaitUnmute, long muteAgeWait) throws Exception {
        if (doWaitMute && doWaitUnmute) {
            throw new XError("doWaitMute && doWaitUnmute");
        }

        while (true) {
            // DataStore stDisplay = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE where enabled = 1");
            // UtData.outTable(stDisplay);
            DataStore stDisplay = db.loadSql(UtReplSql.sql_SrvState("STATE__ENABLED.param_value = 1"));

            // Вычисление состояния
            long count_total = stDisplay.size();
            long countMute = 0;
            long countUnmute = 0;
            long countWaitAge = 0;
            long muteAgeMax = 0;
            long muteAgeMin = Long.MAX_VALUE;
            for (DataRecord recDisplay : stDisplay) {
                long muteAgeWs = recDisplay.getValueLong("mute_age");
                if (muteAgeWs > 0) {
                    countMute = countMute + 1;
                } else {
                    countUnmute = countUnmute + 1;
                }
                if (muteAgeWs >= muteAgeWait) {
                    countWaitAge = countWaitAge + 1;
                }
                if (muteAgeMin > muteAgeWs) {
                    muteAgeMin = muteAgeWs;
                }
                if (muteAgeMax < muteAgeWs) {
                    muteAgeMax = muteAgeWs;
                }
                //
                if (doWaitUnmute) {
                    if (muteAgeWs != 0) {
                        recDisplay.setValue("mute_state", "Wait unmute");
                    } else {
                        recDisplay.setValue("mute_state", "Unmuted ok");
                    }
                } else if (doWaitMute) {
                    if (muteAgeWait == 0) {
                        if (muteAgeWs == 0) {
                            recDisplay.setValue("mute_state", "Wait mute");
                        } else {
                            recDisplay.setValue("mute_state", "Muted ok");
                        }
                    } else {
                        if (muteAgeWs == 0) {
                            recDisplay.setValue("mute_state", "Wait mute");
                        } else if (muteAgeWs < muteAgeWait) {
                            recDisplay.setValue("mute_state", "Wait age");
                        } else {
                            recDisplay.setValue("mute_state", "Age ok");
                        }
                    }
                } else {
                    if (muteAgeWs == 0) {
                        recDisplay.setValue("mute_state", "Unmute");
                    } else {
                        recDisplay.setValue("mute_state", "Mute");
                    }
                }
            }

            // Печать состояния
            UtData.outTable(stDisplay);
            //
            if (countMute == 0) {
                System.out.println("No workstations in MUTE");
            } else if (countMute == count_total) {
                System.out.println("All workstations in MUTE, min age: " + muteAgeMin + ", max age: " + muteAgeMax);
            } else {
                System.out.println("Workstations in MUTE: " + countMute + "/" + count_total);
            }

            // Не делаем ожидания, если не нужно
            if (!doWaitMute && !doWaitUnmute) {
                return muteAgeMax;
            }

            // Выход из ожидания, если все UNMUTE
            if (doWaitUnmute && countUnmute == count_total) {
                return muteAgeMax;
            }

            // Выход из ожидания, если все MUTE и не нужно ждать возраста
            if (doWaitMute && countMute == count_total && muteAgeWait == 0) {
                return muteAgeMax;
            }

            // Выход из ожидания, если если все MUTE и дождались возраста
            if (doWaitMute && countMute == count_total && muteAgeMin >= muteAgeWait) {
                System.out.println("Wait for MUTE age done: " + muteAgeWait);
                return muteAgeMax;
            }

            //
            if (doWaitUnmute) {
                System.out.println("Wait for all UNMUTE");
            } else if (muteAgeWait == 0) {
                System.out.println("Wait for all MUTE");
            } else {
                System.out.println("Wait for MUTE age: " + muteAgeWait + ", done: " + countWaitAge + "/" + count_total);
            }

            //
            Thread.sleep(2000);
        }
    }


    public void srvAppUpdate(String exeFileName, String queName) throws Exception {
        log.info("srvAppUpdate, exe fileName: " + exeFileName);

        UtRepl utRepl = new UtRepl(db, struct);

        // Выбор очереди - общая (queCommon) или личная для станции
        IJdxReplicaQue que;
        if (queName.compareToIgnoreCase(UtQue.SRV_QUE_COMMON) == 0) {
            // Очередь queCommon (общая)
            que = queCommon;

            // Команда на обновление
            IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

            // Системная команда - в исходящую очередь реплик
            que.push(replica);

            //
            log.info("srvAppUpdate, to ws all, QUE_COMMON");
        } else if (queName.compareToIgnoreCase(UtQue.SRV_QUE_OUT001) == 0) {
            for (long wsId : mailerList.keySet()) {
                // Очередь queOut001 станции (инициализационная или для системных команд)
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);
                que = queOut001;

                // Команда на обновление
                IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

                // Системная команда - в исходящую очередь реплик
                que.push(replica);

                //
                log.info("srvAppUpdate, to ws: " + wsId);
            }
        } else {
            throw new XError("Unknown queName: " + queName);
        }
    }

    public void srvMergeRequest(String planFileName) throws Exception {
        log.info("srvMergeRequest, plan file: " + planFileName);

        //
        UtRepl utRepl = new UtRepl(db, struct);

        //
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        Collection<RecMergePlan> mergePlans = reader.readPlans(planFileName);

        //
        IJdxDataSerializer dataSerializer = db.getApp().service(DataSerializerService.class);

        IWsSettings wsSettings = db.getApp().service(WsSettingsService.class);
        long selfWsId = wsSettings.getWsId();

        //
        for (RecMergePlan mergePlan : mergePlans) {
            // Таблица и поля в Serializer-е
            IJdxTable table = struct.getTable(mergePlan.tableName);
            dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));

            //
            log.info("srvMergeRequest, table: " + mergePlan.tableName + ", records delete: " + mergePlan.recordsDelete.size());

            // Добавим эталонную запись на сервере
            Map<String, Object> values = dataSerializer.prepareValues(mergePlan.recordEtalon);
            // Чтобы вставилось с новым PK
            IJdxField pkField = table.getPrimaryKey().get(0);
            String pkFieldName = pkField.getName();
            values.put(pkFieldName, null);
            // Вставляем эталонную запись
            JdxDbUtils dbu = new JdxDbUtils(db, struct);
            long recordEtalonId = dbu.insertRec(mergePlan.tableName, values);

            // Записываем PK только что вставленной записи
            values.put(pkFieldName, recordEtalonId);

            // Исправляем запись mergePlan.recordEtalon в плане (исправляем ссылки если они в плане не подготовлены с дополнением)
            Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
            mergePlan.recordEtalon = valuesStr;

            // Отправим реплику на вставку эталонной записи
            IReplica replicaIns = utRepl.createReplicaInsRecord(mergePlan.tableName, mergePlan.recordEtalon, selfWsId);
            replicaIns.getInfo().setReplicaType(JdxReplicaType.IDE_MERGE);
            queCommon.push(replicaIns);

            // todo рассмотреть неоходимость добавлять каскадно и ВЛИЯЮШИЕ записи для вставляемой эталонной
            //  (и далее - каскадно) - их ТОЖЕ может не оказаться на филиале

            //
            //log.info("srvMergeRequest, replica etalon ins: " + replicaIns.getData());

            // Исправляем ссылки в mergePlan.recordsDelete (если они в плане не подготовлены с дополнением ссылки)
            // Распаковываем PK удаляемых записей
            Collection<Long> recordsDelete = new ArrayList<>();
            for (String recordDeleteIdStr : mergePlan.recordsDelete) {
                Long recordDeleteId = UtJdxData.longValueOf(dataSerializer.prepareValue(recordDeleteIdStr, pkField));
                recordsDelete.add(recordDeleteId);
            }
            // Обратно запаковываем PK удаляемых записей
            Collection<String> recordsDeleteStr = new ArrayList<>();
            for (Long recordDeletePk : recordsDelete) {
                String recordDeletePkStr = dataSerializer.prepareValueStr(recordDeletePk, pkField);
                recordsDeleteStr.add(recordDeletePkStr);
            }
            // Исправляем PK у recordsDelete в плане
            mergePlan.recordsDelete = recordsDeleteStr;
        }

        // Записываем обновленный план (в нем PK новых записей проставлены как надо)
        String planFileNameRef = dataRoot + "temp/" + "plan.json";
        reader.writePlans(mergePlans, planFileNameRef);

        // Формируем команду на merge
        IReplica replicaMerge = utRepl.createReplicaMerge(planFileNameRef);
        queCommon.push(replicaMerge);

        //
        //log.info("srvMergeRequest, replica merge: " + replicaMerge.getData());
        log.info("srvMergeRequest, replica merge done");
    }

    public void srvRequestSnapshot(long destinationWsId, String tableNames, String queName) throws Exception {
        log.info("srvRequestSnapshot, destination wsId: " + destinationWsId + ", tables: " + tableNames + ", que: " + queName);

        // Разложим в список
        List<IJdxTable> tableList = UtJdx.selectTablesByName(tableNames, struct);

        // Сортируем список, чтобы несколько snapsot-реплик не сломали ссылки
        List<IJdxTable> tableListSorted = UtJdx.sortTablesByReference(tableList);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        for (IJdxTable table : tableListSorted) {
            log.info("srvRequestSnapshot, table: " + table.getName());

            // Выбор очереди, куда пошлем запрос - общая (queCommon) или личная для станции (queOut001)
            IJdxReplicaQue que = getQueByName(destinationWsId, queName);

            // Реплика-запрос snapshot
            IReplica replica = utRepl.createReplicaWsSendSnapshot(destinationWsId, table.getName());

            // Реплика-запрос snapshot - в очередь реплик
            que.push(replica);
        }

    }

    /**
     * Отправляет shapshot таблиц в очередь queOut001 на станцию destinationWsId
     *
     * @param destinationWsId станция получатель
     * @param tableNames      Список таблиц через запятую
     */
    public void srvSendSnapshot(long destinationWsId, String tableNames) throws Exception {
        log.info("srvSendSnapshot, destination wsId: " + destinationWsId + ", tables: " + tableNames);

        // Очередь queOut001 станции (инициализационная или для системных команд)
        JdxQueOut001 queOut001 = new JdxQueOut001(db, destinationWsId);
        queOut001.setDataRoot(dataRoot);

        // Разложим в список
        List<IJdxTable> tables = UtJdx.selectTablesByName(tableNames, struct);

        // Правила публикаций (фильтры) для станции wsId.
        // В качестве фильтров на ОТПРАВКУ от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
        IPublicationRuleStorage publicationRule = publicationsInList.get(destinationWsId);

        // Создаем снимок таблицы (разрешаем отсылать чужие записи)
        UtRepl utRepl = new UtRepl(db, struct);
        List<IReplica> replicasRes = utRepl.createSnapshotForTablesFiltered(tables, SERVER_WS_ID, destinationWsId, publicationRule);

        // Отправляем снимок таблицы в очередь queOut001
        utRepl.sendToQue(replicasRes, queOut001);
    }


    /**
     * Отправляем команду на изменение структуры на рабочую станцию
     *
     * @param cfgFileName     правила публикации
     * @param destinationWsId станция, на которую отправляем команду
     * @param queName         в какую очередь поместим команду и выгрузим снимок
     */
    public void srvSetAndSendDbStruct(String cfgFileName, long destinationWsId, String queName) throws Exception {
        log.info("srvSetAndSendDbStruct");

        //
        CfgManager cfgManager = new CfgManager(db);

        // Выбор очереди - общая (queCommon) или личная для станции
        IJdxReplicaQue que = getQueByName(destinationWsId, queName);

        // Читаем новый конфиг
        JSONObject cfgPublicationsNew = UtRepl.loadAndValidateJsonFile(cfgFileName);

        // За новую структуру берем текущую структуру сервера. Она для нас образец.
        IJdxDbStruct structNew = struct;

        // Какой нужно отправить снимок на станцию из-за изменения структуры и правил входных публикаций?
        List<IReplica> replicasSnapshot = genSnapshotForChanged(destinationWsId, cfgPublicationsNew);

        // ---
        db.startTran();
        try {
            // ---
            // Обновляем структуры сервера (в серверном списке рабочий станций SRV_WORKSTATION_LIST)

            // Обновляем конфиг cfg_publications рабочей станции
            cfgManager.setWsCfg(cfgPublicationsNew, CfgType.PUBLICATIONS, destinationWsId);

            // Обновляем конфиг db_struct рабочей станции
            cfgManager.setWsStruct(structNew, destinationWsId);

            // ---
            // Отправляем команду "задать структуру БД"
            UtRepl utRepl = new UtRepl(db, structNew);
            IReplica replica = utRepl.createReplicaSetDbStruct(destinationWsId, cfgPublicationsNew, true);
            que.push(replica);

            // ---
            // Помещаем snapshot-реплики для станции в очередь
            // Формируем snapshot-реплики ПЕРЕД тем, как записать новые настройки для станции,
            // чтобы иметь возможность сравнить старые и новые настройки станции.
            // Отправляем snapshot-реплики ПОСЛЕ отправки новых настроек для станции,
            // чтобы станция СНАЧАЛА получила новые настройки, а потом имела возможность из применять -
            // а то вдруг из-за старой конфигурации снимки будут проигнорированы или применены с ошибкой.
            if (replicasSnapshot != null) {
                UtRepl ut = new UtRepl(db, null);
                ut.sendToQue(replicasSnapshot, que);
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

    /**
     * Задаем новую рабочую структуру базы данных на сервере.
     *
     * @param cfgFileName правила публикации
     * @param queName     в какую очередь поместим команду
     */
    public void srvSetDbStruct(String cfgFileName, String queName) throws Exception {
        log.info("srvSetDbStruct");

        //
        CfgManager cfgManager = new CfgManager(db);

        // Выбор очереди - общая (queCommon) или личная для станции
        IJdxReplicaQue que = getQueByName(SERVER_WS_ID, queName);

        // Читаем новый конфиг
        JSONObject cfgPublicationsNew = UtRepl.loadAndValidateJsonFile(cfgFileName);

        // Сменили конфиг - перечитаем структуру
        IJdxDbStruct structNew = UtRepl.filterStruct(structFull, cfgPublicationsNew);

        // ---
        // Обновляем структуры рабочей станции (конфиги в WS_INFO)

        // Обновляем конфиг cfg_publications своей "серверной" рабочей станции
        cfgManager.setSelfCfg(cfgPublicationsNew, CfgType.PUBLICATIONS);

        // ---
        // Обновляем структуры сервера (в серверном списке рабочий станций SRV_WORKSTATION_LIST)

        // Обновляем конфиг cfg_publications рабочей станции
        cfgManager.setWsCfg(cfgPublicationsNew, CfgType.PUBLICATIONS, SERVER_WS_ID);

        // Обновляем конфиг db_struct рабочей станции
        cfgManager.setWsStruct(structNew, SERVER_WS_ID);

        // ---
        // Отправляем команду "задать структуру БД" для своей "серверной" рабочей станции
        // Отправляем затем, чтобы
        // 1) Обновилась структура рабочей станции
        // 2) Отправился снепшот с сервера, если нужно
        // Это, конечно, можно сделать прямо сейчас, но лучше задействовать штатный механизм,
        // чтобы серверная станция не выделялась среди остальных.
        UtRepl utRepl = new UtRepl(db, structNew);
        IReplica replica = utRepl.createReplicaSetDbStruct(SERVER_WS_ID, cfgPublicationsNew, true);
        que.push(replica);
    }

    public void srvSetAndSendCfg(String cfgFileName, String cfgType, long destinationWsId, String queName) throws Exception {
        log.info("srvSendCfg, cfgFileName: " + new File(cfgFileName).getAbsolutePath() + ", cfgType: " + cfgType + ", destination wsId: " + destinationWsId + ", que: " + queName);

        // Выбор очереди - общая (queCommon) или личная для станции
        IJdxReplicaQue que = getQueByName(destinationWsId, queName);

        //
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfgFileName);

        // ---
        db.startTran();
        try {
            // Обновляем конфиг в таблицах для рабочих станций (SRV_WORKSTATION_LIST)
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setWsCfg(cfg, cfgType, destinationWsId);

            // Отправляем соттветствующие команды в очередь для станции
            UtRepl utRepl = new UtRepl(db, struct);
            IReplica replica = utRepl.createReplicaSetCfg(cfg, cfgType, destinationWsId);
            que.push(replica);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            throw e;
        }
    }

    /**
     * Выбор очереди - общая (queCommon) или личная (queOut001) для станции wsId
     */
    private IJdxReplicaQue getQueByName(long wsId, String queName) {
        IJdxReplicaQue que;

        //
        if (queName.compareToIgnoreCase(UtQue.SRV_QUE_COMMON) == 0) {
            // Очередь queCommon (общая)
            que = queCommon;
        } else if (queName.compareToIgnoreCase(UtQue.SRV_QUE_OUT001) == 0) {
            // Очередь queOut001 станции (инициализационная или для системных команд)
            JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
            queOut001.setDataRoot(dataRoot);
            que = queOut001;
        } else {
            throw new XError("Unknown queName: " + queName);
        }

        //
        return que;
    }

    /**
     * Список активных рабочих станций
     */
    private DataStore loadWsList() throws Exception {
        // Берем только активные
        String sql = "select\n" +
                "  WORKSTATION_LIST.*\n" +
                "from\n" +
                "  " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST WORKSTATION_LIST\n" +
                "  left join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE STATE__ENABLED" +
                "    on (WORKSTATION_LIST.id = STATE__ENABLED.ws_id and STATE__ENABLED.param_name = 'enabled')\n" +
                "where\n" +
                "  STATE__ENABLED.param_value = 1";
        //
        DataStore st = db.loadSql(sql);

        //
        return st;
    }


}


