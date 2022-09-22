package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.utils.io.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.database_info.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.rec_merge.*;
import jdtx.repl.main.api.repair.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.settings.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.task.*;
import org.apache.commons.io.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.json.simple.*;

import java.io.*;
import java.sql.*;
import java.util.*;


/**
 * Контекст рабочей станции
 */
public class JdxReplWs {

    //
    private final long MAX_COMMIT_RECS = 10000;

    // Правила публикации
    protected IPublicationRuleStorage publicationIn;
    protected IPublicationRuleStorage publicationOut;

    //
    protected IJdxQue queIn;
    protected IJdxQue queIn001;
    protected IJdxQue queOut;

    //
    private final Db db;
    protected long wsId;
    protected String wsGuid;
    protected JSONObject cfgDecode;

    /**
     * Рабочая структура БД - только те таблицы, которые мы обрабатываем
     */
    protected IJdxDbStruct struct;
    protected IJdxDbStruct structAllowed;
    protected IJdxDbStruct structFixed;
    /**
     * Полная физическая структура БД
     */
    protected IJdxDbStruct structFull;
    protected String databaseInfo;

    // Параметры приложения
    public IVariantMap appCfg;

    //
    private IMailer mailer;

    //
    protected String dataRoot;
    protected String dataDir;

    //
    public JdxErrorCollector errorCollector = null;

    //
    private static final Log log = LogFactory.getLog("jdtx.Workstation");

    //
    public JdxReplWs(Db db) throws Exception {
        this.db = db;
    }

    public long getWsId() {
        return wsId;
    }

    public String getWsGuid() {
        return wsGuid;
    }

    public String getDataRoot() {
        return dataRoot;
    }

    public String getDataDir() {
        return dataDir;
    }

    public IMailer getMailer() {
        return mailer;
    }

    /**
     * Рабочая станция, запуск
     */
    public void init() throws Exception {
        if (MDC.get("serviceName") == null) {
            MDC.put("serviceName", "ws");
        }

        // Строго обязательно REPEATABLE_READ, иначе сохранение в age возраста аудита
        // будет не синхронно с изменениями в таблицах аудита.
        db.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        // Проверка версии служебных структур в БД
        IDbObjectManager dbObjectManager = db.service(DbObjectManager.class);
        dbObjectManager.checkVerDb();

        // Проверка, что инициализация станции прошла
        dbObjectManager.checkReplicationInit();

        // Читаем и проверяем код нашей рабочей станции
        readIdGuid();

        // В каком каталоге работаем
        initDataRoot();
        initDataDir(wsId);

        // Чтение структуры БД
        IJdxDbStructReader structReader = new JdxDbStructReader();
        structReader.setDb(db);
        structFull = structReader.readDbStruct();

        // Чтение конфигурации
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgWs = cfgManager.getSelfCfg(CfgType.WS);
        JSONObject cfgPublications = cfgManager.getSelfCfg(CfgType.PUBLICATIONS);
        JSONObject cfgDecode = cfgManager.getSelfCfg(CfgType.DECODE);
        this.cfgDecode = cfgDecode;

        // Параметры приложения
        appCfg = loadAppCfg((JSONObject) cfgWs.get("app"));

        // Рабочие каталоги
        String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
        String mailLocalDirTmp = dataRoot + "temp/";

        // Читаем из общей очереди
        queIn = new JdxQueInWs(db, UtQue.QUE_IN, UtQue.STATE_AT_WS);
        String queIn_DirLocal = dataDir + "que_in";
        queIn.setDataRoot(queIn_DirLocal);

        // Читаем из очереди 001
        queIn001 = new JdxQueInWs(db, UtQue.QUE_IN001, UtQue.STATE_AT_WS);
        String queIn001_DirLocal = dataDir + "que_in001";
        queIn001.setDataRoot(queIn001_DirLocal);

        // Пишем в эту очередь
        queOut = new JdxQuePersonal(db, UtQue.QUE_OUT, wsId);
        String queOut_DirLocal = dataDir + "que_out";
        queOut.setDataRoot(queOut_DirLocal);

        // Конфиг для мейлера
        JSONObject cfgMailer = new JSONObject();
        String url = (String) cfgWs.get("url");
        cfgMailer.put("guid", this.wsGuid);
        cfgMailer.put("url", url);
        cfgMailer.put("localDirTmp", mailLocalDirTmp);

        // Мейлер
        mailer = new MailerHttp();
        mailer.init(cfgMailer);

        // Правила публикаций
        publicationIn = PublicationRuleStorage.loadRules(cfgPublications, structFull, "in");
        publicationOut = PublicationRuleStorage.loadRules(cfgPublications, structFull, "out");

        // Формирование рабочей структуры:
        // убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut
        struct = UtRepl.filterStruct(structFull, cfgPublications);

        // Разрешенные и фиксированные структуры
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);
        structAllowed = databaseStructManager.getDbStructAllowed();
        structFixed = databaseStructManager.getDbStructFixed();

        //
        DatabaseInfoReaderService databaseInfoReaderService = db.getApp().service(DatabaseInfoReaderService.class);
        IDatabaseInfoReader databaseInfoReader = databaseInfoReaderService.createDatabaseInfoReader(db, struct);
        databaseInfo = databaseInfoReader.readDatabaseVersion();

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
        //
        String wsIdStr = UtString.padLeft(String.valueOf(wsId), 3, "0");
        dataDir = dataRoot + "ws_" + wsIdStr + "/";
        //
        log.info("dataRoot: " + dataRoot);
    }

    /**
     * Где наши данные.
     * Оформлен как отдельный метод, чтобы можно было вызывать только его (в тестах и т.д.)
     */
    void initDataDir(long wsId) {
        String wsIdStr = UtString.padLeft(String.valueOf(wsId), 3, "0");
        dataDir = dataRoot + "ws_" + wsIdStr + "/";
        //
        log.info("dataDir: " + dataDir);
    }

    /**
     * Читаем и проверяем код и guid нашей станции.
     * Оформлен как отдельный метод, чтобы можно было вызывать только его
     * из jdtx.repl.main.service.UtReplService#remove(), без инициализации и смены версии БД.
     */
    public void readIdGuid() throws Exception {
        IWsSettings wsSettings = db.getApp().service(WsSettingsService.class);
        this.wsId = wsSettings.getWsId();
        this.wsGuid = wsSettings.getWsGuid();
        // Проверяем код нашей станции
        if (this.wsId == 0) {
            throw new XError("Invalid workstation.ws_id == 0");
        }
        log.info("wsId: " + wsId);
    }

    private IVariantMap loadAppCfg(JSONObject app) {
        //
        IVariantMap res = new VariantMap();

        //
        if (app != null) {
            res.put("autoUseRepairReplica", UtJdxData.booleanValueOf(app.get("autoUseRepairReplica"), true));
            res.put("skipForeignKeyViolationIns", UtJdxData.booleanValueOf(app.get("skipForeignKeyViolationIns"), false));
            res.put("skipForeignKeyViolationUpd", UtJdxData.booleanValueOf(app.get("skipForeignKeyViolationUpd"), false));
        }

        //
        return res;
    }

    public JSONObject getCfgDecode() {
        return cfgDecode;
    }

    /**
     * Рабочая станция, инициализация окружения при создании репликации
     */
    public void firstSetup() {
        UtFile.mkdirs(queIn001.getBaseDir());
        UtFile.mkdirs(queIn.getBaseDir());
        UtFile.mkdirs(queOut.getBaseDir());
    }

    /**
     * Проверка версии приложения, обновление при необходимости
     * <p>
     * Рабочая станция вседа обновляет приложение, а сервер - просто ждет пока приложение обновится.
     * Это разделение для того, чтобы на серверной базе
     * сервер и рабчая станция одновременно не кинулись обновлять.
     */
    public void doAppUpdate() throws Exception {
        String appRoot = new File(db.getApp().getRt().getChild("app").getValueString("appRoot")).getCanonicalPath();
        UtAppUpdate ut = new UtAppUpdate(db, appRoot);
        ut.checkAppUpdate(true);
    }

    /**
     * Проверка версии приложения, ошибка при несовпадении.
     */
    public void checkAppUpdate() throws Exception {
        String appRoot = new File(db.getApp().getRt().getChild("app").getValueString("appRoot")).getCanonicalPath();
        UtAppUpdate ut = new UtAppUpdate(db, appRoot);
        ut.checkAppUpdate(false);
    }


    /**
     * Формируем snapshot-реплику для выбранных записей idList, из таблицы tableName,
     * помещаем ее в очередь out.
     */
    public void createSnapshotByIdListIntoQueOut(String tableName, Collection<Long> idList) throws Exception {
        log.info("createTableReplicaByIdList, wsId: " + wsId + ", table: " + tableName + ", count: " + idList.size());

        //
        IJdxTable table = struct.getTable(tableName);
        UtRepl utRepl = new UtRepl(db, struct);

        // Снимок делаем в рамках одной транзакции - чтобы видеть непроитворечивое состояние таблиц
        IReplica replicaSnapshot;
        //
        db.startTran();
        try {
            replicaSnapshot = utRepl.createSnapshotByIdList(wsId, table, idList);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        // Помещаем реплику в очередь
        queOut.push(replicaSnapshot);

        //
        log.info("createReplicaTableByIdList, wsId: " + wsId + ", done");
    }


    /**
     * Обрабатываем свои таблицы аудита на предмет изменений,
     * формируем исходящие реплики.
     */
    public void handleSelfAudit() throws Exception {
        log.info("handleSelfAudit, wsId: " + wsId);

        //
        UtAuditSelector auditSelector = new UtAuditSelector(db, struct, wsId);
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);

        // Если в стостоянии "я замолчал", то молчим
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);
        if (utmm.isMute()) {
            log.warn("handleSelfAudit, workstation is mute");
            return;
        }

        // Проверяем совпадение структур БД
        IJdxDbStruct structAllowed = databaseStructManager.getDbStructAllowed();
        IJdxDbStruct structFixed = databaseStructManager.getDbStructFixed();

        // Проверяем совпадает ли реальная структура БД с разрешенной структурой
        if (struct.getTables().size() == 0 || !UtDbComparer.dbStructIsEqual(struct, structAllowed)) {
            log.warn("handleSelfAudit, database structActual <> structAllowed");
            // Для справки/отладки - структуры в файл
            debugDumpStruct("3.");
            //
            return;
        }
        // Проверяем совпадает ли реальная структура БД с фиксированной структурой
        if (struct.getTables().size() == 0 || !UtDbComparer.dbStructIsEqual(struct, structFixed)) {
            log.warn("handleSelfAudit, database structActual <> structFixed");
            // Для справки/отладки - структуры в файл
            debugDumpStruct("4.");
            //
            return;
        }


        // Формируем реплики (по собственным изменениям)
        db.startTran();
        try {
            // Узнаем (и заодно фиксируем) возраст своего аудита
            UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
            long auditAgeTo = auditAgeManager.markAuditAge();

            // До какого возраста выложили в очередь реплик (из своего аудита)
            JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
            long auditAgeFrom = stateManager.getAuditAgeDoneQueOut();

            //
            long count = 0;
            for (long age = auditAgeFrom + 1; age <= auditAgeTo; age++) {
                IReplica replica = auditSelector.createReplicaFromAudit(publicationOut, age);

                // Пополнение исходящей очереди реплик
                queOut.push(replica);

                // Отметка об обработке аудита в исходящую очередь реплик
                stateManager.setAuditAgeDoneQueOut(age);

                //
                count++;
            }

            //
            if (count > 0) {
                log.info("handleSelfAudit done, wsId: " + wsId + ", audit.age: " + auditAgeFrom + " .. " + auditAgeTo + ", done count: " + count);
            } else {
                log.info("handleSelfAudit done, wsId: " + wsId + ", audit.age: " + auditAgeFrom + ", nothing to do");
            }


            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

    }


    /**
     * Пересоздает реплику из очереди queOut, заменяет существующую на вновь созданную.
     *
     * @param no Номер реплики в очереди queOut
     * @return Пересозданная реплика
     */
    public IReplica recreateQueOutReplica(long no) throws Exception {
        log.info("recreateQueOutReplica, no: " + no);

        // Смотрим старую реплику
        IReplica replicaOriginal = queOut.get(no);

        // По аудиту можем пересозать только реплику типа JdxReplicaType.IDE
        IReplicaInfo replicaInfo = replicaOriginal.getInfo();
        if (replicaInfo.getReplicaType() != JdxReplicaType.IDE) {
            throw new XError("Реплику этого типа невозможно пересоздать, no: " + no + ", replica.type: " + replicaInfo.getReplicaType());
        }
        long age = replicaInfo.getAge();

        // Формируем реплику заново
        UtAuditSelector auditSelector = new UtAuditSelector(db, struct, wsId);
        IReplica replicaRecreated = auditSelector.createReplicaFromAudit(publicationOut, age);

        //
        queOut.put(replicaRecreated, no);

        //
        return replicaRecreated;
    }


    private ReplicaUseResult handleQue(IJdxQue que, long queNoFrom, long queNoTo, boolean forceUse) throws Exception {
        String queName = que.getQueName();
        log.info("handleQue: " + queName + ", self.wsId: " + wsId + ", que.name: " + queName + ", que: " + queNoFrom + " .. " + queNoTo);

        //
        ReplicaUseResult handleQueUseResult = new ReplicaUseResult();

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        long count = 0;
        for (long no = queNoFrom; no <= queNoTo; no++) {
            log.info("handleQue: " + queName + ", self.wsId: " + wsId + ", que.no: " + no + " (count: " + count + "/" + (queNoTo - queNoFrom) + ")");

            //
            ReplicaUseResult replicaUseResult;
            try {
                // Берем реплику из очереди
                IReplica replica = que.get(no);

                // Пробуем применить реплику
                replicaUseResult = useReplicaInternal(replica, forceUse);
            } catch (Exception e) {
                if (UtJdxErrors.errorIs_replicaFile(e)) {
                    log.error("handleQue, error: " + e.getMessage());

                    // Выясним, у кого куда просить
                    // По имени очереди выясним, какой ящик ответит нам за реплику
                    String box;
                    switch (queName) {
                        case UtQue.QUE_IN:
                            box = "to";
                            break;
                        case UtQue.QUE_IN001:
                            box = "to001";
                            break;
                        default:
                            throw new XError("handleQue, unknown mailer.box for que.name: " + queName);
                    }
                    String executor = RequiredInfo.EXECUTOR_SRV;

                    // Попросим автора реплики прислать её в ящик, когда дождемся ответа - починим очередь
                    log.info("receiveOrRequestReplica, try replica receive, box: " + box + ", replica.no: " + no + ", executor: " + executor);
                    IReplica replicaNew = UtMail.receiveOrRequestReplica(mailer, box, no, executor);
                    log.info("receiveOrRequestReplica, replica receive done");

                    // Обновим "битую" реплику в очереди - заменим на нормальную
                    que.remove(no);
                    que.put(replicaNew, no);

                    // Ждем следующего цикла, а пока - ошибка
                    throw new XError("handleQue, requestReplica done, wait for next iteration, queName: " + queName + ", replica.no: " + no);
                } else {
                    throw e;
                }
            }

            if (replicaUseResult.replicaUsed) {
                if (replicaUseResult.lastOwnAgeUsed > handleQueUseResult.lastOwnAgeUsed) {
                    handleQueUseResult.lastOwnAgeUsed = replicaUseResult.lastOwnAgeUsed;
                }
                if (replicaUseResult.lastOwnNoUsed > handleQueUseResult.lastOwnNoUsed) {
                    handleQueUseResult.lastOwnNoUsed = replicaUseResult.lastOwnNoUsed;
                }
            }

            // Реплика использованна?
            if (replicaUseResult.replicaUsed) {
                // Отметим применение реплики
                // Отметку двигаем только вперёд. Это важно учитывать, т.к. бывает ситуация "восстановление базы станции по данным с сервера",
                // в рамках которых может прийти реплика на изменение возраста очередей, а отметку назад двигать не надо.
                long queDoneNow = stateManager.getQueNoDone(queName);
                if (no > queDoneNow) {
                    stateManager.setQueNoDone(queName, no);
                } else {
                    log.info("setQueNoDone was not set, queName: " + queName + ", queNo new: " + no + ", queNo now: " + queDoneNow);
                }

                //
                count++;
            } else {
                // Не отмечаем
                log.info("handleQue, replica not used");
            }

            // Надо останавливаться?
            if (replicaUseResult.doBreak) {
                // Останавливаемся
                log.info("handleQue, break using replicas");
                break;
            }

        }

        //
        if (count > 0) {
            log.info("handleQue: " + queName + ", self.wsId: " + wsId + ", que: " + queNoFrom + " .. " + queNoTo + ", done count: " + count);
        } else {
            log.info("handleQue: " + queName + ", self.wsId: " + wsId + ", que: " + queNoFrom + ", nothing to do");
        }


        //
        return handleQueUseResult;
    }

    public ReplicaUseResult useReplicaFile(File f) throws Exception {
        return useReplicaFile(f, true);
    }

    private ReplicaUseResult useReplicaFile(File f, boolean forceApplySelf) throws Exception {
        log.info("useReplicaFile, self.wsId: " + wsId + ", file: " + f.getAbsolutePath());

        //
        IReplica replica = new ReplicaFile();
        replica.setData(f);
        JdxReplicaReaderXml.readReplicaInfo(replica);

        // Пробуем применить реплику
        ReplicaUseResult replicaUseResult = useReplicaInternal(replica, forceApplySelf);

        // Реплика использованна?
        if (!replicaUseResult.replicaUsed) {
            log.info("useReplicaFile, replica not used");
        }

        //
        return replicaUseResult;
    }


    /**
     * Реакция на команду - перевод в режим "MUTE"
     */
    private void useReplica_MUTE(IReplica replica) throws Exception {
        // Узнаем получателя
        JSONObject info;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        long destinationWsId = UtJdxData.longValueOf(info.get("destinationWsId"));

        // Реакция на команду, если получатель - все станции или именно наша
        if (destinationWsId == 0 || destinationWsId == wsId) {
            log.info("useReplica_MUTE, self.wsId: " + wsId);

            // Последняя обработка собственного аудита
            handleSelfAudit();

            // Переход в состояние "Я замолчал"
            JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
            muteManager.muteWorkstation();

            // Отчитаемся - выкладывание реплики "Я замолчал"
            reportReplica(JdxReplicaType.MUTE_DONE);
        }
    }

    /**
     * Реакция на команду - отключение режима "MUTE"
     */
    private void useReplica_UNMUTE(IReplica replica) throws Exception {
        // Узнаем получателя
        JSONObject info;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        long destinationWsId = UtJdxData.longValueOf(info.get("destinationWsId"));

        // Реакция на команду, если получатель - все станции или именно наша
        if (destinationWsId == 0 || destinationWsId == wsId) {
            log.info("useReplica_UNMUTE, self.wsId: " + wsId);

            // Выход из состояния "Я замолчал"
            JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
            muteManager.unmuteWorkstation();

            // Отчитаемся - выкладывание реплики "Я перестал молчать"
            reportReplica(JdxReplicaType.UNMUTE_DONE);
        }
    }

    /**
     * Реакция на команду - SET_STATE
     */
    private void useReplica_SET_STATE(IReplica replica, ReplicaUseResult useResult) throws Exception {
        // Узнаем получателя
        JSONObject wsStateJson;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            wsStateJson = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        long destinationWsId = UtJdxData.longValueOf(wsStateJson.get("destinationWsId"));

        // Реакция на команду, если получатель - именно наша
        if (destinationWsId == wsId) {
            JdxWsState wsState = new JdxWsState();
            wsState.fromJson(wsStateJson);

            //
            log.info("useReplica_SET_STATE, self.wsId: " + wsId + ", state: " + wsState);

            //
            db.startTran();
            try {
                // --- in
                // Выставим отметку получения в QueIn (её двигаем только вперёд)
                long queInNoNow = queIn.getMaxNo();
                if (wsState.QUE_IN_NO > queInNoNow) {
                    queIn.setMaxNo(wsState.QUE_IN_NO);
                }
                // Выставим отметку использования для QueIn (её двигаем только вперёд)
                JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
                long queInNoDoneNow = stateManager.getQueNoDone("in");
                if (wsState.QUE_IN_NO_DONE > queInNoDoneNow) {
                    stateManager.setQueNoDone("in", wsState.QUE_IN_NO_DONE);
                }

                // --- in001
                // Выставим отметку получения в QueIn001 (её двигаем только вперёд)
                long queIn001NoNow = queIn001.getMaxNo();
                if (wsState.QUE_IN001_NO > queIn001NoNow) {
                    queIn001.setMaxNo(wsState.QUE_IN001_NO);
                }
                // Выставим отметку использования для QueIn001 (её двигаем только вперёд)
                long queIn001NoDoneNow = stateManager.getQueNoDone("in001");
                if (wsState.QUE_IN001_NO_DONE > queIn001NoDoneNow) {
                    stateManager.setQueNoDone("in001", wsState.QUE_IN001_NO_DONE);
                }

                // --- out
                // Выставим отметку получения в QueIn001 (её двигаем только вперёд)
                long queOutNoNow = queOut.getMaxNo();
                if (wsState.QUE_OUT_NO > queOutNoNow) {
                    queOut.setMaxNo(wsState.QUE_OUT_NO);
                }
                // Выставим отметку использования для QueOut (её двигаем только вперёд)
                //long queOutNoDoneNow = stateManager.getQueNoDone("out");
                long queOutNoDoneNow = stateManager.getAuditAgeDoneQueOut();
                if (wsState.AUDIT_AGE_DONE > queOutNoDoneNow) {
                    stateManager.setAuditAgeDoneQueOut(wsState.AUDIT_AGE_DONE);
                }

                // --- MAIL_SEND_DONE
                JdxMailSendStateManagerWs mailStateManagerWs = new JdxMailSendStateManagerWs(db);
                long mailSendDoneNow = mailStateManagerWs.getMailSendDone();
                if (wsState.MAIL_SEND_DONE > mailSendDoneNow) {
                    mailStateManagerWs.setMailSendDone(wsState.MAIL_SEND_DONE);
                }

                // --- AGE
                UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
                long auditAgeNow = auditAgeManager.getAuditAge();
                if (wsState.AGE > auditAgeNow) {
                    auditAgeManager.setAuditAge(wsState.AGE);
                }

                // --- Состояние MUTE
                JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
                if (wsState.MUTE == JdxMuteManagerWs.STATE_MUTE) {
                    // Последняя обработка собственного аудита
                    handleSelfAudit();

                    // Переход в состояние "Я замолчал"
                    muteManager.muteWorkstation();

                    // Отчитаемся - выкладывание реплики "Я замолчал"
                    reportReplica(JdxReplicaType.MUTE_DONE);
                } else {
                    // Выход из состояния "Я замолчал"
                    muteManager.unmuteWorkstation();

                    // Отчитаемся - выкладывание реплики "Я перестал молчать"
                    reportReplica(JdxReplicaType.UNMUTE_DONE);
                }

                //
                db.commit();
            } catch (Exception e) {
                db.rollback(e);
                throw e;
            }

            // --- Отчитаемся
            reportReplica(JdxReplicaType.SET_STATE_DONE);

            //
            useResult.doBreak = true;
        }
    }

    /**
     * Реакция на команду "починить генераторы"
     */
    private void useReplica_REPAIR_GENERATORS(IReplica replica) throws Exception {
        // Узнаем получателя
        JSONObject info;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        long destinationWsId = UtJdxData.longValueOf(info.get("destinationWsId"));

        // Реакция на команду, если получатель - все станции или именно наша
        if (destinationWsId == 0 || destinationWsId == wsId) {

            // Чиним генераторы
            UtPkGeneratorRepair generatorRepair = new UtPkGeneratorRepair(db, struct);
            generatorRepair.repairGenerators();

            // Отчитаемся
            reportReplica(JdxReplicaType.REPAIR_GENERATORS_DONE);
        }
    }

    /**
     * Реакция на команду - SEND_SNAPSHOT
     */
    private void useReplica_SEND_SNAPSHOT(IReplica replica) throws Exception {
        // Узнаем параметры команды: получателя и таблицу
        JSONObject info;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        long destinationWsId = UtJdxData.longValueOf(info.get("destinationWsId"));
        String tableName = (String) info.get("tableName");

        // Реакция на команду, если получатель - именно наша
        if (destinationWsId == wsId) {
            // Список из одной таблицы
            List<IJdxTable> tables = new ArrayList<>();
            tables.add(struct.getTable(tableName));

            // Создаем снимок таблицы (разрешаем отсылать чужие записи)
            // Параметры (для правил публикации и фильтрации): автор и получатель реплики реплики - wsId
            UtRepl utRepl = new UtRepl(db, struct);
            List<IReplica> replicasRes = utRepl.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOut);

            // Отправляем снимок таблицы в очередь queOut
            utRepl.sendToQue(replicasRes, queOut);

            // Выкладывание реплики "snapshot отправлен"
            reportReplica(JdxReplicaType.SEND_SNAPSHOT_DONE);
        }
    }

    /**
     * Реакция на команду - задать "разрешенную" структуру БД
     */
    private void useReplica_SET_DB_STRUCT(IReplica replica, ReplicaUseResult useResult) throws Exception {
        // В реплике - параметры команды
        JSONObject info;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        // Надо ли отправлять snapshot после добавления новых таблиц
        boolean sendSnapshotForNewTables = UtJdxData.booleanValueOf(info.get("sendSnapshot"), true);
        //
        long destinationWsId = UtJdxData.longValueOf(info.get("destinationWsId"));


        // Пришла команда для нашей станции (или всем станциям)?
        if (destinationWsId == 0 || destinationWsId == wsId) {
            log.info("useReplica_SET_DB_STRUCT, self.wsId: " + wsId + ", destinationWsId: " + destinationWsId);

            // В реплике - новая "разрешенная" структура
            IJdxDbStruct structAllowedNew;
            InputStream stream = JdxReplicaReaderXml.createInputStreamData(replica);
            try {
                JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
                structAllowedNew = struct_rw.read(stream);
            } finally {
                stream.close();
            }

            // В реплике - новая конфигурация правил публикации
            JSONObject cfgPublicationsNew;
            InputStream cfgStream = JdxReplicaReaderXml.createInputStream(replica, "cfg.publications.json");
            try {
                String cfgStr = loadStringFromSream(cfgStream);
                cfgPublicationsNew = UtRepl.loadAndValidateJsonStr(cfgStr);
            } finally {
                cfgStream.close();
            }
            //
            IPublicationRuleStorage publicationInNew = PublicationRuleStorage.loadRules(cfgPublicationsNew, structFull, "in");
            IPublicationRuleStorage publicationOutNew = PublicationRuleStorage.loadRules(cfgPublicationsNew, structFull, "out");

            // Формирование новой (будущей) рабочей структуры:
            // убирание того, чего нет ни в одном из правил публикаций publicationInNew и publicationOutNew
            IJdxDbStruct structNew = UtRepl.getStructCommon(structFull, publicationInNew, publicationOutNew);

            // Устанавливаем "разрешенную" структуру БД
            DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);
            databaseStructManager.setDbStructAllowed(structAllowedNew);

            // Проверяем возможность фиксации структуры БД
            // т.е. превращения "разрешенной" в "фиксированную"
            boolean equal_Actual_Allowed = UtDbComparer.dbStructIsEqualTables(structNew, structAllowedNew);
            if (!equal_Actual_Allowed) {
                // Нет возможности фиксации структуры -
                // наша будущая ребочая не совпадает с "разрешенной"
                log.warn("useReplica_SET_DB_STRUCT, can`t ApplyNewDbStruct: structNew <> structAllowedNew");

                // Для справки/отладки - структуры в файл
                debugDumpStruct("2.structNew", structNew);
                debugDumpStruct("2.structAllowedNew", structAllowedNew);

                // Если фиксацию структуры делать нельзя (реальная структура БД не обновлена или по иным причинам),
                // то НЕ метим реплику как использованную
                useResult.replicaUsed = false;
                useResult.doBreak = true;

                return;
            }
            // Наша реальная совпадает с разрешенной, но отличается от зафиксированной

            // Начинаем фиксацию структуры
            log.info("SET_DB_STRUCT, start");

            // Определяем разницу между старым и новым составом таблиц, отправляемых со станции на сервер.
            List<IJdxTable> tablesAdded = new ArrayList<>();
            List<IJdxTable> tablesRemoved = new ArrayList<>();
            List<IJdxTable> tablesChanged = new ArrayList<>();

            // Находим разницу между старой и новой структурой
            UtDbComparer.getStructDiff(structFixed, structAllowedNew, tablesAdded, tablesRemoved, tablesChanged);

            // Находим разницу между старыми и новыми правилами публикации (правило "out" рабочей станции)
            UtPublicationRule.getPublicationRulesDiff(structAllowedNew, publicationOut, publicationOutNew, tablesAdded, tablesRemoved, tablesChanged);

            // В списках tablesAdded, tablesRemoved, tablesChanged могут встретиться повторения (мы же два раза сравнивали).
            // Кроме того, Структура structAllowedNew, переданная на сравнение не содержат полной информации,
            // в частности о первичных ключах, поэтому tablesAdded тоже неполная.
            // Исправим, т.к. это важно при создании триггеров.
            tablesAdded = UtJdx.selectTablesByName(tablesAdded, structFull);
            tablesRemoved = UtJdx.selectTablesByName(tablesRemoved, structFull);
            tablesChanged = UtJdx.selectTablesByName(tablesChanged, structFull);


            // Выполняем применение изменения структуры:
            // Подгоняем структуру аудита под реальную структуру (добавляем для новых и удаляем для удаленных таблиц)
            // В отдельной транзакции
            doChangeAudit(tablesAdded, tablesRemoved);

            // Выполняем применение изменения структуры:
            // Делаем выгрузку snapshot в queOut (для добавленных и измененных таблиц)
            // Обязательно в ОТДЕЛЬНОЙ транзакции (отдельной от изменения структуры).
            if (sendSnapshotForNewTables) {
                List<IJdxTable> tables = new ArrayList<>();
                tables.addAll(tablesAdded);
                tables.addAll(tablesChanged);

                // Делаем snapshot
                UtRepl utRepl = new UtRepl(db, structNew);
                List<IReplica> replicasRes = utRepl.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOutNew);

                // Отправляем snapshot
                utRepl.sendToQue(replicasRes, queOut);
            } else {
                log.info("dbStructApplyFixed, snapshot not send");
            }


            // Обновляем конфиг cfg_publications своей рабочей станции
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setSelfCfg(cfgPublicationsNew, CfgType.PUBLICATIONS);

            // Устанавливаем "фиксированную" структуру БД своей рабочей станции
            databaseStructManager.setDbStructFixed(structAllowedNew);

            // Выкладывание реплики "структура принята"
            reportReplica(JdxReplicaType.SET_DB_STRUCT_DONE);

            //
            log.info("SET_DB_STRUCT, complete");


            // Смена структуры требует переинициализацию репликатора,
            // поэтому обработка входящих реплик прерывается
            useResult.doBreak = true;
        }
    }

    /**
     * Реакция на команду - "задать конфигурацию"
     */
    private void useReplica_SET_CFG(IReplica replica, ReplicaUseResult useResult) throws Exception {
        // Параметры команды
        JSONObject info;
        InputStream cfgInfoStream = JdxReplicaReaderXml.createInputStream(replica, "cfg.info.json");
        try {
            String cfgInfoStr = loadStringFromSream(cfgInfoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgInfoStr);
        } finally {
            cfgInfoStream.close();
        }
        //
        long destinationWsId = UtJdxData.longValueOf(info.get("destinationWsId"));
        String cfgType = (String) info.get("cfgType");

        // Пришла конфигурация для нашей станции (или всем станциям)?
        if (destinationWsId == 0 || destinationWsId == wsId) {
            log.info("useReplica_SET_CFG, self.wsId: " + wsId + ", destinationWsId: " + destinationWsId);

            // В реплике - новая конфигурация
            JSONObject cfg;
            InputStream cfgStream = JdxReplicaReaderXml.createInputStream(replica, "cfg.json");
            try {
                String cfgStr = loadStringFromSream(cfgStream);
                cfg = UtRepl.loadAndValidateJsonStr(cfgStr);
            } finally {
                cfgStream.close();
            }

            // Обновляем конфиг своей рабочей станции
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setSelfCfg(cfg, cfgType);

            // Выкладывание реплики "конфигурация принята"
            reportReplica(JdxReplicaType.SET_CFG_DONE);


            // Обновление конфигурации требует переинициализацию репликатора,
            // поэтому обработка входящих реплик прерывается
            useResult.doBreak = true;
        }
    }


    /**
     * Дополнение аудита (создание журналов и триггеров) - удаление для исключенных и добавление для новых таблиц.
     * В отдельной транзакции.
     *
     * @param tablesAdded   Добавленные таблицы
     * @param tablesRemoved Удаленные таблицы
     */
    private void doChangeAudit(List<IJdxTable> tablesAdded, List<IJdxTable> tablesRemoved) throws Exception {
        // В tables будет соблюден порядок сортировки таблиц с учетом foreign key.
        // При последующем применении snapsot важен порядок.
        tablesAdded = UtJdx.sortTablesByReference(tablesAdded);

        // Актуализируем аудит
        db.startTran();
        try {
            //
            IDbObjectManager dbObjectManager = db.service(DbObjectManager.class);

            //
            long n;

            // Удаляем аудит для удаленных таблиц
            n = 0;
            for (IJdxTable table : tablesRemoved) {
                n++;
                log.info("  dropAudit " + n + "/" + tablesRemoved.size() + " " + table.getName());
                //
                dbObjectManager.dropAudit(table.getName());
            }

            // Создаем аудит для новых таблиц
            n = 0;
            for (IJdxTable table : tablesAdded) {
                n++;
                log.info("  createAudit " + n + "/" + tablesAdded.size() + " " + table.getName());

                //
                if (UtRepl.tableSkipRepl(table)) {
                    log.info("  createAudit, tableSkipRepl == true, table: " + table.getName());
                    continue;
                }

                // Создание отслеживания аудита таблицы
                dbObjectManager.createAudit(table);
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }

    /**
     * Реакция на команду - запуск обновления
     * В этой реплике - номер версии приложения и бинарник для обновления (его надо запустить)
     */
    private void useReplica_UPDATE_APP(IReplica replica, ReplicaUseResult useResult) throws Exception {
        // ---
        // Номер версии приложения
        String appVersionAllowed;
        InputStream stream = JdxReplicaReaderXml.createInputStream(replica, "version");
        try {
            File versionFile = File.createTempFile("~JadatexSync", ".version");
            UtFile.copyStream(stream, versionFile);
            appVersionAllowed = UtFile.loadString(versionFile);
            versionFile.delete();
        } finally {
            stream.close();
        }


        // ---
        // Распаковываем бинарник
        // TODO Обработать ситуацию, когда антивирус съел бинарник.
        // Надо научиться при отсутствии бинарника снова искать последнюю реплику и распаковывать бинарник непосредственно перед запуском
        InputStream replicaStream = JdxReplicaReaderXml.createInputStream(replica, ".exe");
        try {
            UtFile.mkdirs("install");
            File exeFile = new File("install/JadatexSync-update-" + appVersionAllowed + ".exe");
            OutputStream exeFileStream = new FileOutputStream(exeFile);
            UtFile.copyStream(replicaStream, exeFileStream);
            exeFileStream.close();
        } finally {
            replicaStream.close();
        }


        // ---
        // Отмечаем разрешенную версию.
        // Реальное обновление программы будет позже, при следующем запуске
        AppVersionManager appVersionManager = new AppVersionManager(db);
        appVersionManager.setAppVersionAllowed(appVersionAllowed);


        // ---
        // Выкладывание реплики "Я принял обновление приложения"
        reportReplica(JdxReplicaType.UPDATE_APP_DONE);


        // Обновление приложения требует перезапуск репликатора, поэтому обработка входящих реплик прерывается
        useResult.doBreak = true;
    }

    /**
     * Реакция на реплику с данными
     */
    private void useReplica_IDE_SNAPSHOT(IReplica replica, boolean forceApplySelf, ReplicaUseResult useResult) throws Exception {
        int replicaType = replica.getInfo().getReplicaType();

        // Совпадает ли реальная структура БД с разрешенной структурой
        boolean isEqualStruct_Actual_Allowed = UtDbComparer.dbStructIsEqual(struct, structAllowed);


        // Реальная структура базы НЕ совпадает с разрешенной структурой
        if (!isEqualStruct_Actual_Allowed) {
            // Для справки/отладки - не совпадающие структуры - в файл
            debugDumpStruct("5.");

            // Мягкая или жесткая ошибка.
            // Если структура еще не приходила (станция новая),
            // то исключения в логах - бесят, поэтому просто пока прерываемся.
            if (struct.getTables().size() == 0) {
                // Просто ждем
                useResult.replicaUsed = false;
                useResult.doBreak = true;
                log.error("handleQueIn, structActual <> structAllowed");
                return;
            } else {
                // Генерим ошибку
                throw new XError("handleQueIn, structActual <> structAllowed");
            }
        }

        // Свои собственные snapshot-реплики точно можно не применять
        if (isReplicaSelfSnapshot(replica.getInfo()) && !forceApplySelf) {
            log.info("skip self snapshot");
            return;
        }

        // Реальная структура базы НЕ совпадает со структурой, с которой была подготовлена реплика
        JdxReplicaReaderXml.readReplicaInfo(replica);
        String replicaStructCrc = replica.getInfo().getDbStructCrc();
        String dbStructActualCrc = UtDbComparer.getDbStructCrcTables(struct);
        if (replicaStructCrc.compareToIgnoreCase(dbStructActualCrc) != 0) {
            // Для справки/отладки - структуры в файл
            debugDumpStruct("6.");

            // Мягкая или жесткая ошибка.
            // Если структура еще не приходила (станция новая),
            // то исключения в логах - бесят, поэтому просто пока прерываемся.
            if (struct.getTables().size() == 0) {
                // Просто ждем
                useResult.replicaUsed = false;
                useResult.doBreak = true;
                log.error("handleQueIn, wait for database struct, database.structCrc <> replica.structCrc, expected: " + dbStructActualCrc + ", actual: " + replicaStructCrc);
                return;
            } else {
                // Генерим ошибку
                throw new XError("handleQueIn, database.structCrc <> replica.structCrc, expected: " + dbStructActualCrc + ", actual: " + replicaStructCrc);
            }
        }


        // Режим применения собственных реплик
        boolean forceApply_ignorePublicationRules = false;
        if (replicaType == JdxReplicaType.SNAPSHOT || replicaType == JdxReplicaType.IDE_MERGE) {
            // Предполагается, что SNAPSHOT или IDE_MERGE просто так не присылают,
            // значит дело серьезное и нужно обязательно применить.
            forceApply_ignorePublicationRules = true;
        } else if (replicaType == JdxReplicaType.IDE && replica.getInfo().getWsId() == wsId && forceApplySelf) {
            // Свои реплики применяем принудительно, даже если они отфильтруются правилами публикации.
            // Фильтроваться свои реплики могут, если правила на отправку отличаются от правил на получение,
            // а применять их нужно, чтобы обеспечить синхронный поток реплик
            // (иначе получим классическую ситуацию "станции А и В обменялись значениями").
            forceApply_ignorePublicationRules = true;
        }

        // Для реплики типа SNAPSHOT - не слишком огромные порции коммитов
        long commitPortionMax = 0;
        if (replicaType == JdxReplicaType.SNAPSHOT) {
            commitPortionMax = MAX_COMMIT_RECS;
        }

        //
        UtAuditApplyer auditApplyer = new UtAuditApplyer(db, struct);
        auditApplyer.jdxReplWs = this;

        // Параметры (для правил публикации)
        Map<String, String> filterParams = new HashMap<>();
        // Параметры (для правил публикации): автор реплики
        filterParams.put("wsAuthor", String.valueOf(replica.getInfo().getWsId()));
        // Параметры (для правил публикации): получатель реплики (для правил публикации)
        filterParams.put("wsDestination", String.valueOf(wsId));

        //
        auditApplyer.applyReplica(replica, publicationIn, filterParams, forceApply_ignorePublicationRules, commitPortionMax);
    }

    private boolean isReplicaSelfSnapshot(IReplicaInfo replicaInfo) {
        int replicaType = replicaInfo.getReplicaType();
        long replicaWsId = replicaInfo.getWsId();
        return replicaType == JdxReplicaType.SNAPSHOT && replicaWsId == wsId;
    }

    /**
     * Реакция на команду слияния записей
     */
    private void useReplica_MERGE(IReplica replica) throws Exception {
        //
        log.info("UseReplica MERGE");

        // Сохраняем результат выполнения задачи
        String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
        String dirResult = dataRoot + "ws_" + sWsId + "/merge/" + JdxStorageFile.getNo(replica.getData().getName()) + "/";
        UtFile.cleanDir(dirResult);
        File resultFile = new File(dirResult + "result.zip");
        log.info("merge result in: " + dirResult);

        // Распаковываем файл с задачей на слияние
        InputStream stream = JdxReplicaReaderXml.createInputStream(replica, "plan.json");
        try {
            OutputStream fileStream = new FileOutputStream(dirResult + "plan.json");
            UtFile.copyStream(stream, fileStream);
            fileStream.close();
        } finally {
            stream.close();
        }

        // Читаем задачу на слияние
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        Collection<RecMergePlan> mergePlans = reader.readPlans(dirResult + "plan.json");

        //
        log.info("mergePlans.count: " + mergePlans.size());


        // Исполняем задачу на слияние
        db.startTran();
        try {
            // Исполняем
            IJdxDataSerializer dataSerializer = db.getApp().service(DataSerializerService.class);
            //
            JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer);
            recMerger.execMergePlan(mergePlans, resultFile);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }

    private ReplicaUseResult useReplicaInternal(IReplica replica, boolean forceApplySelf) throws Exception {
        ReplicaUseResult useResult = new ReplicaUseResult();

        // Проверим crc реплики
        if (!isReplicaSelfSnapshot(replica.getInfo())) {
            if (!replica.getData().exists()) {
                throw new XError("useReplicaInternal: " + UtJdxErrors.message_replicaDataFileNotExists);
            }
            //
            String crcFile = UtJdx.getMd5File(replica.getData());
            String crcInfo = replica.getInfo().getCrc();
            if (!UtJdx.equalCrc(crcFile, crcInfo)) {
                throw new XError("useReplicaInternal: " + UtJdxErrors.message_replicaBadCrc + ", file.crc: " + crcFile + ", info.crc: " + crcInfo);
            }
        }

        useResult.replicaUsed = true;
        useResult.doBreak = false;
        if (wsId == replica.getInfo().getWsId()) {
            useResult.lastOwnAgeUsed = replica.getInfo().getAge();
            useResult.lastOwnNoUsed = replica.getInfo().getNo();
        }

        //
        int replicaType = replica.getInfo().getReplicaType();
        switch (replicaType) {
            case JdxReplicaType.UPDATE_APP: {
                useReplica_UPDATE_APP(replica, useResult);
                break;
            }

            case JdxReplicaType.MUTE: {
                useReplica_MUTE(replica);
                break;
            }

            case JdxReplicaType.UNMUTE: {
                useReplica_UNMUTE(replica);
                break;
            }

            case JdxReplicaType.SET_STATE: {
                useReplica_SET_STATE(replica, useResult);
                break;
            }

            case JdxReplicaType.REPAIR_GENERATORS: {
                useReplica_REPAIR_GENERATORS(replica);
                break;
            }

            case JdxReplicaType.SEND_SNAPSHOT: {
                useReplica_SEND_SNAPSHOT(replica);
                break;
            }

            case JdxReplicaType.SET_DB_STRUCT: {
                useReplica_SET_DB_STRUCT(replica, useResult);
                break;
            }

            case JdxReplicaType.SET_CFG: {
                useReplica_SET_CFG(replica, useResult);
                break;
            }

            case JdxReplicaType.MERGE: {
                useReplica_MERGE(replica);
                break;
            }

            case JdxReplicaType.IDE:
            case JdxReplicaType.IDE_MERGE:
            case JdxReplicaType.SNAPSHOT: {
                useReplica_IDE_SNAPSHOT(replica, forceApplySelf, useResult);
                break;
            }

            case JdxReplicaType.MUTE_DONE:
            case JdxReplicaType.UNMUTE_DONE:
            case JdxReplicaType.UPDATE_APP_DONE:
            case JdxReplicaType.SET_DB_STRUCT_DONE:
            case JdxReplicaType.SET_CFG_DONE:
            case JdxReplicaType.SET_STATE_DONE:
            case JdxReplicaType.REPAIR_GENERATORS_DONE:
            case JdxReplicaType.SEND_SNAPSHOT_DONE: {
                break;
            }

            default: {
                throw new XError("Unknown replicaType: " + replicaType);
            }
        }

        //
        return useResult;
    }


    /**
     * Применяем входящие реплики из очереди
     */
    public void handleAllQueIn() throws Exception {
        ReplicaUseResult useResult = handleQueIn001();
        if (useResult.doBreak) {
            // Раз просили прерваться - значит прерываемся
            return;
        }
        handleQueIn(false);
    }


    /**
     * Удаление старых реплик в ящиках, задействованных в задаче чтения с сервера.
     */
    public void wsCleanupMailInBox() throws Exception {
        String box = "to";
        long no = queIn.getMaxNo();
        long deleted = mailer.deleteAll(box, no);
        if (deleted != 0) {
            log.info("mailer.deleted, no: " + no + ", box: " + box + " deleted: " + deleted);
        }

        //
        box = "to001";
        no = queIn001.getMaxNo();
        deleted = mailer.deleteAll(box, no);
        if (deleted != 0) {
            log.info("mailer.deleted, no: " + no + ", box: " + box + " deleted: " + deleted);
        }
    }


    private ReplicaUseResult handleQueIn(boolean forceUse) throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long queInNoDone = stateManager.getQueNoDone("in");
        long queInNoAvailable = queIn.getMaxNo();
        return handleQue(queIn, queInNoDone + 1, queInNoAvailable, forceUse);
    }

    private ReplicaUseResult handleQueIn001() throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long queInNoDone = stateManager.getQueNoDone("in001");
        long queInNoAvailable = queIn001.getMaxNo();
        return handleQue(queIn001, queInNoDone + 1, queInNoAvailable, false);
    }

    private String loadStringFromSream(InputStream stream) throws Exception {
        StringLoader ldr = new StringLoader();
        UtLoad.fromStream(ldr, stream);
        return ldr.getResult();
    }


    /**
     * Рабочая станция: отправка системной реплики в исходящую очередь,
     * например, ответа "Я замолчал" или "Я уже не молчу".
     */
    public void reportReplica(int replicaType) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(replicaType);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(-1);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Заканчиваем формирование файла реплики.
        // Заканчиваем cразу-же, т.к. для реплики этого типа не нужно содержимое
        replicaWriter.replicaFileClose();

        //
        db.startTran();  // todo зачем тогда тут транзакция??? !!!!!!!!!!!!!!    Везде, где есть  que***.push - проверить необходимость транзакции
        try {
        // Системная реплика - в исходящую очередь реплик
        queOut.push(replica);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }


    // Физически забираем данные
    public void replicasReceive() throws Exception {
        // --- Ящик to001 в очередь queIn001
        // Узнаем сколько получено у нас
        long selfReceivedNo1 = queIn001.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo1 = mailer.getBoxState("to001");

        // Физически получаем данные
        selfReceivedNo1 = selfReceivedNo1 + 1;
        receiveInternal(mailer, "to001", selfReceivedNo1, srvAvailableNo1, queIn001);

        // --- Ящик to в очередь queIn
        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getBoxState("to");

        if (selfReceivedNo != -1) {
            // Физически получаем данные
            selfReceivedNo = selfReceivedNo + 1;
            receiveInternal(mailer, "to", selfReceivedNo, srvAvailableNo, queIn);
        } else {
            log.warn("JdxReplWs.receive wait, queIn.getMaxNo == -1, self.wsId: " + wsId + ", box: to, que.name: in, srv.available: " + srvAvailableNo);
        }
    }


    IReplica receiveInternalStep(IMailer mailer, String box, long no, IJdxReplicaQue que) throws Exception {
        // Физически забираем данные с почтового сервера
        IReplica replica = UtMail.receiveOrRequestReplica(mailer, box, no, RequiredInfo.EXECUTOR_SRV);

        // Читаем поля заголовка
        JdxReplicaReaderXml.readReplicaInfo(replica);

        // Помещаем реплику в очередь
        que.push(replica);

        // Удаляем с почтового сервера
        //mailer.delete(box, no);

        //
        return replica;
    }

    /**
     * Скачивает из ящика box письма в запрошенном диапазоне и помещает их в очередь que
     */
    void receiveInternal(IMailer mailer, String box, long no_from, long no_to, IJdxReplicaQue que) throws Exception {
        log.info("receive, self.wsId: " + wsId + ", box: " + box + ", que.name: " + ((IJdxQueNamed) que).getQueName() + ", " + no_from + ".." + no_to);

        //
        long count = 0;
        for (long no = no_from; no <= no_to; no++) {
            log.debug("receive, receiving.no: " + no);

            IReplicaInfo info;
            try {
                // Информация о реплике с почтового сервера
                info = mailer.getReplicaInfo(box, no);
            } catch (Exception exceptionMail) {
                // Какая-то ошибка
                if (UtJdxErrors.errorIs_replicaMailNotFound(exceptionMail)) {
                    // Ошибка: реплики в ящике нет - запросм сами повторную передачу
                    UtMail.handleReplicaMailNotFound(mailer, box, no, RequiredInfo.EXECUTOR_SRV, exceptionMail);
                }
                throw exceptionMail;
            }

            // Нужно ли скачивать эту реплику с сервера?
            IReplica replica;
            if (isReplicaSelfSnapshot(info)) {
                // Свои собственные snapshot-реплики можно не скачивать (и в дальнейшем не применять)
                log.info("Found self snapshot replica, no: " + no + ", replica.age: " + info.getAge());
                // Имитируем реплику просто чтобы положить в очередь.
                // Никто не заметит, что реплика пустая, т.к. она НЕ нужна
                replica = new ReplicaFile();
                replica.getInfo().setReplicaType(info.getReplicaType());
                replica.getInfo().setWsId(info.getWsId());
                replica.getInfo().setNo(info.getNo());
                replica.getInfo().setAge(info.getAge());
                //replica.getInfo().setCrc(info.getCrc()); по идее crc тоже удобнее НЕ прописывать - как сигнал, что и файла тоже нет

                // Просто помещаем реплику в очередь
                que.push(replica);

                // Удаляем с почтового сервера
                //mailer.delete(box, no);
            } else {
                receiveInternalStep(mailer, box, no, que);
            }

            //
            count++;
        }


        // Отметить попытку чтения (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.read", box);


        // Отметить состояние рабочей станции
        Map info = getInfoWs();
        mailer.setData(info, "ws.info", null);


        //
        if (count > 0) {
            log.info("receive, self.wsId: " + wsId + ", box: " + box + ", que.name: " + ((IJdxQueNamed) que).getQueName() + ", receive.no: " + no_from + " .. " + no_to + ", done count: " + count);
        } else {
            log.info("receive, self.wsId: " + wsId + ", box: " + box + ", que.name: " + ((IJdxQueNamed) que).getQueName() + ", receive.no: " + no_from + ", nothing to receive");
        }
    }


    /**
     * Отправка реплик с рабочей станции, штатная
     */
    public void replicasSend() throws Exception {
        JdxMailSendStateManagerWs stateManager = new JdxMailSendStateManagerWs(db);
        UtMail.sendQueToMail_State(wsId, queOut, mailer, "from", stateManager);
    }


    /**
     * Отправка реплик с рабочей станции, по требованию.
     * Пересоздает реплики, если запросили.
     */
    public void replicasSend_Required() throws Exception {
        String box = "from";
        // Выясняем, что запросили передать
        IJdxMailSendStateManager mailStateManager = new JdxMailSendStateManagerWs(db);
        RequiredInfo requiredInfo = mailer.getSendRequired(box);
        MailSendTask sendTask = UtMail.getRequiredSendTask(mailStateManager, requiredInfo, RequiredInfo.EXECUTOR_WS);

        // Нужно реплики формировать заново?
        if (sendTask != null && sendTask.recreate) {
            // Формируем заново
            for (long no = sendTask.sendFrom; no <= sendTask.sendTo; no++) {
                recreateQueOutReplica(no);
            }
        }

        // Отправляем из очереди, что запросили
        UtMail.sendQueToMail_Required(sendTask, wsId, queOut, mailer, box, mailStateManager);
    }


    public Map getInfoWs() throws Exception {
        Map info = new HashMap<>();

        //
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxMailSendStateManagerWs stateMailManager = new JdxMailSendStateManagerWs(db);
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);

        //
        long out_auditAgeActual = auditAgeManager.getAuditAge(); // Возраст аудита БД
        long out_queAvailable = stateManager.getAuditAgeDoneQueOut();  // Возраст аудита, до которого сформирована исходящая очередь
        long out_sendDone = stateMailManager.getMailSendDone();  // Возраст, до которого исходящая очередь отправлена на сервер
        long in_queInNoAvailable = queIn.getMaxNo();             // До какого номера есть реплики во входящей очереди
        long in_queInNoDone = stateManager.getQueNoDone("in"); // Номер реплики, до которого обработана (применена) входящая очередь
        boolean isMute = utmm.isMute();

        //
        info.put("out_auditAgeActual", out_auditAgeActual);
        info.put("out_queAvailable", out_queAvailable);
        info.put("out_sendDone", out_sendDone);
        info.put("in_queInNoAvailable", in_queInNoAvailable);
        info.put("in_queInNoDone", in_queInNoDone);
        info.put("databaseInfo", databaseInfo);
        info.put("isMute", isMute);

        //
        try {
            long in_mailAvailable = mailer.getBoxState("to");    // Сколько есть на сервере в ящике для станции
            info.put("in_mailAvailable", in_mailAvailable);
        } catch (Exception e) {
            info.put("in_mailAvailable", e.getMessage());
        }

        //
        return info;
    }


    /**
     * Обработаем аварийную ситуацию - поищем недостающие записи (на которые мы ссылаемся) у себя в каталоге для входящих реплик
     * и выложим их в виде реплики (готовом виде для применения)
     *
     * @return Команды на вставку/изменение этой записи, оформленные в виде реплики
     */
    public File handleFailedInsertUpdateRef(JdxForeignKeyViolationException e) throws Exception {
        IDbErrors dbErrors = db.service(DbErrorsService.class);
        IJdxTable thisTable = dbErrors.get_ForeignKeyViolation_tableInfo(e, struct);
        IJdxForeignKey foreignKey = dbErrors.get_ForeignKeyViolation_refInfo(e, struct);
        IJdxField refField = foreignKey.getField();
        IJdxTable refTable = refField.getRefTable();
        //
        String thisTableName = thisTable.getName();
        String thisTableRefFieldName = refField.getName();
        //
        String refTableName = refTable.getName();
        String refTableFieldName = foreignKey.getTableField().getName();
        //
        String refTableId = e.recValues.get(thisTableRefFieldName);

        //
        log.error("Searching foreign key: " + thisTableName + "." + thisTableRefFieldName + " -> " + refTableName + "." + refTableFieldName + ", foreign key: " + refTableId);

        //
        File outReplicaFile = new File(dataRoot + "temp/" + refTableName + "_" + refTableId.replace(":", "_") + ".zip");
        // Если в одной реплике много ошибочных записей, то искать можно только один раз,
        // иначе на каждую ссылку будет выполнятся поиск, что затянет выкидывание ошибки
        if (outReplicaFile.exists()) {
            log.error("Файл с репликой - результатами поиска уже есть: " + outReplicaFile.getAbsolutePath());
            return outReplicaFile;
        }

        // todo: при невозможности применения реплики из-за fk, и последующем поиске в репликах, не искать в репликах queIn глубже, чем текущая реплика - приедут более старшие, от других станций, которые поправят дела
        // Поиск проблемной записи выполняется в этих каталогах
        String dirNameIn001 = queIn001.getBaseDir();
        String dirNameIn = queIn.getBaseDir();
        String dirNameOut = queOut.getBaseDir();
        String dirs = dirNameIn001 + "," + dirNameIn + "," + dirNameOut;
        // Собираем все операции с проблемной записью в одну реплику
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.findRecordInReplicas(refTableName, refTableId, dirs, true, true, outReplicaFile.getAbsolutePath());

        //
        log.error("Файл с репликой - результатами поиска сформирован: " + replica.getData().getAbsolutePath());

        //
        return outReplicaFile;
    }

    void repairQueByDir(IJdxQue que, long noQue, long noQueDir) throws Exception {
        String queName = que.getQueName();
        log.warn("repairQueByDir: " + queName + ", self.wsId: " + wsId + ", que: " + (noQue + 1) + " .. " + noQueDir);

        JdxStorageFile queFile = new JdxStorageFile();
        queFile.setDataRoot(que.getBaseDir());

        long count = 0;
        for (long no = noQue + 1; no <= noQueDir; no++) {
            log.warn("repairQueByDir: " + queName + ", self.wsId: " + wsId + ", que.no: " + no + " (" + count + "/" + (noQueDir - noQue) + ")");

            // Извлекаем реплику из закромов
            IReplica replica = queFile.get(no);
            JdxReplicaReaderXml.readReplicaInfo(replica);

            // Пополнение (восстановление) очереди
            que.push(replica);

            //
            count = count + 1;
        }

        //
        log.warn("repairQueByDir: " + queName + ", self.wsId: " + wsId + ", que: " + (noQue + 1) + " .. " + noQueDir + ", done count: " + count);
    }

    /**
     * Рабочая станция, задачи по уходу за станцией.
     * Для очередей, задействованных в задаче чтения с сервера.
     */
    public void wsHandleRoutineTaskIn() throws Exception {
        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе
        UtRepl.clearTrashFiles(queIn);
        UtRepl.clearTrashFiles(queIn001);
    }

    /**
     * Выявить ситуацию "станцию восстановили из бэкапа" и починить ее.
     *
     * @param doRepair              Запускать ремонт при обнаружении неисправности.
     * @param doPrintIfNeedNoRepair Нужно ли печатать состояние, если нет неисправности.
     * @throws Exception если обнаружена неисправность (надо чинить), но чинить не просили.
     */
    public void repairAfterBackupRestore(boolean doRepair, boolean doPrintIfNeedNoRepair) throws Exception {
        // ---
        // Анализ
        // ---

        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        // ---
        // Очереди

        // ---
        // Cколько исходящих реплик есть у нас в очереди QueOut (в базе)
        long noQueOut = queOut.getMaxNo();

        // Сколько исходящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueOutDir = queOut.getMaxNoFromDir();

        // Сколько исходящих реплик фактически отправлено на сервер (спросим у почтового сервера)
        long noQueOutSendSrv = mailer.getSendDone("from");


        // ---
        // Сколько входящих получено у нас в очереди QueIn (в базе)
        long noQueIn = queIn.getMaxNo();

        // Сколько входящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueInDir = queIn.getMaxNoFromDir();

        // До какого возраста запрашивалась очередь QueIn с сервера (спросим у почтового сервера)
        long noQueInReadSrv = mailer.getReceiveDone("to");


        // ---
        // Сколько входящих получено у нас в "официальной" очереди
        long noQueIn001 = queIn001.getMaxNo();

        // Сколько входящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueIn001Dir = queIn001.getMaxNoFromDir();

        // До какого возраста запрашивалась очередь QueIn001 с сервера (спросим у почтового сервера)
        long noQueIn001ReadSrv = mailer.getReceiveDone("to001");


        // ---
        // Отметки

        // Сколько исходящих реплик отмечено как отправленое на сервер
        JdxMailSendStateManagerWs mailStateManager = new JdxMailSendStateManagerWs(db);
        long noQueOutSendMarked = mailStateManager.getMailSendDone();

        // До какого возраста обработана очередь QueIn
        long noQueInUsed = stateManager.getQueNoDone("in");

        // До какого возраста обработана очередь QueIn001
        long noQueIn001Used = stateManager.getQueNoDone("in001");


        // ---
        // Есть ли отметка о начале ремонта
        JdxRepairLockFileManager repairLockFileManager = new JdxRepairLockFileManager(getDataDir());
        JdxRepairInfoManager repairInfoManager = new JdxRepairInfoManager(mailer);
        //
        File lockFile = repairLockFileManager.getRepairLockFile();


        /*
        если отправили на сервер больше, чем есть у нас - значит у нас НЕ ХВАТАЕТ данных,
        которые мы отправили на сервер в прошлой жизни.
        Значит, пока мы из ВХОДЯЩЕЙ очереди не получим и не применим НАШУ последнюю реплику - ремонт не закончен
        */

        // Допускается, если в рабочем каталоге QueIn меньше реплик, чем в очереди QueIn (noQueIn > noQueInDir).
        // Это бывает из-за того, что при получении собственных snapshot-реплик, мы ее не скачиваем (т.к. она нам не нужна).
        //
        // Допускается, если не все исходящие реплики находятся в каталоге (noQueOut > noQueOutDir).
        // Это бывает, если удален каталог с репликами.
        boolean needRepair = false;

        if (lockFile.exists()) {
            log.warn("Need repair: lockFile.exists, " + repairLockFileManager.repairLockFileStr());
            needRepair = true;
        }

        if (noQueIn001 != -1 && noQueIn001 < noQueIn001Dir) {
            log.warn("Need repair: noQueIn001 < noQueIn001Dir, noQueIn001: " + noQueIn001 + ", noQueIn001Dir: " + noQueIn001Dir);
            needRepair = true;
        }
        if (noQueIn001 < noQueIn001Used) {
            // Допускается, если не все входящие реплики использованы (noQueIn > noQueInUsed).
            // Это бывает, если прерывается процесс применения реплик.
            // Это не страшно, т.к. при следующем запуске применение возобновится.
            log.warn("Need repair: noQueIn001 < noQueIn001Used, noQueIn001: " + noQueIn001 + ", noQueIn001Used: " + noQueIn001Used);
        }
        if (noQueIn001 < noQueIn001ReadSrv) {
            // Если отличия только на одно письмо - то проверим, осталась ли само письмо
            if (noQueIn001 == (noQueIn001ReadSrv - 1) && isReplicaInBox("to001", noQueIn001ReadSrv)) {
                // Письмо на месте - не считаем ситуацию аварийной.
                // Это бывает, если прервался цикл: чтение с почты - запись в файл - запись в очередь в БД - удаление письма.
                // Это не ошибка, возобновим чтение с нужного места.
                log.info("Need repair: noQueIn001 < noQueIn001ReadSrv, noQueIn001: " + noQueIn001 + ", noQueIn001ReadSrv: " + noQueIn001ReadSrv);
            } else {
                log.warn("Need repair: noQueIn001 < noQueIn001ReadSrv, noQueIn001: " + noQueIn001 + ", noQueIn001ReadSrv: " + noQueIn001ReadSrv);
                needRepair = true;
            }
        }

        if (noQueIn != -1 && noQueIn < noQueInDir) {
            log.warn("Need repair: noQueIn < noQueInDir, noQueIn: " + noQueIn + ", noQueInDir: " + noQueInDir);
            needRepair = true;
        }
        if (noQueIn < noQueInUsed) {
            // Допускается, если не все входящие реплики использованы (noQueIn > noQueInUsed).
            // Это бывает, если прерывается процесс применения реплик.
            // Это не страшно, т.к. при следующем запуске применение возобновится.
            log.info("Need repair: noQueIn < noQueInUsed, noQueIn: " + noQueIn + ", noQueInUsed: " + noQueInUsed);
        }
        if (noQueIn < noQueInReadSrv) {
            // Если отличия только на одно письмо - то проверим, осталась ли само письмо
            if (noQueIn == (noQueInReadSrv - 1) && isReplicaInBox("to", noQueInReadSrv)) {
                // Письмо на месте - не считаем ситуацию аварийной (CRC не проверяем- бывало так, что реплику приготовили заново, ПОСЛЕ получения станцией).
                // Это бывает, если прервался цикл: чтение с почты - запись в файл - запись в очередь в БД - удаление письма.
                // Это не ошибка, возобновим чтение с нужного места.
                log.info("Need repair: noQueIn < noQueInReadSrv, noQueIn: " + noQueIn + ", noQueInReadSrv: " + noQueInReadSrv);
            } else {
                log.warn("Need repair: noQueIn < noQueInReadSrv, noQueIn: " + noQueIn + ", noQueInReadSrv: " + noQueInReadSrv);
                needRepair = true;
            }
        }

        if (noQueOut < noQueOutDir) {
            log.warn("Need repair: noQueOut < noQueOutDir, noQueOut: " + noQueOut + ", noQueOutDir: " + noQueOutDir);
            needRepair = true;
        }
        if (noQueOut < noQueOutSendSrv) {
            log.warn("Need repair: noQueOut < noQueOutSendSrv, noQueOut: " + noQueOut + ", noQueOutSendSrv: " + noQueOutSendSrv);
            needRepair = true;
        }
        if (noQueOutSendMarked != noQueOutSendSrv) {
            // Проверяем ситуацию: помеченное (noQueSendMarked) на 1 письмо меньше отправленного (noQueOutSendSrv)
            if (!UtMail.checkQueSendMarked(noQueOutSendMarked, noQueOutSendSrv, queOut, mailer, "from", mailStateManager)) {
                log.warn("Need repair: QueOut.SendMarked != QueOut.SendSrv, noQueOutSendMarked: " + noQueOutSendMarked + ", noQueOutSendSrv: " + noQueOutSendSrv);
                needRepair = true;
            }
        }

        //
        if (needRepair || doPrintIfNeedNoRepair) {
            log.warn("Restore from backup: need repair: " + needRepair);
            log.warn("  self.wsId: " + wsId);
            log.warn("  noQueIn001: " + noQueIn001);
            log.warn("  noQueIn001Dir: " + noQueIn001Dir);
            log.warn("  noQueIn001ReadSrv: " + noQueIn001ReadSrv);
            log.warn("  noQueIn001Used: " + noQueIn001Used);
            log.warn("  noQueIn: " + noQueIn);
            log.warn("  noQueInDir: " + noQueInDir);
            log.warn("  noQueInReadSrv: " + noQueInReadSrv);
            log.warn("  noQueInUsed: " + noQueInUsed);
            log.warn("  noQueOut: " + noQueOut);
            log.warn("  noQueOutDir: " + noQueOutDir);
            log.warn("  noQueOutSendSrv: " + noQueOutSendSrv);
            log.warn("  noQueOutSendMarked: " + noQueOutSendMarked);
            log.warn("  lockFile: " + repairLockFileManager.repairLockFileStr());
            log.warn("  need repair: " + needRepair);
        }

        //
        if (!needRepair) {
            return;
        }


        // ---
        // Отметим, что проблема обнаружена. После этой отметки ремонт считается НАЧАТЫМ, но НЕ ЗАВЕРШЕННЫМ.
        if (needRepair && !lockFile.exists()) {
            if (noQueIn001Used == 0 && noQueInUsed == 0) {
                repairLockFileManager.repairLockFileCreate(UtCnv.toMap("repairMode_EmptyBase", true));
            } else {
                repairLockFileManager.repairLockFileCreate(null);
            }

            // Отметка на сервер
            repairInfoManager.setRequestRepair(repairLockFileManager.repairLockFileGuid());
        }

        //
        if (needRepair && !doRepair) {
            String errInfo = "" +
                    "noQueIn001: " + noQueIn001 + ", " +
                    "noQueIn001Dir: " + noQueIn001Dir + ", " +
                    "noQueIn001ReadSrv: " + noQueIn001ReadSrv + ", " +
                    "noQueIn001Used: " + noQueIn001Used + ", " +
                    "noQueIn: " + noQueIn + ", " +
                    "noQueInDir: " + noQueInDir + ", " +
                    "noQueInReadSrv: " + noQueInReadSrv + ", " +
                    "noQueInUsed: " + noQueInUsed + ", " +
                    "noQueOut: " + noQueOut + ", " +
                    "noQueOutDir: " + noQueOutDir + ", " +
                    "noQueOutSendSrv: " + noQueOutSendSrv + ", " +
                    "noQueOutSendMarked: " + noQueOutSendMarked + ", " +
                    "lockFile: " + repairLockFileManager.repairLockFileStr() + ", " +
                    "need repair: " + needRepair;
            throw new XError("Detected restore from backup, repair needed: " + errInfo);
        }

        log.warn("==========");
        log.warn("Restore from backup: start repair, lockFile: " + repairLockFileManager.repairLockFileStr());

        //
        Map repairParams = repairLockFileManager.repairLockFileMap();
        boolean repairMode_EmptyBase = UtJdxData.booleanValueOf(repairParams.get("repairMode_EmptyBase"));
        if (repairMode_EmptyBase) {
            log.warn("repairMode_EmptyBase: " + repairMode_EmptyBase);
        }


        // ---
        // Ремонт очередей по данным из каталогов
        // Берем входящие реплики из каталога, кладем их в свою входящую очередь (потом они будут использованы).
        // Ошибки игнорируем, т.к. все, что не исправится из каталога через repairQueByDir будет завпрошено с сервера
        // ---

        // Ситуация: noQueIn001 < noQueInDir001
        // Ремонт очереди QueIn001 по данным из каталога
        try {
            if (noQueIn001 < noQueIn001Dir) {
                repairQueByDir(queIn001, noQueIn001, noQueIn001Dir);
            }
        } catch (Exception e) {
            log.error("repairQueByDir: queIn001, error: " + e.getMessage());
        }
        // Теперь входная очередь QueIn001 такая
        noQueIn001 = queIn001.getMaxNo();

        // Ситуация: noQueIn < noQueInDir
        // Ремонт очереди QueIn по данным из каталога
        try {
            if (noQueIn < noQueInDir) {
                repairQueByDir(queIn, noQueIn, noQueInDir);
            }
        } catch (Exception e) {
            log.error("repairQueByDir: queIn, error: " + e.getMessage());
        }
        // Теперь входная очередь QueIn такая
        noQueIn = queIn.getMaxNo();

        // Ситуация: noQueOut < noQueOutDir
        // Ремонт очереди QueOut по данным из каталога
        try {
            if (noQueOut < noQueOutDir) {
                repairQueByDir(queOut, noQueOut, noQueOutDir);
            }
        } catch (Exception e) {
            log.error("repairQueByDir: queOut, error: " + e.getMessage());
        }
        // Теперь исходящая очередь такая
        noQueOut = queOut.getMaxNo();


        // ---
        // Особая ситуация "база совсем потеряна, полностью пустая"
        // Применим реплику SET_STATE
        // ---
        if (repairMode_EmptyBase && noQueIn001Used == 0 && noQueIn001 > 0) {
            // Применим реплику SET_STATE
            handleQue(queIn001, 1, 1, false);

            // Надо останавливаться
            throw new XError("handleQueIn001, break using replicas");
        }


        // ---
        // Ремонт очередей по данным с сервера
        // ---

        boolean waitRepairQueBySrv = false;


        // Чиним неправильные состояния:
        //  - noQueIn001 < noQueIn001ReadSrv
        //  - noQueIn < noQueInReadSrv
        //  - noQueOut < noQueOutSendSrv
        // Читаем с сервера очереди queIn001, queIn, queIn001, queOut до тех пор,
        // пока не получим все, что получили до сбоя (реплики от noQue*** + 1 до noQue******Srv),
        // Пока чтение не закончится успехом - выкидываем ошибку (или ждем, если стоит флаг ожидания)
        do {
            boolean repairQueBySrv_doneOk_queIn001 = readQueFromSrv_Interval(queIn001, "to001", noQueIn001 + 1, noQueIn001ReadSrv);
            boolean repairQueBySrv_doneOk_queIn = readQueFromSrv_Interval(queIn, "to", noQueIn + 1, noQueInReadSrv);
            boolean repairQueBySrv_doneOk_queOut = readQueFromSrv_Interval(queOut, "from", noQueOut + 1, noQueOutSendSrv);

            if (repairQueBySrv_doneOk_queIn001 && repairQueBySrv_doneOk_queIn && repairQueBySrv_doneOk_queOut) {
                break;
            }

            if (waitRepairQueBySrv) {
                Thread.sleep(5000);
            } else {
                log.warn("doneOk queIn001: " + repairQueBySrv_doneOk_queIn001);
                log.warn("doneOk queIn: " + repairQueBySrv_doneOk_queIn);
                log.warn("doneOk queOut: " + repairQueBySrv_doneOk_queOut);
                throw new XError("Wait for readQueFromSrv_Interval");
            }

        } while (true);

        // Тут мы полностью получили то состояние очередей queIn001 и queIn, которую эта база читала последней,
        // и такое состояние QueOut, которую эта база отправляла последней.
        noQueIn001 = queIn001.getMaxNo();
        noQueIn = queIn.getMaxNo();
        noQueOut = queOut.getMaxNo();


        // ---
        // Добиваемся того, чтобы в очереди QueIn оказались и все ранее отправленные наши СОБСТВЕННЫЕ реплики.
        // Это нужно, чтобы после завершения ремонта пользователь увидел те данные, которые водил сам, до сбоя.
        //
        // Применение для восстановления СВОИХ данных queOut недопустимо, т.к. реплики queOut формировались вперемешку
        // с получением queIn и могут содержать ССЫЛКИ на значения, полученные через queIn (а сейчас их в базе пока нет).
        // Таким образом, просто применить только свой queOut - опасно.
        //
        // И наоборот, для восстановления всех своих данных достаточно дождаться ТОЛЬКО потока queIn,
        // в котором будут, в том числе, и ВСЕ наши данные.

        // Проверим, есть ли в queIn есть ВСЕ наши СОБСТВЕННЫЕ (исходящие) реплики до того номера,
        // который мы ранее (до сбоя) отправили на сервер.
        if (!repairMode_EmptyBase) {
            boolean needWait_selfReplica_queIn_fromSrv = false;
            long noSelfReplica_queIn = 0;
            long no = queIn.getMaxNo();
            while (no > 0) {
                IReplica replicaQueIn = queIn.get(no);
                //
                if (replicaQueIn.getInfo().getWsId() == wsId) {
                    noSelfReplica_queIn = replicaQueIn.getInfo().getNo();
                    needWait_selfReplica_queIn_fromSrv = noQueOutSendSrv > noSelfReplica_queIn;
                    break;
                }

                //
                no = no - 1;
            }

            // Если в нашей queIn наших СОБСТВЕННЫХ (ранее нами же отправленных из QueOut) реплик пока нет, то
            // запрашиваем и читаем queIn с сервера до тех пор, пока не получим собственную реплику нужного номера (т.е. номера noQueOutSendSrv)
            // Пока чтение не закончится успехом - выкидываем ошибку (или ждем, если стоит флаг ожидания)
            if (needWait_selfReplica_queIn_fromSrv) {
                do {
                    boolean repairQueBySrv_doneOk_queIn = readQueFromSrv_RepicaNo(queIn, "to", queIn.getMaxNo() + 1, noQueOutSendSrv);

                    if (repairQueBySrv_doneOk_queIn) {
                        break;
                    }

                    if (waitRepairQueBySrv) {
                        Thread.sleep(5000);
                    } else {
                        throw new XError("Wait for readQueFromSrv_RepicaNo");
                    }

                } while (true);
            }
        }

        // Тут мы полностью получили то состояние очереди queIn, которое позволит отремонтровать все данные.
        noQueIn = queIn.getMaxNo();


        // ---
        // Ремонт данных
        // ---


        // Чиним (восстанавливаем) данные на основе входящих реплик queIn001.
        handleQueIn001();

        // Проверяем, что все применили
        noQueIn001Used = stateManager.getQueNoDone("in001");
        if (noQueIn001Used != queIn001.getMaxNo()) {
            throw new XError("Use queIn001 - que is not completely used, noQueIn001Used: " + noQueIn001Used + ", queIn001.getMaxNo: " + queIn001.getMaxNo());
        }

        // ---
        // Среди входящих есть и НАШИ СОБСТВЕННЫЕ реплики, важно их применить именно сейчас, когда начат ремонт.
        // Иначе при применении входящей очереди в рамках обычной работы - не будет вызван ремонт генераторов,
        // а из-за наличия во входящей очереди НАШИХ СОБСТВЕННЫХ потерянных данных - генераторы перейдут в нецелостное состояние.
        // Чиним (восстанавливаем) данные на основе входящих реплик QueIn.
        handleQueIn(true);

        // Проверяем, что все применили
        noQueInUsed = stateManager.getQueNoDone("in");
        if (noQueInUsed != queIn.getMaxNo()) {
            throw new XError("Use queIn - que is not completely used, noQueInUsed: " + noQueInUsed + ", queIn.getMaxNo: " + queIn.getMaxNo());
        }


        if (!repairMode_EmptyBase) {

            // ---
            // Отслеживаем наш последний возраст age, встретившийся в НАШИХ СОБСТВЕННЫХ репликах при примененнии QueIn.
            // Ремонт отметки возраста ОБРАБОТАННОГО аудита делаем именно по нему
            long lastOwnAgeUsed = -1;
            long no00 = queIn.getMaxNo();
            while (no00 > 0) {
                IReplica replica = queIn.get(no00);
                //
                if (replica.getInfo().getWsId() == wsId) {
                    long age = replica.getInfo().getAge();
                    if (age != -1 && age > lastOwnAgeUsed) {
                        lastOwnAgeUsed = age;
                        break;
                    }
                }

                //
                no00 = no00 - 1;
            }


            // ---
            // Если имеющаяся исходящая очередь старше реплик, которые мы еще НЕ ОТПРАВЛЯЛИ на сервер, значит исходящая очередь
            // содержит реплики, которые мы НЕ ПРИМЕНЯЛИ в рамках ремонта данных путем применения QueIn.
            // Чиним (восстанавливаем) данные на основе исходящей очереди QueOut.
            int count = 0;
            for (long no1 = noQueOutSendSrv + 1; no1 <= noQueOut; no1++) {
                log.warn("Use queOut, self.wsId: " + wsId + ", queOut.no: " + no1 + " (" + count + "/" + (noQueOut - noQueOutSendSrv) + ")");

                // Извлекаем собственную реплику из закромов queOut
                IReplica replica = queOut.get(no1);

                // Отслеживаем наш последний возраст age, встретившийся в НАШИХ СОБСТВЕННЫХ репликах при примененнии QueOut.
                // Ремонт отметки возраста ОБРАБОТАННОГО аудита делаем именно по нему
                long age = replica.getInfo().getAge();
                if (age != -1 && age > lastOwnAgeUsed) {
                    lastOwnAgeUsed = age;
                }

                // Пробуем применить собственную реплику
                ReplicaUseResult useResult = useReplicaInternal(replica, true);

                //
                if (!useResult.replicaUsed) {
                    throw new XError("Use queOut, useResult.replicaUsed == false");
                }
                if (useResult.doBreak) {
                    throw new XError("Use queOut, replica useResult.doBreak == true");
                }
                log.warn("Use queOut, used: " + no1);


                //
                count = count + 1;
            }

            //
            if (count > 0) {
                log.warn("Use queOut, self.wsId: " + wsId + ", queOut: " + (noQueOutSendSrv + 1) + " .. " + noQueOut + ", done count: " + count);
            } else {
                log.info("Use queOut, self.wsId: " + wsId + ", queOut: " + noQueOut + ", nothing to do");
            }


            // ---
            // Тут мы полностью получили такую базу, какой она была на момент отправки последних своих данных.
            // Можно чинить генераторы, отметки разных возрастов и т.п.
            // ---


            // ---
            // Чиним отметки
            // ---


            // ---
            // Если отмечено "отправлено на сервер" не совпадает с фактической отправкой на сервер.
            if (noQueOutSendMarked != noQueOutSendSrv) {
                if (noQueOutSendMarked < noQueOutSendSrv) {
                    // Отметка отстает от сервера
                    // В отличие от процедуры ремонта repairSendTaskBySrvState тут можно передвинуть вперед -
                    // ведь мы отремонтировали очередь до уровня noQueOutSendSrv.
                    // Просто исправляем отметку "отправлено на сервер".
                    mailStateManager.setMailSendDone(noQueOutSendSrv);
                    log.warn("Repair noQueOutSendMarked != noQueOutSendSrv, setMailSendDone, " + noQueOutSendMarked + " -> " + noQueOutSendSrv);
                } else {
                    // Отметка станции опережает отметку почтового сервера.
                    // Не ошибка
                    log.info("Unable to repair marked, noQueSendMarked > noQueSendSrv, noQueOutSendMarked: " + noQueOutSendMarked + ", noQueOutSendSrv: " + noQueOutSendSrv);
                }
            }


            // ---
            // До какого возраста обработана очередь QueIn (noQueInUsed) - нет необходимости чинить,
            // т.к. она уже сдвинута вызовом handleQueIn


            // ---
            // Исправление отметок аудита
            // ---

            // После ремонта данных применением собственных реплик из очередей QueIn и QueOut
            // аудит таблиц пуст, а отметка возраста аудита ("возраст age" для таблиц аудита) все ещё содержит устаревшее состояние.
            // Чиним отметку возраста аудита.
            UtAuditAgeManager auditManager = new UtAuditAgeManager(db, struct);
            long ageNow = auditManager.getAuditAge();
            if (ageNow < lastOwnAgeUsed) {
                auditManager.setAuditAge(lastOwnAgeUsed);
                log.warn("Repair auditAge, " + ageNow + " -> " + lastOwnAgeUsed);
            }

            // После применения собственных реплик из очередей QueIn и QueOut отметка возраста ОБРАБОТАННОГО аудита
            // (до какого возраста аудит отмечен как выложенный в очередь QueOut) все ещё содержит устаревшее состояние.
            // Чиним отметку возраста обработанного аудита.
            long ageQueOutDoneNow = stateManager.getAuditAgeDoneQueOut();
            if (ageQueOutDoneNow < lastOwnAgeUsed) {
                stateManager.setAuditAgeDoneQueOut(lastOwnAgeUsed);
                log.warn("Repair ageQueOutDone, " + ageQueOutDoneNow + " -> " + lastOwnAgeUsed);
            }
        }


        // ---
        // После применения собственных реплик генераторы находятся в устаревшем состоянии.
        // Чиним генераторы.
        UtPkGeneratorRepair generatorRepair = new UtPkGeneratorRepair(db, struct);
        generatorRepair.repairGenerators();


        // ---
        // Убираем отметку "ремонт начат".
        // После этого ремонт считается завершенным.
        repairLockFileManager.repairLockFileDelete();
        repairInfoManager.setNoRepair();

        //
        log.warn("Restore from backup: repair done");
        log.warn("----------");
    }

    private boolean isReplicaInBox(String box, long no) throws Exception {
        try {
            mailer.getReplicaInfo(box, no);
            return true;
        } catch (Exception e) {
            if (UtJdxErrors.errorIs_replicaMailNotFound(e)) {
                return false;
            } else {
                throw e;
            }
        }
    }

    /**
     * Читаем в очередь que с сервера реплики в диапазоне от replicaNoFrom до replicaNoTo.
     * Если надо - заказываем у сервера повторную передачу.
     *
     * @return =true, если все заказанные реплики прочитаны с сервера
     */
    private boolean readQueFromSrv_Interval(IJdxQue que, String box, long replicaNoFrom, long replicaNoTo) throws Exception {
        if (replicaNoFrom > replicaNoTo) {
            // Запрошен пустой диапазон
            return true;
        }

        //
        String queName = que.getQueName();
        log.info("readQueFromSrv_Interval, receive, que: " + queName + ", box: " + box + ", replicaNoFrom: " + replicaNoFrom + ", replicaNoTo: " + replicaNoTo);

        // Если в ящике нет того, что нам нужно - попросим сразу прислать всесь диапазон, который мы намерены скачивать
        if (!isReplicaInBox(box, replicaNoFrom) || !isReplicaInBox(box, replicaNoTo)) {
            if (replicaNoFrom == 1) {
                // Запрос с самого первого номера - не разрешаем (это слишком много)
                log.warn("readQueFromSrv_Interval, not valid replicaNoFrom == 1, que: " + queName + ", box: " + box);
            } else {
                // Просим диапазон
                RequiredInfo requiredInfo = new RequiredInfo();
                requiredInfo.executor = RequiredInfo.EXECUTOR_SRV;
                requiredInfo.requiredFrom = replicaNoFrom;
                requiredInfo.requiredTo = replicaNoTo;
                mailer.setSendRequired(box, requiredInfo);
            }
        }

        // Читаем с сервера
        long no = replicaNoFrom;
        while (no <= replicaNoTo) {
            try {
                log.info("readQueFromSrv_Interval, receive, que: " + queName + ", box: " + box + ", no: " + no);

                //
                receiveInternalStep(mailer, box, no, que);

                //
                no++;
            } catch (Exception e) {
                log.warn("readQueFromSrv_Interval, error: " + e.getMessage());
                return false;
            }
        }

        //
        return true;
    }

    /**
     * Читаем в очередь que с сервера реплики, пока не встретим СОБСТВЕННУЮ реплику с номером не менее requiredReplicaSelfQueNo.
     * Если надо - заказываем у сервера повторную передачу.
     *
     * @return =true, если все заказанные реплики прочитаны с сервера
     */
    private boolean readQueFromSrv_RepicaNo(IJdxQue que, String box, long replicaNoFrom, long noSelfReplica_required) throws Exception {
        String queName = que.getQueName();
        log.info("readQueFromSrv_RepicaNo, receive, que: " + queName + ", box: " + box + ", replicaNoFrom: " + replicaNoFrom + ", noSelfReplica_required: " + noSelfReplica_required);

        // Если в ящике нет того, что нам нужно - попросим сразу прислать всесь диапазон, который мы намерены скачивать
        if (!isReplicaInBox(box, replicaNoFrom)) {
            RequiredInfo requiredInfo = new RequiredInfo();
            requiredInfo.executor = RequiredInfo.EXECUTOR_SRV;
            requiredInfo.requiredFrom = replicaNoFrom;
            requiredInfo.requiredTo = -1;
            mailer.setSendRequired(box, requiredInfo);
        }

        //
        long noSelfReplica = 0;
        long no = replicaNoFrom;
        while (noSelfReplica < noSelfReplica_required) {
            try {
                log.info("readQueFromSrv_RepicaNo, receive, que: " + queName + ", box: " + box + ", no: " + no);

                //
                IReplica replica = receiveInternalStep(mailer, box, no, que);

                //
                if (replica.getInfo().getWsId() == wsId) {
                    noSelfReplica = replica.getInfo().getNo();
                }

                //
                no++;
            } catch (Exception e) {
                log.warn("readQueFromSrv_RepicaNo, error: " + e.getMessage());
                return false;
            }
        }

        //
        return true;
    }

    public void wsCreateSnapshot(String tableNames, String outName) throws Exception {
        // Разложим в список
        List<IJdxTable> tables = UtJdx.selectTablesByName(tableNames, struct);

        // Создаем снимок для таблиц (разрешаем отсылать чужие записи)
        UtRepl utRepl = new UtRepl(db, struct);
        List<IReplica> replicasRes = utRepl.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOut);

        // Отправляем снимки таблиц в файл(ы)
        if (replicasRes.size() == 1) {
            IReplica replica = replicasRes.get(0);
            File resFile = new File(outName);
            FileUtils.moveFile(replica.getData(), resFile);
            log.info(resFile.getAbsolutePath());
        } else {
            // В tables будет соблюден порядок сортировки таблиц с учетом foreign key.
            // При наиименовании файлов важен порядок.
            tables = UtJdx.sortTablesByReference(tables);
            //
            for (int i = 0; i < replicasRes.size(); i++) {
                IReplica replica = replicasRes.get(i);
                File resFile = new File(outName + "/" + UtString.padLeft(String.valueOf(i), 3, '0') + "." + tables.get(i).getName() + ".zip");
                FileUtils.moveFile(replica.getData(), resFile);
                log.info(resFile.getAbsolutePath());
            }
        }
    }

    /**
     * Отправляет shapshot таблиц в общую очередь queOut
     *
     * @param tableNames Список таблиц через запятую
     */
    public void wsSendSnapshot(String tableNames) throws Exception {
        // Разложим в список
        List<IJdxTable> tables = UtJdx.selectTablesByName(tableNames, struct);

        // Создаем снимок для таблиц (разрешаем отсылать чужие записи)
        UtRepl utRepl = new UtRepl(db, struct);
        List<IReplica> replicasRes = utRepl.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOut);

        // Отправляем снимки таблиц в очередь queOut
        utRepl.sendToQue(replicasRes, queOut);
    }

    public void debugDumpStruct(String prefix, IJdxDbStruct struct) throws Exception {
        prefix = "ws_" + wsId + "-" + prefix;
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        struct_rw.toFile(struct, dataRoot + "temp/" + prefix + ".xml");
        UtFile.saveString(UtDbComparer.getDbStructCrcTables(struct), new File(dataRoot + "temp/" + prefix + ".crc"));
    }

    public void debugDumpStruct(String prefix) throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        struct_rw.toFile(struct, dataRoot + "temp/" + prefix + "dbStruct.actual.xml");
        struct_rw.toFile(structAllowed, dataRoot + "temp/" + prefix + "dbStruct.allowed.xml");
        struct_rw.toFile(structFixed, dataRoot + "temp/" + prefix + "dbStruct.fixed.xml");
        struct_rw.toFile(structFull, dataRoot + "temp/" + prefix + "dbStruct.full.xml");
        UtFile.saveString(UtDbComparer.getDbStructCrcTables(struct), new File(dataRoot + "temp/" + prefix + "dbStruct.actual.crc"));
        UtFile.saveString(UtDbComparer.getDbStructCrcTables(structAllowed), new File(dataRoot + "temp/" + prefix + "dbStruct.allowed.crc"));
        UtFile.saveString(UtDbComparer.getDbStructCrcTables(structFixed), new File(dataRoot + "temp/" + prefix + "dbStruct.fixed.crc"));
        UtFile.saveString(UtDbComparer.getDbStructCrcTables(structFull), new File(dataRoot + "temp/" + prefix + "dbStruct.full.crc"));
    }

}
