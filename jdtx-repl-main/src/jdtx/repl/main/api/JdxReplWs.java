package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.utils.io.*;
import jandcode.utils.variant.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.database_info.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.pk_generator.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.rec_merge.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.task.*;
import org.apache.commons.io.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.joda.time.*;
import org.json.simple.*;

import java.io.*;
import java.sql.*;
import java.util.*;


/**
 * Контекст рабочей станции
 */
public class JdxReplWs {

    //
    private long MAX_COMMIT_RECS = 10000;

    // Правила публикации
    protected IPublicationRuleStorage publicationIn;
    protected IPublicationRuleStorage publicationOut;

    //
    protected IJdxQue queIn;
    protected IJdxQue queIn001;
    protected IJdxQue queOut;

    //
    private Db db;
    protected long wsId;
    protected String wsGuid;
    protected IJdxDbStruct struct;
    protected IJdxDbStruct structAllowed;
    protected IJdxDbStruct structFixed;
    protected String databaseInfo;

    // Параметры приложения
    public IVariantMap appCfg;

    //
    private IMailer mailer;

    //
    protected String dataRoot;

    //
    public JdxErrorCollector errorCollector = null;

    //
    private static Log log = LogFactory.getLog("jdtx.Workstation");

    //
    public JdxReplWs(Db db) throws Exception {
        this.db = db;

        // Строго обязательно REPEATABLE_READ, иначе сохранение в age возраста аудита
        // будет не синхронно с изменениями в таблицах аудита.
        db.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    public long getWsId() {
        return wsId;
    }

    public String getWsGuid() {
        return wsGuid;
    }

    public IMailer getMailer() {
        return mailer;
    }

    /**
     * Рабочая станция, запуск
     */
    public void init() throws Exception {
        MDC.put("serviceName", "ws");

        //
        dataRoot = new File(db.getApp().getRt().getChild("app").getValueString("dataRoot")).getCanonicalPath();
        dataRoot = UtFile.unnormPath(dataRoot) + "/";
        log.info("dataRoot: " + dataRoot);

        // Проверка версии служебных структур в БД
        UtDbObjectManager ut = new UtDbObjectManager(db);
        ut.checkReplVerDb();

        // Проверка, что инициализация станции прошла
        ut.checkReplDb();

        // Чтение структуры БД
        IJdxDbStructReader structReader = new JdxDbStructReader();
        structReader.setDb(db);
        IJdxDbStruct structActual = structReader.readDbStruct();

        // Читаем код нашей станции
        readIdGuid();

        // Чтение конфигурации
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgWs = cfgManager.getSelfCfg(CfgType.WS);
        JSONObject cfgPublications = cfgManager.getSelfCfg(CfgType.PUBLICATIONS);
        JSONObject cfgDecode = cfgManager.getSelfCfg(CfgType.DECODE);

        // Параметры приложения
        appCfg = loadAppCfg((JSONObject) cfgWs.get("app"));

        // Рабочие каталоги
        String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
        String mailLocalDirTmp = dataRoot + "temp/";

        // Читаем из общей очереди
        queIn = new JdxQueCommon(db, UtQue.QUE_IN, UtQue.STATE_AT_WS);
        String queIn_DirLocal = dataRoot + "ws_" + sWsId + "/que_in";
        queIn.setDataRoot(queIn_DirLocal);

        // Читаем из очереди 001
        queIn001 = new JdxQueCommon(db, UtQue.QUE_IN001, UtQue.STATE_AT_WS);
        String queIn001_DirLocal = dataRoot + "ws_" + sWsId + "/que_in001";
        queIn001.setDataRoot(queIn001_DirLocal);

        // Пишем в эту очередь
        queOut = new JdxQuePersonal(db, UtQue.QUE_OUT, wsId);
        String queOut_DirLocal = dataRoot + "ws_" + sWsId + "/que_out";
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

        // Стратегии перекодировки каждой таблицы
        RefDecodeStrategy.initInstance(cfgDecode);

        // Правила публикаций
        this.publicationIn = PublicationRuleStorage.loadRules(cfgPublications, structActual, "in");
        this.publicationOut = PublicationRuleStorage.loadRules(cfgPublications, structActual, "out");


        // Фильтрация структуры: убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut
        struct = UtRepl.getStructCommon(structActual, this.publicationIn, this.publicationOut);


        //
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);
        structAllowed = databaseStructManager.getDbStructAllowed();
        structFixed = databaseStructManager.getDbStructFixed();

        //
        DatabaseInfoReaderService svc = db.getApp().service(DatabaseInfoReaderService.class);
        IDatabaseInfoReader databaseInfoReader = svc.createDatabaseInfoReader(db, struct);
        databaseInfo = databaseInfoReader.readDatabaseVersion();

        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
    }

    /**
     * Читаем код нашей станции.
     * Оформлен как отдельный метод, чтобы можно было вызывать только его
     * из jdtx.repl.main.service.UtReplService#remove(), без инициализации и смены версии БД.
     */
    public void readIdGuid() throws Exception {
        DataRecord rec = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO").getCurRec();
        this.wsId = rec.getValueLong("ws_id");
        //
        this.wsGuid = rec.getValueString("guid");
        // Проверяем код нашей станции
        if (this.wsId == 0) {
            throw new XError("Invalid workstation.ws_id == 0");
        }
    }

    private IVariantMap loadAppCfg(JSONObject app) {
        //
        IVariantMap res = new VariantMap();

        //
        if (app != null) {
            res.put("autoUseRepairReplica", UtJdxData.booleanValueOf(app.get("autoUseRepairReplica"), false));
            res.put("skipForeignKeyViolationIns", UtJdxData.booleanValueOf(app.get("skipForeignKeyViolationIns"), false));
            res.put("skipForeignKeyViolationUpd", UtJdxData.booleanValueOf(app.get("skipForeignKeyViolationUpd"), false));
        }

        //
        return res;
    }

    /**
     * Рабочая станция, инициализация окружения
     */
    public void initFirst() {
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
    public void checkAppUpdate() throws Exception {
        String appRoot = new File(db.getApp().getRt().getChild("app").getValueString("appRoot")).getCanonicalPath();
        UtAppUpdate ut = new UtAppUpdate(db, appRoot);
        ut.checkAppUpdate(true);
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

        // Создаем реплику
        IReplica replicaSnapshot;
        //
        db.startTran();
        try {
            // Создаем реплику
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
     * Выполнение фиксации структуры:
     * - дополнение аудита
     * - "реальная" структура запоминается как "зафиксированная"
     *
     * @return true, если структуры были обновлены или не требуют обновления.
     */
    public boolean dbStructApplyFixed(boolean sendSnapshotForNewTables) throws Exception {
        log.info("dbStructApplyFixed, checking");

        // Читаем структуры
        IJdxDbStruct structActual = struct;
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);
        IJdxDbStruct structFixed = databaseStructManager.getDbStructFixed();
        IJdxDbStruct structAllowed = databaseStructManager.getDbStructAllowed();

        // Сравниваем
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();
        //
        boolean equal_Actual_Allowed = UtDbComparer.dbStructIsEqualTables(structActual, structAllowed);
        boolean equal_Actual_Fixed = UtDbComparer.getStructDiffTables(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        // Нет необходимости в фиксации структуры -
        // все структуры совпадают (до таблиц)
        if (equal_Actual_Allowed && equal_Actual_Fixed) {
            log.info("dbStructApplyFixed, no diff found, Actual == Allowed == Fixed");

            // Для справки/отладки - структуры в файл
            debugDumpStruct("1.");

            //
            return true;
        }

        // Нет возможности фиксации структуры -
        // реальная не совпадает с разрешенной
        if (!equal_Actual_Allowed) {
            log.warn("dbStructApplyFixed, Actual <> Allowed");

            // Для справки/отладки - структуры в файл
            debugDumpStruct("2.");

            //
            return false;
        }

        // Начинаем фиксацию структуры -
        // реальная совпадает с разрешенной, но отличается от зафиксированной
        log.info("dbStructApplyFixed, start");

        // Обеспечиваем порядок сортировки таблиц с учетом foreign key (при применении структуры будем делать snapsot, там важен порядок)
        List<IJdxTable> tablesNew = UtJdx.sortTablesByReference(structDiffNew.getTables());


        // Подгоняем структуру аудита под реальную структуру
        db.startTran();
        try {
            //
            UtDbObjectManager objectManager = new UtDbObjectManager(db);

            //
            long n;

            // Удаляем аудит для удаленных таблиц
            List<IJdxTable> tablesRemoved = structDiffRemoved.getTables();
            n = 0;
            for (IJdxTable table : tablesRemoved) {
                n++;
                log.info("  dropAudit " + n + "/" + tablesRemoved.size() + " " + table.getName());
                //
                objectManager.dropAudit(table.getName());
            }

            // Создаем аудит для новых таблиц
            n = 0;
            for (IJdxTable table : tablesNew) {
                n++;
                log.info("  createAudit " + n + "/" + tablesNew.size() + " " + table.getName());

                //
                if (UtRepl.tableSkipRepl(table)) {
                    log.info("  createAudit, skip not found in tableSkipRepl, table: " + table.getName());
                    continue;
                }

                // Создание таблицы аудита
                objectManager.createAuditTable(table);

                // Создание тригеров
                objectManager.createAuditTriggers(table);
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }


        // Делаем выгрузку snapshot, обязательно в ОТДЕЛЬНОЙ транзакции (отдельной от изменения структуры).
        // В некоторых СУБД (напр. Firebird) изменение структуры происходит ВНУТРИ транзакций,
        // тогда получится, что пока делается snapshot, аудит не работает.
        // Таким образом данные, вводимые во время подготовки аудита и snapshot-та, не попадут ни в аудит, ни в snapshot,
        // т.к. таблица аудита ЕЩЕ не видна другим транзакциям, а данные, продолжающие поступать в snapshot, УЖЕ не видны нашей транзакции.
        if (sendSnapshotForNewTables) {
            UtRepl ut = new UtRepl(db, struct);
            List<IReplica> replicasRes;

            // Делаем выгрузку snapshot (в отдельной транзакции)
            db.startTran();
            try {
                // Параметры (для правил публикации и фильтрации): автор и получатель реплики реплики - wsId
                replicasRes = ut.createSnapshotForTablesFiltered(tablesNew, wsId, wsId, publicationOut, true);

                //
                db.commit();
            } catch (Exception e) {
                db.rollback(e);
                throw e;
            }

            // Отправляем snapshot
            ut.sendToQue(replicasRes, queOut);
        } else {
            log.info("dbStructApplyFixed, snapshot not send");
        }


        //
        log.info("dbStructApplyFixed, complete");

        //
        return true;
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
            long auditAgeFrom = stateManager.getAgeQueOutDone();

            //
            long queAuditState = 0;
            if (queAuditState >= auditAgeFrom) {
                // todo
                // У нас выложено в аудит больше, чем есть у нас.
                // Это говорит от том, что наша база восстановлена из старой копии, и успела наотправлять реплик НА СЕРВЕР

                // todo запретить создание реплики, если ее ФАЙЛ уже есть,
                // это говорит о том, что наша база восстановлена из старой копии, и успела наготовить реплик в ЛОКАЛЬНЫЕ ФАЙЛЫ
            }
            // todo запретить передавать то, что уже есть физически в ящике. команда "повтори передачу" должна расчитвать, что файлов в ящике уже НЕТ

            // todo сделать команду "повтор приготовления реплик", для случая, если реплику съел вирус или иных проблем с ЛОКАЛЬНЫМ файлом,
            // команда включает в себя команду "повторно отправить реплики mailer.getSendRequired()"

            //
            long count = 0;
            for (long age = auditAgeFrom + 1; age <= auditAgeTo; age++) {
                IReplica replica = auditSelector.createReplicaFromAudit(publicationOut, age);

                // Пополнение исходящей очереди реплик
                queOut.push(replica);

                // Отметка об обработке аудита в исходящую очередь реплик
                stateManager.setAgeQueOutDone(age);

                //
                count++;
            }

            //
            if (count > 0) {
                log.info("handleSelfAudit, wsId: " + wsId + ", audit.age: " + auditAgeFrom + " .. " + auditAgeTo + ", done count: " + count);
            } else {
                log.info("handleSelfAudit, wsId: " + wsId + ", audit.age: " + auditAgeFrom + ", nothing to do");
            }


            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

    }


    /**
     * Пересоздает реплику заново.
     * todo Пока не используется, на будещее
     *
     * @param age Для какого возраста
     * @return Пересозданная реплика
     */
    public IReplica recreateQueOutReplicaAge(long age) throws Exception {
        log.info("recreateQueOutReplica, age: " + age);

        // Можем пересозать только по аудиту JdxReplicaType.IDE
        IReplicaInfo replicaInfo = queOut.get(age).getInfo();
        if (replicaInfo.getReplicaType() != JdxReplicaType.IDE) {
            throw new XError("Реплику невозможно пересоздать, age:" + age + ", replicaType: " + replicaInfo.getReplicaType());
        }


        // Формируем реплику заново
        UtAuditSelector auditSelector = new UtAuditSelector(db, struct, wsId);
        IReplica replicaRecreated = auditSelector.createReplicaFromAudit(publicationOut, age);

        // Копируем реплику на место старой
        IReplica replicaOriginal = queOut.get(age);
        File replicaOriginalFile = replicaOriginal.getData();

        log.info("Original replica file: " + replicaOriginalFile.getAbsolutePath());
        log.info("Original replica size: " + replicaOriginalFile.length());
        log.info("Recreated replica file: " + replicaRecreated.getData().getAbsolutePath());
        log.info("Recreated replica size: " + replicaRecreated.getData().length());

        FileUtils.forceDelete(replicaOriginalFile);
        FileUtils.copyFile(replicaRecreated.getData(), replicaOriginalFile);
        FileUtils.forceDelete(replicaRecreated.getData());

        //
        return replicaOriginal;
    }


    private ReplicaUseResult handleQue(IJdxReplicaStorage que, String queName, long queNoFrom, long queNoTo, boolean forceUse) throws Exception {
        log.info("handleQue: " + queName + ", self.wsId: " + wsId + ", que.name: " + ((JdxQue) que).getQueName() + ", que: " + queNoFrom + " .. " + queNoTo);

        //
        ReplicaUseResult handleQueUseResult = new ReplicaUseResult();

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        long count = 0;
        for (long no = queNoFrom; no <= queNoTo; no++) {
            log.info("handleQue: " + queName + ", self.wsId: " + wsId + ", que.no: " + no + " (count: " + count + "/" + (queNoTo - queNoFrom) + ")");

            //
            IReplica replica = que.get(no);

            // Пробуем применить реплику
            ReplicaUseResult replicaUseResult;
            try {
                replicaUseResult = useReplicaInternal(replica, forceUse);
            } catch (Exception e) {
                // Пробуем что-то сделать с проблемой применения реплики
                log.error("handleQue: " + e.getMessage());
                handleError_UseReplica(que, queName, no, mailer, e);
                break;
            }

            if (replicaUseResult.replicaUsed && replicaUseResult.lastOwnAgeUsed > handleQueUseResult.lastOwnAgeUsed) {
                handleQueUseResult.lastOwnAgeUsed = replicaUseResult.lastOwnAgeUsed;
            }

            // Реплика использованна?
            if (replicaUseResult.replicaUsed) {
                // Отметим применение реплики
                // Отметку двигаем только вперёд. Это важно учитывать, т.к. бывает ситуация "восстановление базы станции по данным с сервера",
                // в рамках которых может прийти реплика на изменение возраста очередей.
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

    private void handleError_UseReplica(IJdxReplicaStorage que, String queName, long no, IMailer mailer, Exception exceptionUse) throws Exception {
        if (UtJdxErrors.errorIs_replicaUsedBadCrc(exceptionUse) || UtJdxErrors.errorIs_replicaFileNotExists(exceptionUse) || UtJdxErrors.errorIs_replicaNotFoundContent(exceptionUse)) {
            String box;
            switch (queName) {
                case UtQue.QUE_IN:
                    box = "to";
                    break;
                case UtQue.QUE_IN001:
                    box = "to001";
                    break;
                default:
                    throw new XError("handleUseError, unknown mailer.box for que.name: " + queName);
            }

            try {
                // Проверим, есть ли такая реплика в ящике (еще осталась или запросили на предыдущем цикле)
                IReplicaInfo info = mailer.getReplicaInfo(box, no);

                //
                log.info("handleError_UseReplica, try replica receive, replica.no: " + no + ", box: " + box);

                // Скачиваем
                IReplica replica = mailer.receive(box, no);

                // Проверяем целостность скачанного
                UtJdx.checkReplicaCrc(replica, info.getCrc());

                // Читаем заголовок
                JdxReplicaReaderXml.readReplicaInfo(replica);

                // Обновим "битую" реплику - заменим на нормальную
                String actualFileName = JdxStorageFile.getFileName(no);
                File actualFile = new File(((JdxStorageFile) que).getBaseDir() + actualFileName);
                if (actualFile.exists() && !actualFile.delete()) {
                    throw new XError("handleError_UseReplica, unable to delete bad replica: " + actualFile.getAbsolutePath());
                }
                //
                que.put(replica, no);

                // Удаляем с почтового сервера
                mailer.delete(box, no);

                //
                log.info("handleError_UseReplica, replica receive done");
                throw new XError("handleError_UseReplica, replica receive done, replica.no: " + no + ", box: " + box);
            } catch (Exception exceptionMail) {
                if (UtJdxErrors.errorIs_MailerReplicaNotFound(exceptionMail)) {
                    log.info("handleError_UseReplica, try request replica: " + no + ", box: " + box);
                    // Реплику еще не просили - попросим прислать
                    SendRequiredInfo requiredInfo = new SendRequiredInfo();
                    requiredInfo.requiredFrom = no;
                    requiredInfo.requiredTo = no;
                    // Просим
                    mailer.setSendRequired(box, requiredInfo);

                    //
                    log.info("handleError_UseReplica, request done");
                    throw new XError("handleError_UseReplica, request done, replica.no: " + no + ", box: " + box);
                } else {
                    throw exceptionMail;
                }
            }

        } else {
            throw exceptionUse;
        }
    }

    public ReplicaUseResult useReplicaFile(File f) throws Exception {
        return useReplicaFile(f, true);
    }

    private ReplicaUseResult useReplicaFile(File f, boolean forceApplySelf) throws Exception {
        log.info("useReplicaFile, file: " + f.getAbsolutePath());

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
            // Последняя обработка собственного аудита
            handleSelfAudit();

            // Переход в состояние "Я замолчал"
            JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
            muteManager.muteWorkstation();

            // Выкладывание реплики "Я замолчал"
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

            // Выход из состояния "Я замолчал"
            JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
            muteManager.unmuteWorkstation();

            // Отчитаемся
            reportReplica(JdxReplicaType.UNMUTE_DONE);
        }
    }

    /**
     * Реакция на команду - SET_STATE
     */
    private void useReplica_SET_STATE(IReplica replica) throws Exception {
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
                long queOutNoDoneNow = stateManager.getAgeQueOutDone();
                if (wsState.QUE_OUT_NO_DONE > queOutNoDoneNow) {
                    //stateManager.setQueNoDone("out", wsState.QUE_OUT_NO_DONE);
                    stateManager.setAgeQueOutDone(wsState.QUE_OUT_NO_DONE);
                }

                // --- MAIL_SEND_DONE
                JdxMailStateManagerWs mailStateManagerWs = new JdxMailStateManagerWs(db);
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
                if (wsState.MUTE == 1) {
                    // Последняя обработка собственного аудита
                    handleSelfAudit();

                    // Переход в состояние "Я замолчал"
                    muteManager.muteWorkstation();
                } else {
                    // Выход из состояния "Я замолчал"
                    muteManager.unmuteWorkstation();
                }

                //
                db.commit();
            } catch (Exception e) {
                db.rollback(e);
                throw e;
            }

            // --- Отчитаемся
            reportReplica(JdxReplicaType.SET_STATE_DONE);
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

            // Чиним генераторы.
            PkGenerator pkGenerator = new PkGenerator_PS(db, struct);
            pkGenerator.repairGenerators();

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
            UtRepl ut = new UtRepl(db, struct);
            List<IReplica> replicasRes = ut.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOut, false);

            // Отправляем снимок таблицы в очередь queOut
            ut.sendToQue(replicasRes, queOut);

            // Выкладывание реплики "snapshot отправлен"
            reportReplica(JdxReplicaType.SEND_SNAPSHOT_DONE);
        }
    }

    /**
     * Реакция на команду - задать "разрешенную" структуру БД
     */
    private void useReplica_SET_DB_STRUCT(IReplica replica, ReplicaUseResult useResult) throws Exception {
        // Узнаем параметры команды: надо ли отправлять snapshot после добавления новых таблиц
        JSONObject info;
        InputStream infoStream = JdxReplicaReaderXml.createInputStream(replica, "info.json");
        try {
            String cfgStr = loadStringFromSream(infoStream);
            info = UtRepl.loadAndValidateJsonStr(cfgStr);
        } finally {
            infoStream.close();
        }
        boolean sendSnapshot = UtJdxData.booleanValueOf(info.get("sendSnapshot"), true);

        //
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);

        // В этой реплике - новая "разрешенная" структура
        InputStream stream = JdxReplicaReaderXml.createInputStreamData(replica);
        try {
            JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
            IJdxDbStruct struct = struct_rw.read(stream);
            // Запоминаем "разрешенную" структуру БД
            databaseStructManager.setDbStructAllowed(struct);
        } finally {
            stream.close();
        }

        // Проверяем серьезность измемения структуры и необходимость пересоздавать аудит
        if (!dbStructApplyFixed(sendSnapshot)) {
            // Если пересоздать аудит не удалось (структуры не обновлены или по иным причинам),
            // то не метим реплику как использованную
            log.warn("handleQueIn, dbStructApplyFixed <> true");
            useResult.replicaUsed = false;
            useResult.doBreak = true;
            return;
        }

        // Запоминаем текущую структуру БД как "фиксированную" структуру
        databaseStructManager.setDbStructFixed(struct);

        // Выкладывание реплики "структура принята"
        reportReplica(JdxReplicaType.SET_DB_STRUCT_DONE);

        // Перечитывание собственной structAllowed, structFixed
        this.structAllowed = databaseStructManager.getDbStructAllowed();
        this.structFixed = databaseStructManager.getDbStructFixed();

    }

    /**
     * Реакция на команду - "задать конфигурацию"
     */
    private void useReplica_SET_DB_CFG(IReplica replica, ReplicaUseResult useResult) throws Exception {
        // В этой реплике - данные о новой конфигурации
        JSONObject cfgInfo;
        InputStream cfgInfoStream = JdxReplicaReaderXml.createInputStream(replica, "cfg.info.json");
        try {
            String cfgInfoStr = loadStringFromSream(cfgInfoStream);
            cfgInfo = UtRepl.loadAndValidateJsonStr(cfgInfoStr);
        } finally {
            cfgInfoStream.close();
        }

        // Данные о новой конфигурации
        long destinationWsId = UtJdxData.longValueOf(cfgInfo.get("destinationWsId"));
        String cfgType = (String) cfgInfo.get("cfgType");

        // Пришла конфигурация для нашей станции (или всем станциям)?
        if (destinationWsId == 0 || destinationWsId == wsId) {
            // В этой реплике - новая конфигурация
            JSONObject cfg;
            InputStream cfgStream = JdxReplicaReaderXml.createInputStream(replica, "cfg.json");
            try {
                String cfgStr = loadStringFromSream(cfgStream);
                cfg = UtRepl.loadAndValidateJsonStr(cfgStr);
            } finally {
                cfgStream.close();
            }

            // Обновляем конфиг в своей таблице
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setSelfCfg(cfg, cfgType);

            // Выкладывание реплики "конфигурация принята"
            reportReplica(JdxReplicaType.SET_CFG_DONE);

            // Обновление конфигурации требует переинициализацию репликатора, поэтому обработка входящих реплик прерывается
            useResult.doBreak = true;
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
    private void useReplica_IDE_SNAPSHOT(IReplica replica, boolean forceApplySelf) throws Exception {
        int replicaType = replica.getInfo().getReplicaType();

        // Совпадает ли реальная структура БД с разрешенной структурой
        boolean isEqualStruct_Actual_Allowed = true;
        if (!UtDbComparer.dbStructIsEqual(struct, structAllowed)) {
            isEqualStruct_Actual_Allowed = false;
        }


        // Реальная структура базы НЕ совпадает с разрешенной структурой
        if (!isEqualStruct_Actual_Allowed) {
            // Для справки/отладки - не совпадающие структуры - в файл
            debugDumpStruct("5.");
            // Генерим ошибку
            throw new XError("handleQueIn, structActual <> structAllowed");
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
            //
            throw new XError("handleQueIn, database.structCrc <> replica.structCrc, expected: " + dbStructActualCrc + ", actual: " + replicaStructCrc);
        }

        // todo: Проверим протокол репликатора, с помощью которого была подготовлена реплика
        // String protocolVersion = (String) replica.getInfo().getProtocolVersion();
        // if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
        //      throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
        //}


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
        UtAuditApplyer auditApplyer = new UtAuditApplyer(db, struct, wsId);
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
            IJdxDataSerializer dataSerializer = new JdxDataSerializerDecode(db, wsId);
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
                throw new XError("useReplicaInternal: " + UtJdxErrors.message_replicaFileNotExists);
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
                useReplica_SET_STATE(replica);
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
                useReplica_SET_DB_CFG(replica, useResult);
                break;
            }

            case JdxReplicaType.MERGE: {
                useReplica_MERGE(replica);
                break;
            }

            case JdxReplicaType.IDE:
            case JdxReplicaType.IDE_MERGE:
            case JdxReplicaType.SNAPSHOT: {
                useReplica_IDE_SNAPSHOT(replica, forceApplySelf);
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
    public void handleQueIn() throws Exception {
        ReplicaUseResult useResult = handleQueIn001();
        if (useResult.doBreak) {
            // Раз просили прерваться - значит прерываемся
            return;
        }
        handleQueIn(false);
    }

    private ReplicaUseResult handleQueIn(boolean forceUse) throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long queInNoDone = stateManager.getQueNoDone("in");
        long queInNoAvailable = queIn.getMaxNo();
        return handleQue(queIn, "in", queInNoDone + 1, queInNoAvailable, forceUse);
    }

    private ReplicaUseResult handleQueIn001() throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long queInNoDone = stateManager.getQueNoDone("in001");
        long queInNoAvailable = queIn001.getMaxNo();
        return handleQue(queIn001, "in001", queInNoDone + 1, queInNoAvailable, false);
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
        db.startTran();  // зачем тогда тут транзакция??? !!!!!!!!!!!!!!    Везде, где есть  que***.push - проверить необходимость транзакции
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

    /**
     * @deprecated Разобраться с репликацией через папку - сейчас полностью сломано
     */
    @Deprecated
    public void receiveFromDir(String cfgFileName, String mailDir) throws Exception {
/*
        // Готовим локальный мейлер
        mailDir = UtFile.unnormPath(mailDir) + "/";
        String guid = this.getWsGuid();
        String guidPath = guid.replace("-", "/");

        //
        JSONObject cfgData = UtRepl.loadAndValidateJsonFile(cfgFileName));

        // Конфиг для мейлера
        JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgWs.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IMailer mailerLocal = new MailerLocalFiles();
        mailerLocal.init(cfgWs);

        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getBoxState("to");

        // Физически забираем данные
        receiveInternal(mailerLocal, "to", selfReceivedNo + 1, srvAvailableNo, queIn);
*/
    }


    // Физически забираем данные
    public void receive() throws Exception {
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


    void receiveInternal(IMailer mailer, String boxName, long no_from, long no_to, IJdxReplicaQue que) throws Exception {
        log.info("receive, self.wsId: " + wsId + ", box: " + boxName + ", que.name: " + ((JdxQue) que).getQueName());

        //
        long count = 0;
        for (long no = no_from; no <= no_to; no++) {
            log.debug("receive, receiving.no: " + no);

            // Информация о реплике с почтового сервера
            IReplicaInfo info = mailer.getReplicaInfo(boxName, no);

            // Нужно ли скачивать эту реплику с сервера?
            IReplica replica;
            if (isReplicaSelfSnapshot(info)) {
                // Свои собственные установочные реплики (snapshot таблиц) можно не скачивать
                // (и в дальнейшем не применять)
                log.info("Found self snapshot replica, no: " + no + ", replica.age: " + info.getAge());
                // Имитируем реплику просто чтобы положить в очередь.
                // Никто не заметит, что реплика пустая, т.к. она НЕ нужна
                replica = new ReplicaFile();
                replica.getInfo().setReplicaType(info.getReplicaType());
                replica.getInfo().setWsId(info.getWsId());
                replica.getInfo().setAge(info.getAge());
                //replica.getInfo().setCrc(info.getCrc()); по идее crc тоже удобнее НЕ прописывать - как сигнал, что и файла тоже нет
            } else {
                // Физически забираем данные реплики с сервера
                replica = mailer.receive(boxName, no);

                // Проверяем целостность скачанного
                UtJdx.checkReplicaCrc(replica, info.getCrc());

                // Читаем заголовок
                JdxReplicaReaderXml.readReplicaInfo(replica);
            }

            // Помещаем реплику в свою входящую очередь
            que.push(replica);

            // Удаляем с почтового сервера
            mailer.delete(boxName, no);

            //
            count++;
        }


        // Отметить попытку чтения (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.read", boxName);
        // Отметить состояние рабочей станции (станция отчитывается о себе для отслеживания активности станции)
        Map info = getInfoWs();
        mailer.setData(info, "ws.info", null);


        //
        if (count > 0) {
            log.info("receive, self.wsId: " + wsId + ", box: " + boxName + ", que.name: " + ((JdxQue) que).getQueName() + ", receive.no: " + no_from + " .. " + no_to + ", done count: " + count);
        } else {
            log.info("receive, self.wsId: " + wsId + ", box: " + boxName + ", que.name: " + ((JdxQue) que).getQueName() + ", receive.no: " + no_from + ", nothing to receive");
        }
    }


    /**
     * @deprecated Разобраться с репликацией через папку - сейчас полностью сломано
     */
    @Deprecated
    public void sendToDir(String cfgFileName, String mailDir, long age_from, long age_to, boolean doMarkDone) throws Exception {
        // Готовим локальный мейлер
        JSONObject cfgData = UtRepl.loadAndValidateJsonFile(cfgFileName);
        //
        mailDir = UtFile.unnormPath(mailDir) + "/";
        String guid = getWsGuid();
        String guidPath = guid.replace("-", "/");

        // Конфиг для мейлера
        cfgData = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgData.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IMailer mailerLocal = new MailerLocalFiles();
        mailerLocal.init(cfgData);


        // Сколько своего аудита уже отправлено на сервер
        JdxMailStateManagerWs stateManager = new JdxMailStateManagerWs(db);
        long srvSendAge = stateManager.getMailSendDone();

        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxNo();

        // От какого возраста отправлять. Если не указано - начнем от ранее отправленного
        if (age_from == 0L) {
            age_from = srvSendAge + 1;
        }

        // До какого возраста отправлять. Если не указано - все у нас что есть в очереди на отправку
        if (age_to == 0L) {
            age_to = selfQueOutAge;
        }


        // Физически отправляем данные
        // sendInternal(mailerLocal, age_from, age_to, doMarkDone); // Заменен на UtMail.sendQueToMail
    }


    public void send() throws Exception {
        JdxMailStateManagerWs stateManager = new JdxMailStateManagerWs(db);
        UtMail.sendQueToMail(wsId, queOut, mailer, "from", stateManager);
    }


    public Map getInfoWs() throws Exception {
        Map info = new HashMap<>();

        //
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxMailStateManagerWs stateMailManager = new JdxMailStateManagerWs(db);
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);

        //
        long out_auditAgeActual = auditAgeManager.getAuditAge(); // Возраст аудита БД
        long out_queAvailable = stateManager.getAgeQueOutDone();  // Возраст аудита, до которого сформирована исходящая очередь
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
        IJdxTable thisTable = UtDbErrors.get_ForeignKeyViolation_tableInfo(e, struct);
        IJdxForeignKey foreignKey = UtDbErrors.get_ForeignKeyViolation_refInfo(e, struct);
        IJdxField refField = foreignKey.getField();
        IJdxTable refTable = refField.getRefTable();
        //
        String thisTableName = thisTable.getName();
        String thisTableRefFieldName = refField.getName();
        //
        String refTableName = refTable.getName();
        String refTableFieldName = foreignKey.getTableField().getName();
        //
        String refTableId = (String) e.recValues.get(thisTableRefFieldName);

        //
        log.error("Searching foreign key: " + thisTableName + "." + thisTableRefFieldName + " -> " + refTableName + "." + refTableFieldName + ", foreign key: " + refTableId);

        //
        File outReplicaFile = new File(dataRoot + "temp/" + refTableName + "_" + refTableId.replace(":", "_") + ".zip");
        // Если в одной реплике много ошибочных записей, то искать можно только один раз,
        // иначе на каждую ссылку будет выполнятся поиск, что затянет выкидывание ошибки
        if (outReplicaFile.exists()) {
            log.error("Файл с временной репликой - результатами поиска уже есть: " + outReplicaFile.getAbsolutePath());
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
        log.error("Файл с временной репликой - результатами поиска сформирован: " + replica.getData().getAbsolutePath());

        //
        return outReplicaFile;
    }

    /**
     * Выявить ситуацию "станцию восстановили из бэкапа" и починить ее
     * todo: НЕ обрабатывает ситуацию "в очереди реплики есть, а в каталоге их нет (пропали файлы)"
     */
    public void repairAfterBackupRestore(boolean doRepair, boolean doPrint) throws Exception {
        JdxStorageFile queFile = new JdxStorageFile();

        // ---
        // Сколько входящих получено у нас в "официальной" очереди
        long noQueIn = queIn.getMaxNo();

        // Сколько входящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueInDir = ((JdxStorageFile) queIn).getMaxNoFromDir();

        // До какого возраста обработана очередь QueIn
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long noQueInUsed = stateManager.getQueNoDone("in");

        // Сколько входящих получено у нас в "официальной" очереди
        long noQueIn001 = queIn001.getMaxNo();

        // Сколько входящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueIn001Dir = ((JdxStorageFile) queIn001).getMaxNoFromDir();

        // До какого возраста обработана очередь QueIn001
        long noQueIn001Used = stateManager.getQueNoDone("in001");


        // ---
        // Сколько исходящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueOutDir = ((JdxStorageFile) queOut).getMaxNoFromDir();

        // Cколько исходящих реплик есть у нас "официально", т.е. в очереди реплик (в базе)
        long noQueOut = queOut.getMaxNo();


        // ---
        // Сколько исходящих реплик фактически отправлено на сервер (спросим у почтового сервера)
        long noQueOutSrvSendDone = mailer.getSendDone("from");

        // Сколько исходящих реплик отмечено как отправленое на сервер
        JdxMailStateManagerWs mailStateManager = new JdxMailStateManagerWs(db);
        long noQueOutSendMarked = mailStateManager.getMailSendDone();

        // Проверим, что помеченное на 1 меньше отправленного, и CRC одинаковое
        boolean normal_Marked_SrvDone = false;
        if ((noQueOutSendMarked + 1) == noQueOutSrvSendDone) {
            // Читаем информацию о реплике с сервера
            IReplicaInfo infoSrv = ((MailerHttp) mailer).getLastReplicaInfo("from");
            String crcSrv = infoSrv.getCrc();
            // Берем реплику из очереди
            IReplica replicaWs = queOut.get(noQueOutSrvSendDone);
            // Сравниваем CRC
            if (UtJdx.equalReplicaCrc(replicaWs, crcSrv)) {
                log.warn("Detected restore from backup, workstation and server have equal replica.no and equal replica.crc");
                // Чиним возраст "отправлено на сервер"
                mailStateManager.setMailSendDone(noQueOutSrvSendDone);
                log.warn("Repair mailSendDone, " + noQueOutSendMarked + " -> " + noQueOutSrvSendDone);
                //
                normal_Marked_SrvDone = true;
            }
        }


        // Есть ли отметка о начале ремонта
        File lockFile = new File(dataRoot + "temp/repairBackup.lock");


        // Допускается, если в каталоге для QueIn меньше реплик, чем в очереди QueIn (noQueIn > noQueInDir).
        // Это бывает из-за того, что при получении собственных snapshot-реплик, мы ее не скачиваем (т.к. она нам не нужна).
        //
        // Допускается, если не все входящие реплики использованы (noQueIn > noQueInUsed).
        // Это бывает, если прерывается процесс применения реплик.
        // Это не страшно, т.к. при следующем запуске применение возобновится.
        //
        // Допускается, если noQueOutSendMarked на 1 меньше noQueOutSrvSendDone, но при этом CRC реплики queOut[noQueOutSrvSendDone] одинаковая на сервере и в очереди.
        // Это бывает, если прерывается процесс передачи реплик на этапе отметки.
        // Это не страшно, т.к. не говорит о подмене/востановлении базы.
        boolean needRepair;
        if ((noQueIn == -1 || noQueIn >= noQueInDir) &&
                (noQueIn >= noQueInUsed) &&
                (noQueOut == noQueOutDir) &&
                (noQueOutSendMarked == noQueOutSrvSendDone || normal_Marked_SrvDone) &&
                !lockFile.exists()
        ) {
            needRepair = false;
        } else {
            needRepair = true;
        }

        //
        if (needRepair || doPrint) {
            log.warn("Detected restore from backup, self.wsId: " + wsId);
            log.warn("  noQueIn001: " + noQueIn001);
            log.warn("  noQueIn001Dir: " + noQueIn001Dir);
            log.warn("  noQueIn001Used: " + noQueIn001Used);
            log.warn("  noQueIn: " + noQueIn);
            log.warn("  noQueInDir: " + noQueInDir);
            log.warn("  noQueInUsed: " + noQueInUsed);
            log.warn("  noQueOut: " + noQueOut);
            log.warn("  noQueOutDir: " + noQueOutDir);
            log.warn("  noQueOutSrvSendDone: " + noQueOutSrvSendDone);
            log.warn("  noQueOutSendMarked: " + noQueOutSendMarked);
            log.warn("  normal_Marked_SrvDone: " + normal_Marked_SrvDone);
            log.warn("  lockFile: " + lockFile.exists());
            log.warn("  need repair: " + needRepair);
        }

        //
        if (!needRepair) {
            return;
        }

        //
        if (needRepair && !doRepair) {
            String errInfo = "" +
                    "noQueIn001: " + noQueIn001 + ", " +
                    "noQueIn001Dir: " + noQueIn001Dir + ", " +
                    "noQueIn001Used: " + noQueIn001Used + ", " +
                    "noQueIn: " + noQueIn + ", " +
                    "noQueInDir: " + noQueInDir + ", " +
                    "noQueInUsed: " + noQueInUsed + ", " +
                    "noQueOut: " + noQueOut + ", " +
                    "noQueOutDir: " + noQueOutDir + ", " +
                    "noQueOutSrvSendDone: " + noQueOutSrvSendDone + ", " +
                    "noQueOutSendMarked: " + noQueOutSendMarked + ", " +
                    "normal_Marked_SrvDone: " + normal_Marked_SrvDone + ", " +
                    "lockFile: " + lockFile.exists() + ", " +
                    "need repair: " + needRepair;
            throw new XError("Detected restore from backup, repair needed: " + errInfo);
        }


        // ---
        // После этой отметки ремонт считается НАЧАТЫМ, но НЕ ЗАВЕРШЕННЫМ.
        // ---
        if (!lockFile.exists()) {
            UtFile.saveString(String.valueOf(new DateTime()), lockFile);
        }


        // ---
        // Сначала чиним получение реплик
        // ---

        // Ситуация: noQueIn <> noQueInDir
        // Берем входящие реплики, которые мы пропустили.
        // Кладем их в свою входящую очередь (потом они будут использованы штатным механизмом).
        // Запрос на сервер повторной отправки входящих реплик - НЕ НУЖНО - они у нас уже есть.
        queFile.setDataRoot(queIn.getBaseDir());
        long count = 0;
        for (long no = noQueIn + 1; no <= noQueInDir; no++) {
            log.warn("Repair queIn, self.wsId: " + wsId + ", queIn.no: " + no + " (" + count + "/" + (noQueInDir - noQueIn) + ")");

            // Извлекаем входящую реплику из закромов
            IReplica replica = queFile.get(no);
            JdxReplicaReaderXml.readReplicaInfo(replica);

            // Пополнение (восстановление) входящей очереди
            queIn.push(replica);

            //
            count = count + 1;
        }

        //
        if (count > 0) {
            log.warn("Repair queIn, self.wsId: " + wsId + ", queIn: " + noQueIn + " .. " + noQueInDir + ", done count: " + count);
        } else {
            log.info("Repair queIn, self.wsId: " + wsId + ", queIn: " + noQueIn + ", nothing to do");
        }

        // ---
        // А теперь применяем все входящие реплики штатным механизмом.
        // Важно их применить, т.к. среди входящих есть и НАШИ СОБСТВЕННЫЕ, но еще не примененные.
        ReplicaUseResult queInUseResult = handleQueIn(true);


        // Запоминаем наш последний возраст, встретившийся в репликах
        long lastOwnAgeUsed = queInUseResult.lastOwnAgeUsed;


        // ---
        // Чиним данные на основе собственного аудита (queOut)
        // ---

        // Применяем их у себя (это нужно - для случая восстановления из бэкапа именно в ИСХОДЯЩИХ репликах содержатся самые последние данные).
        // Восстанавливаем исходящую очередь (вообще-то уже не обязательно, т.к. ОТПРАВЛЕННЫЕ реплики больше не нужны, но просто для порядка).
        queFile.setDataRoot(queOut.getBaseDir());
        count = 0;
        for (long no = noQueOut + 1; no <= noQueOutDir; no++) {
            log.warn("Repair queOut, self.wsId: " + wsId + ", queOut.no: " + no + " (" + count + "/" + (noQueOutDir - noQueOut) + ")");

            // Извлекаем собственную реплику из закромов queOut
            IReplica replica = queFile.get(no);
            JdxReplicaReaderXml.readReplicaInfo(replica);
            long age = replica.getInfo().getAge();

            // Отслеживаем наш последний возраст, встретившийся в репликах
            if (age > lastOwnAgeUsed) {
                lastOwnAgeUsed = age;
            }

            // Применяем собственную реплику
            if (queInUseResult.lastOwnAgeUsed != 0 && age != 0 && age <= queInUseResult.lastOwnAgeUsed) {
                // Учтем, до которого возраста мы получили и применили НАШИ СОБСТВЕННЫЕ реплики
                // из ВХОДЯЩЕЙ очереди QueIn и применим реплику из queOut ТОЛЬКО НЕ примененные
                log.warn("Repair queOut, already used: " + no + ", skipped");
            } else {
                // Пробуем применить собственную реплику
                ReplicaUseResult useResult = useReplicaInternal(replica, true);

                //
                if (!useResult.replicaUsed) {
                    throw new XError("Repair queOut, useResult.replicaUsed == false");
                }
                if (useResult.doBreak) {
                    throw new XError("Repair queOut, replica useResult.doBreak == true");
                }
                log.warn("Repair queOut, used: " + no);
            }

            // Пополнение (восстановление) исходящей очереди реплик
            queOut.push(replica);

            //
            count = count + 1;
        }

        //
        if (count > 0) {
            log.warn("Repair queOut, self.wsId: " + wsId + ", queOut: " + noQueOut + " .. " + noQueOutDir + ", done count: " + count);
        } else {
            log.info("Repair queOut, self.wsId: " + wsId + ", queOut: " + noQueOut + ", nothing to do");
        }


        // ---
        // Чиним генераторы.
        // После применения собственных реплик генераторы находятся в устаревшем сосоянии.
        PkGenerator pkGenerator = new PkGenerator_PS(db, struct);
        pkGenerator.repairGenerators();


        // ---
        // Чиним отметку возраста аудита.
        // После применения собственных реплик состояние "возраст age" для рабочей станции все ещё содержит устаревшее состояние.
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        long ageNow = auditAgeManager.getAuditAge();
        if (ageNow < lastOwnAgeUsed) {
            auditAgeManager.setAuditAge(lastOwnAgeUsed);
            log.warn("Repair auditAge, " + ageNow + " -> " + lastOwnAgeUsed);
        }


        // ---
        // Если возраст "отправлено на сервер" меньше, чем фактический размер исходящей очереди -
        // чиним отметку об отправке собственных реплик.
        if (noQueOutSendMarked < noQueOutSrvSendDone) {
            mailStateManager.setMailSendDone(noQueOutSrvSendDone);
            log.warn("Repair mailSendDone, " + noQueOutSendMarked + " -> " + noQueOutSrvSendDone);
        }


        // ---
        // До какого возраста аудит отмечен как выложенный в очередь QueOut
        long ageQueOutDoneNow = stateManager.getAgeQueOutDone();
        if (ageQueOutDoneNow < lastOwnAgeUsed) {
            stateManager.setAgeQueOutDone(lastOwnAgeUsed);
            log.warn("Repair ageQueOutDone, " + ageQueOutDoneNow + " -> " + lastOwnAgeUsed);
        }


        // ---
        // Убираем отметку.
        // После этой отметки ремонт считается завершенным.
        if (!lockFile.delete()) {
            throw new XError("Can`t delete lock: " + lockFile);
        }

    }

    public void wsCreateSnapshot(String tableNames, String outName) throws Exception {
        // Разложим в список
        List<IJdxTable> tables = UtJdx.stringToTables(tableNames, struct);

        // Создаем снимок таблицы (разрешаем отсылать чужие записи)
        UtRepl ut = new UtRepl(db, struct);
        List<IReplica> replicasRes = ut.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOut, false);

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

    public void wsSendSnapshot(String tableNames) throws Exception {
        // Разложим в список
        List<IJdxTable> tables = UtJdx.stringToTables(tableNames, struct);

        // Создаем снимок таблицы (разрешаем отсылать чужие записи)
        UtRepl ut = new UtRepl(db, struct);
        List<IReplica> replicasRes = ut.createSnapshotForTablesFiltered(tables, wsId, wsId, publicationOut, false);

        // Отправляем снимок таблицы в очередь queOut
        ut.sendToQue(replicasRes, queOut);
    }

    public void debugDumpStruct(String prefix) throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        struct_rw.toFile(struct, dataRoot + "temp/" + prefix + "dbStruct.actual.xml");
        struct_rw.toFile(structAllowed, dataRoot + "temp/" + prefix + "dbStruct.allowed.xml");
        struct_rw.toFile(structFixed, dataRoot + "temp/" + prefix + "dbStruct.fixed.xml");
    }

}
