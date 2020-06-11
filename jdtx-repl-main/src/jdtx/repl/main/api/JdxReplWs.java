package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.utils.io.*;
import jandcode.web.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ut.*;
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


    private class ReplicaUseResult {
        boolean replicaUsed = true;
        boolean doBreak = false;
        // Возраст своих использованных реплик (важно при восстановлении после сбоев)
        long lastOwnAgeUsed = -1;
    }

    //
    private long MAX_COMMIT_RECS = 10000;

    // Правила публикации
    private IPublication publicationIn;
    private IPublication publicationOut;

    //
    private IJdxQueCommon queIn;
    private IJdxQuePersonal queOut;
    //private JdxQueCommonFile queIn;
    //private JdxQuePersonalFile queOut;

    //
    private Db db;
    protected long wsId;
    protected IJdxDbStruct struct;
    protected IJdxDbStruct structAllowed;
    protected IJdxDbStruct structFixed;

    //
    private IMailer mailer;

    //
    private String dataRoot;

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

    public IMailer getMailer() {
        return mailer;
    }

    /**
     * Рабочая станция, настройка
     */
    public void init() throws Exception {
        MDC.put("serviceName", "ws");

        //
        dataRoot = new File(db.getApp().getRt().getChild("app").getValueString("dataRoot")).getCanonicalPath();
        dataRoot = UtFile.unnormPath(dataRoot) + "/";
        log.info("dataRoot: " + dataRoot);

        // Проверка наличия в БД служебных структур и их версии
        UtDbObjectManager ut = new UtDbObjectManager(db);
        ut.checkReplVerDb();

        // Проверка, что инициализация станции прошла
        ut.checkReplDb();

        // Чтение структуры БД
        IJdxDbStructReader structReader = new JdxDbStructReader();
        structReader.setDb(db);
        IJdxDbStruct structActual = structReader.readDbStruct();

        // Читаем код нашей станции
        DataRecord rec = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation").getCurRec();
        // Проверяем код нашей станции
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Invalid workstation.ws_id == 0");
        }
        this.wsId = rec.getValueLong("ws_id");

        // Чтение конфигурации
        UtCfg utCfg = new UtCfg(db);
        JSONObject cfgDbWs = utCfg.getSelfCfg(UtCfgType.WS);
        JSONObject cfgDbPublications = utCfg.getSelfCfg(UtCfgType.PUBLICATIONS);
        JSONObject cfgDbDecode = utCfg.getSelfCfg(UtCfgType.DECODE);

        // Рабочие каталоги
        String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
        String queIn_DirLocal = dataRoot + "ws_" + sWsId + "/queIn";
        String queOut_DirLocal = dataRoot + "ws_" + sWsId + "/queOut";
        String mailLocalDirTmp = dataRoot + "temp/";

        // Читаем из этой очереди
        queIn = new JdxQueCommonFile(db, JdxQueType.IN);
        queIn.setBaseDir(queIn_DirLocal);

        // Пишем в эту очередь
        queOut = new JdxQuePersonalFile(db, JdxQueType.OUT);
        queOut.setBaseDir(queOut_DirLocal);

        // Конфиг для мейлера
        JSONObject cfgWs = new JSONObject();
        String url = (String) cfgDbWs.get("url");
        String guid = rec.getValueString("guid");
        cfgWs.put("guid", guid);
        cfgWs.put("url", url);
        cfgWs.put("localDirTmp", mailLocalDirTmp);

        // Мейлер
        mailer = new MailerHttp();
        mailer.init(cfgWs);

        // Стратегии перекодировки каждой таблицы
        if (cfgDbDecode != null && RefDecodeStrategy.instance == null) {
            RefDecodeStrategy.instance = new RefDecodeStrategy();
            RefDecodeStrategy.instance.init(cfgDbDecode);
        }

        // Правила публикаций
        this.publicationIn = new Publication();
        this.publicationOut = new Publication();
        UtRepl.fillPublications(cfgDbPublications, structActual, this.publicationIn, this.publicationOut);


        // Фильтрация структуры: убирание того, чего нет в публикациях publicationIn и publicationOut
        struct = UtRepl.getStructCommon(structActual, this.publicationIn, this.publicationOut);


        //
        UtDbStructApprover dbStructApprover = new UtDbStructApprover(db);
        structAllowed = dbStructApprover.getDbStructAllowed();
        structFixed = dbStructApprover.getDbStructFixed();


        // Проверка версии приложения, обновление при необходимости
        UtAppVersion_DbRW appVersionRW = new UtAppVersion_DbRW(db);
        String appVersionAllowed = appVersionRW.getAppVersionAllowed();
        String appVersionActual = UtRepl.getVersion();
        if (appVersionAllowed.length() == 0) {
            log.info("appVersionAllowed.length == 0, appVersionActual: " + appVersionActual);
        } else if (appVersionActual.compareToIgnoreCase("SNAPSHOT") == 0) {
            log.warn("appVersionActual == SNAPSHOT, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
        } else if (appVersionAllowed.compareToIgnoreCase(appVersionActual) != 0) {
            log.info("appVersionAllowed != appVersionActual, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
            if (Ut.tryParseInteger(appVersionAllowed) != 0 && Ut.tryParseInteger(appVersionAllowed) < Ut.tryParseInteger(appVersionActual)) {
                // Установлена боле новая версия - не будем обновлять до более старой
                log.warn("appVersionAllowed < appVersionActual, skip application update");
            } else {
                // Есть более новая версия - будем обновлять
                doAppUpdate(appVersionAllowed);
            }
        }

        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
    }

    void doAppUpdate(String appVersionAllowed) throws Exception {
        File exeFile = new File("install/JadatexSync-update-" + appVersionAllowed + ".exe");
        log.info("start app update, exeFile: " + exeFile);

        // Запуск обновления
        List<String> res = new ArrayList<>();
        int exitCode = UtRun.run(res, exeFile.getAbsolutePath(), "/SILENT", "/repl-service-install");

        //
        if (exitCode != 0) {
            System.out.println("exitCode: " + exitCode);
            for (String outLine : res) {
                System.out.println(outLine);
            }

            //
            throw new XError("Failed to update application, appVersionAllowed: " + appVersionAllowed);
        }
    }

    /**
     * Формируем установочную реплику
     */
    public void createTableSnapshotReplica(String tableName) throws Exception {
        log.info("createReplicaTableSnapshot, wsId: " + wsId + ", table: " + tableName);

        //
        IJdxTable publicationTable = publicationOut.getData().getTable(tableName);
        if (publicationTable == null) {
            log.info("createReplicaTableSnapshot, skip createSnapshot, not found in publicationOut, table: " + tableName);
            return;
        }

        //
        UtRepl utRepl = new UtRepl(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        db.startTran();
        try {
            // Искусственно увеличиваем возраст (установочная реплика сдвигает возраст БД на 1)
            long age = utRepl.incAuditAge();
            log.info("createReplicaTableSnapshot, tableName: " + tableName + ", new age: " + age);

            // Создаем установочную реплику
            IReplica replicaSnapshot = utRepl.createReplicaTableSnapshot(wsId, publicationTable, age);

            // Помещаем реплику в очередь
            queOut.put(replicaSnapshot);

            //
            stateManager.setAuditAgeDone(age);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        //
        log.info("createReplicaTableSnapshot, wsId: " + wsId + ", done");
    }

    /**
     * Формируем реплику по выбранным записям
     */
    public void createTableReplicaByIdList(String tableName, Collection<Long> idList) throws Exception {
        log.info("createTableReplicaByIdList, wsId: " + wsId + ", table: " + tableName + ", count: " + idList.size());

        //
        IJdxTable table = struct.getTable(tableName);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        db.startTran();
        try {
            // Искусственно увеличиваем возраст (реплика сдвигает возраст БД на 1)
            long age = utRepl.incAuditAge();
            log.info("createReplicaTableByIdList, tableName: " + tableName + ", new age: " + age);

            // Создаем реплику
            IReplica replicaSnapshot = utRepl.createReplicaTableByIdList(wsId, table, age, idList);

            // Помещаем реплику в очередь
            queOut.put(replicaSnapshot);

            //
            stateManager.setAuditAgeDone(age);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

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
    public boolean dbStructApplyFixed() throws Exception {
        log.info("dbStructApplyFixed, checking");

        // Читаем структуры
        IJdxDbStruct structActual = struct;
        UtDbStructApprover dbStructRW = new UtDbStructApprover(db);
        IJdxDbStruct structFixed = dbStructRW.getDbStructFixed();
        IJdxDbStruct structAllowed = dbStructRW.getDbStructAllowed();

        // Сравниваем
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();
        //
        boolean equal_Actual_Allowed = UtDbComparer.dbStructIsEqualTables(structActual, structAllowed);
        boolean equal_Actual_Fixed = UtDbComparer.getStructDiffTables(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

        // Нет необходимости в фиксации структуры -
        // все структуры совпадают
        if (equal_Actual_Allowed && equal_Actual_Fixed) {
            log.info("dbStructApplyFixed, no diff found, Actual == Allowed == Fixed");
            //
            return true;
        }

        // Нет возможности фиксации структуры -
        // реальная не совпадает с разрешенной
        if (!equal_Actual_Allowed) {
            log.warn("dbStructApplyFixed, Actual <> Allowed");

            // Для справки/отладки - структуры в файл
            JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
            struct_rw.toFile(structActual, dataRoot + "temp/dbStruct.actual.xml");
            struct_rw.toFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
            struct_rw.toFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");

            //
            return false;
        }

        // Начинаем фиксацию структуры -
        // реальная совпадает с разрешенной, но отличается от зафиксированной
        log.info("dbStructApplyFixed, start");

        // Обеспечиваем порядок сортировки таблиц с учетом foreign key (при применении структуры будем делать snapsot, там важен порядок)
        List<IJdxTable> tablesNew = JdxUtils.sortTablesByReference(structDiffNew.getTables());


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


        // Делаем выгрузку snapshot, обязательно в ОТДЕЛЬНОЙ транзакции.
        // В некоторых СУБД (напр. Firebird) изменение структуры происходит ВНУТРИ транзакций,
        // тогда получится, что пока делается snapshot, аудит не работает.
        // Таким образом данные, вводимые во время подготовки аудита и snapshot-та, не попадут ни в аудит, ни в snapshot,
        // т.к. таблица аудита ЕЩЕ не видна другим транзакциям, а данные, продолжающие поступать в snapshot, УЖЕ не видны нашей транзакции.
        db.startTran();
        try {
            //
            long n = 0;
            for (IJdxTable table : tablesNew) {
                n++;
                log.info("  createSnapshot " + n + "/" + tablesNew.size() + " " + table.getName());

                //
                if (UtRepl.tableSkipRepl(table)) {
                    log.info("  createSnapshot, skip not found in tableSkipRepl, table: " + table.getName());
                    continue;
                }

                // Создание snapshot-реплики
                createTableSnapshotReplica(table.getName());
            }


            // Запоминаем текущую структуру БД как "фиксированную" структуру
            dbStructRW.setDbStructFixed(structActual);

            //
            db.commit();

        } catch (Exception e) {
            db.rollback(e);
            throw e;
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
        UtRepl utRepl = new UtRepl(db, struct);
        UtDbStructApprover dbStructApprover = new UtDbStructApprover(db);

        // Если в стостоянии "я замолчал", то молчим
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);
        if (utmm.isMute()) {
            log.warn("handleSelfAudit, workstation is mute");
            return;
        }

        // Проверяем совпадение структур
        IJdxDbStruct structAllowed = dbStructApprover.getDbStructAllowed();
        IJdxDbStruct structFixed = dbStructApprover.getDbStructFixed();

        // Проверяем совпадает ли реальная структура БД с разрешенной структурой
        if (struct.getTables().size() == 0 || !UtDbComparer.dbStructIsEqual(struct, structAllowed)) {
            log.warn("handleSelfAudit, database structActual <> structAllowed");
            // Для справки/отладки - структуры в файл
            JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
            struct_rw.toFile(struct, dataRoot + "temp/dbStruct.actual.xml");
            struct_rw.toFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
            struct_rw.toFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");
            //
            return;
        }
        // Проверяем совпадает ли реальная структура БД с фиксированной структурой
        if (struct.getTables().size() == 0 || !UtDbComparer.dbStructIsEqual(struct, structFixed)) {
            log.warn("handleSelfAudit, database structActual <> structFixed");
            // Для справки/отладки - структуры в файл
            JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
            struct_rw.toFile(struct, dataRoot + "temp/dbStruct.actual.xml");
            struct_rw.toFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
            struct_rw.toFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");
            //
            return;
        }


        // Формируем реплики (по собственным изменениям)
        db.startTran();
        try {
            long count = 0;

            // Узнаем (и заодно фиксируем) возраст своего аудита
            UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
            long auditAgeTo = auditAgeManager.markAuditAge();

            // До какого возраста отметили выкладывание в очередь реплик (из своего аудита)
            JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
            long auditAgeFrom = stateManager.getAuditAgeDone();

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
            for (long age = auditAgeFrom + 1; age <= auditAgeTo; age++) {
                IReplica replica = utRepl.createReplicaFromAudit(wsId, publicationOut, age);

                // Пополнение исходящей очереди реплик
                queOut.put(replica);

                // Отметка о пополнении исходящей очереди реплик
                stateManager.setAuditAgeDone(age);

                //
                count++;
            }

            //
            if (auditAgeFrom <= auditAgeTo) {
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
     * Применяем входящие реплики
     */
    public ReplicaUseResult handleQueIn(boolean forceUse) throws Exception {
        log.info("handleQueIn, self.wsId: " + wsId);

        //
        ReplicaUseResult handleQueInUseResult = new ReplicaUseResult();

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        long queInNoDone = stateManager.getQueInNoDone();
        long queInNoAvailable = queIn.getMaxNo();

        //
        long count = 0;
        for (long no = queInNoDone + 1; no <= queInNoAvailable; no++) {
            log.info("handleQueIn, self.wsId: " + wsId + ", queIn.no: " + no + " (" + count + "/" + (queInNoAvailable - queInNoDone) + ")");

            //
            IReplica replica = queIn.getByNo(no);

            // Пробуем применить реплику
            ReplicaUseResult replicaUseResult = useReplica(replica, forceUse);

            if (replicaUseResult.replicaUsed && replicaUseResult.lastOwnAgeUsed > handleQueInUseResult.lastOwnAgeUsed) {
                handleQueInUseResult.lastOwnAgeUsed = replicaUseResult.lastOwnAgeUsed;
            }

            // Реплика использованна?
            if (replicaUseResult.replicaUsed) {
                // Отметим применение реплики
                stateManager.setQueInNoDone(no);
                //
                count++;
            } else {
                // Не отмечаем
                log.info("handleQueIn, replica not used");
            }

            // Надо останавливаться?
            if (replicaUseResult.doBreak) {
                // Останавливаемся
                log.info("handleQueIn, break using replicas");
                break;
            }

        }

        //
        if (queInNoDone <= queInNoAvailable) {
            log.info("handleQueIn, self.wsId: " + wsId + ", queIn: " + queInNoDone + " .. " + queInNoAvailable + ", done count: " + count);
        } else {
            log.info("handleQueIn, self.wsId: " + wsId + ", queIn: " + queInNoDone + ", nothing to do");
        }


        //
        return handleQueInUseResult;
    }

    private ReplicaUseResult useReplica(IReplica replica, boolean forceApplySelf) throws Exception {
        ReplicaUseResult useResult = new ReplicaUseResult();
        useResult.replicaUsed = true;
        useResult.doBreak = false;
        if (wsId == replica.getInfo().getWsId()) {
            useResult.lastOwnAgeUsed = replica.getInfo().getAge();
        }

        // Инструменты
        UtAuditApplyer auditApplyer = new UtAuditApplyer(db, struct);
        JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
        UtDbStructApprover dbStructApprover = new UtDbStructApprover(db);
        UtAppVersion_DbRW appVersionManager = new UtAppVersion_DbRW(db);

        // Совпадает ли реальная структура БД с разрешенной структурой
        boolean isEqualStruct_Actual_Allowed = true;
        if (!UtDbComparer.dbStructIsEqual(struct, structAllowed)) {
            isEqualStruct_Actual_Allowed = false;
        }


        //
        switch (replica.getInfo().getReplicaType()) {
            case JdxReplicaType.UPDATE_APP: {
                // Реакция на команду - запуск обновления

                // ===
                // В этой реплике - версия приложения и бинарник для обновления (для запуска)

                // Версия
                String appVersionAllowed;
                InputStream stream = UtRepl.createInputStream(replica, "version");
                try {
                    File versionFile = File.createTempFile("~JadatexSync", ".version");
                    UtFile.copyStream(stream, versionFile);
                    appVersionAllowed = UtFile.loadString(versionFile);
                    versionFile.delete();
                } finally {
                    stream.close();
                }

                // Распаковываем бинарник
                // TODO Обработать ситуацию, когда антивирус съел бинарник.
                // Надо научиться при отсутствии бинарника снова искать последнюю реплику и распаковывать бинарник непосредственно перед запуском
                InputStream replicaStream = UtRepl.createInputStream(replica, ".exe");
                try {
                    UtFile.mkdirs("install");
                    File exeFile = new File("install/JadatexSync-update-" + appVersionAllowed + ".exe");
                    OutputStream exeFileStream = new FileOutputStream(exeFile);
                    UtFile.copyStream(replicaStream, exeFileStream);
                    exeFileStream.close();
                } finally {
                    replicaStream.close();
                }


                // ===
                // Отмечаем разрешенную версию.
                // Реальное обновление программы будет позже, при следующем запуске
                appVersionManager.setAppVersionAllowed(appVersionAllowed);


                // ===
                // Выкладывание реплики "Я принял обновление"
                reportReplica(JdxReplicaType.UPDATE_APP_DONE);


                // Обновление приложения требует перезапуск репликатора, поэтому обработка входящих реплик прерывается
                useResult.doBreak = true;

                //
                break;
            }

            case JdxReplicaType.MUTE: {
                // Реакция на команду - перевод в режим "MUTE"

                // Узнаем получателя
                JSONObject info;
                InputStream infoStream = UtRepl.createInputStream(replica, "info.json");
                try {
                    String cfgStr = loadStringFromSream(infoStream);
                    info = (JSONObject) UtJson.toObject(cfgStr);
                } finally {
                    infoStream.close();
                }
                long destinationWsId = Long.valueOf(String.valueOf(info.get("destinationWsId")));


                // Реакция на команду, если получатель - все станции или именно наша
                if (destinationWsId == 0 || destinationWsId == wsId) {
                    // Последняя обработка собственного аудита
                    handleSelfAudit();

                    // Переход в состояние "Я замолчал"
                    muteManager.muteWorkstation();

                    // Выкладывание реплики "Я замолчал"
                    reportReplica(JdxReplicaType.MUTE_DONE);
                }

                //
                break;
            }

            case JdxReplicaType.UNMUTE: {
                // Реакция на команду - отключение режима "MUTE"

                // Узнаем получателя
                JSONObject info;
                InputStream infoStream = UtRepl.createInputStream(replica, "info.json");
                try {
                    String cfgStr = loadStringFromSream(infoStream);
                    info = (JSONObject) UtJson.toObject(cfgStr);
                } finally {
                    infoStream.close();
                }
                long destinationWsId = Long.valueOf(String.valueOf(info.get("destinationWsId")));

                // Реакция на команду, если получатель - все станции или именно наша
                if (destinationWsId == 0 || destinationWsId == wsId) {

                    // Выход из состояния "Я замолчал"
                    muteManager.unmuteWorkstation();

                    // Выкладывание реплики "Я уже не молчу"
                    reportReplica(JdxReplicaType.UNMUTE_DONE);
                }

                //
                break;
            }

            case JdxReplicaType.SET_DB_STRUCT: {
                // Реакция на команду - задать "разрешенную" структуру БД

                // В этой реплике - новая "разрешенная" структура
                InputStream stream = UtRepl.getReplicaInputStream(replica);
                try {
                    JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
                    IJdxDbStruct struct = struct_rw.read(stream);
                    // Запоминаем "разрешенную" структуру БД
                    dbStructApprover.setDbStructAllowed(struct);
                } finally {
                    stream.close();
                }

                // Проверяем структуры и пересоздаем аудит
                if (!dbStructApplyFixed()) {
                    // Если пересоздать аудит не удалось (структуры не обновлены или по иным причинам),
                    // то не метим реплтику как использованную
                    log.warn("handleQueIn, dbStructApplyFixed <> true");
                    useResult.replicaUsed = false;
                    useResult.doBreak = true;
                    break;
                }

                // Выкладывание реплики "структура принята"
                reportReplica(JdxReplicaType.SET_DB_STRUCT_DONE);

                //
                break;
            }

            case JdxReplicaType.SET_CFG: {
                // Реакция на команду - "задать конфигурацию"


                // В этой реплике - данные о новой конфигурации
                JSONObject cfgInfo;
                InputStream cfgInfoStream = UtRepl.createInputStream(replica, "cfg.info.json");
                try {
                    String cfgInfoStr = loadStringFromSream(cfgInfoStream);
                    cfgInfo = (JSONObject) UtJson.toObject(cfgInfoStr);
                } finally {
                    cfgInfoStream.close();
                }

                // Данные о новой конфигурации
                String cfgType = (String) cfgInfo.get("cfgType");
                long destinationWsId = Long.valueOf(String.valueOf(cfgInfo.get("destinationWsId")));

                // Пришла конфигурация для нашей станции (или всем станциям)?
                if (destinationWsId == 0 || destinationWsId == wsId) {
                    // В этой реплике - новая конфигурация
                    JSONObject cfg;
                    InputStream cfgStream = UtRepl.createInputStream(replica, "cfg.json");
                    try {
                        String cfgStr = loadStringFromSream(cfgStream);
                        cfg = (JSONObject) UtJson.toObject(cfgStr);
                    } finally {
                        cfgStream.close();
                    }

                    // Обновляем конфиг в своей таблице
                    UtCfg utCfg = new UtCfg(db);
                    utCfg.setSelfCfg(cfg, cfgType);

                    // Выкладывание реплики "конфигурация принята"
                    reportReplica(JdxReplicaType.SET_CFG_DONE);

                    // Обновление конфигурации требует переинициализацию репликатора, поэтому обработка входящих реплик прерывается
                    useResult.doBreak = true;
                }

                //
                break;
            }
            case JdxReplicaType.IDE:
            case JdxReplicaType.SNAPSHOT: {
                // Реальная структура базы НЕ совпадает с разрешенной структурой
                if (!isEqualStruct_Actual_Allowed) {
                    // Для справки/отладки - не совпадающие структуры - в файл
                    JdxDbStruct_XmlRW structRwXml = new JdxDbStruct_XmlRW();
                    structRwXml.toFile(struct, dataRoot + "temp/dbStruct.actual.xml");
                    structRwXml.toFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
                    structRwXml.toFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");
                    // Генерим ошибку
                    log.error("====================================================================");
                    log.error("====================================================================");
                    log.error("====================================================================");
                    //throw new XError("handleQueIn, structActual <> structAllowed");
                    log.error("handleQueIn, structActual <> structAllowed");
                    log.error("====================================================================");
                    log.error("====================================================================");
                    log.error("====================================================================");
                }

                // Свои собственные установочные реплики точно можно не применять
                if (replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT && replica.getInfo().getWsId() == wsId) {
                    log.info("skip self snapshot");
                    break;
                }

                // Реальная структура базы НЕ совпадает со структурой, с которой была подготовлена реплика
                JdxReplicaReaderXml.readReplicaInfo(replica);
                String replicaStructCrc = replica.getInfo().getDbStructCrc();
                String dbStructActualCrc = UtDbComparer.getDbStructCrcTables(struct);
                if (replicaStructCrc.compareToIgnoreCase(dbStructActualCrc) != 0) {
                    // Для справки/отладки - структуры в файл
                    JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
                    struct_rw.toFile(struct, dataRoot + "temp/dbStruct.actual.xml");
                    //
                    log.error("====================================================================");
                    log.error("====================================================================");
                    log.error("====================================================================");
                    log.error("handleQueIn, database.structCrc <> replica.structCrc, expected: " + dbStructActualCrc + ", actual: " + replicaStructCrc);
                    log.error("====================================================================");
                    log.error("====================================================================");
                    log.error("====================================================================");
                    //throw new XError("handleQueIn, database.structCrc <> replica.structCrc, expected: " + dbStructActualCrc + ", actual: " + replicaStructCrc);
                }

                // todo: Проверим протокол репликатора, с помощью которого была подготовлена реплика
                // String protocolVersion = (String) replica.getInfo().getProtocolVersion();
                // if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
                //      throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
                //}


                // Применение реплик
                boolean forceApply = false;
                if (replica.getInfo().getReplicaType() == JdxReplicaType.IDE && replica.getInfo().getWsId() == wsId && forceApplySelf) {
                    // Свои применяем принудительно, даже если они отфильтруются правилами публикации
                    forceApply = true;
                }
                //
                InputStream inputStream = null;
                try {
                    // Распакуем XML-файл из Zip-архива
                    inputStream = UtRepl.getReplicaInputStream(replica);

                    //
                    JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

                    // Для реплики типа SNAPSHOT - не слишком огромные порции коммитов
                    long commitPortionMax = 0;
                    if (replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
                        commitPortionMax = MAX_COMMIT_RECS;
                    }

                    //
                    auditApplyer.jdxReplWs = this;
                    auditApplyer.applyReplica(replicaReader, publicationIn, forceApply, wsId, commitPortionMax);
                } finally {
                    // Закроем читателя Zip-файла
                    if (inputStream != null) {
                        inputStream.close();
                    }
                }

                //
                break;
            }
        }

        //
        return useResult;
    }

    public ReplicaUseResult handleQueIn() throws Exception {
        return handleQueIn(false);
    }

    private String loadStringFromSream(InputStream stream) throws Exception {
        StringLoader ldr = new StringLoader();
        UtLoad.fromStream(ldr, stream);
        return ldr.getResult();
    }


    /**
     * Рабочая станция: отправка системной реплики
     * (например, ответа "Я замолчал" или "Я уже не молчу")
     * в исходящую очередь
     */
    public void reportReplica(int replicaType) throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        UtRepl utRepl = new UtRepl(db, struct);

        // Весь свой аудит предварительно выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        // Искусственно увеличиваем возраст (системная реплика сдвигает возраст БД на 1)
        long age = utRepl.incAuditAge();
        log.info("reportReplica, replicaType: " + replicaType + ", new age: " + age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(replicaType);
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);

        //
        utRepl.createOutputXML(replica);
        utRepl.closeOutputXML();

        //
        db.startTran();
        try {
            // Системная реплика - в исходящую очередь реплик
            queOut.put(replica);

            // Системная реплика - отметка об отправке
            stateManager.setAuditAgeDone(age);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }

    public void receiveFromDir(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальный мейлер
        mailDir = UtFile.unnormPath(mailDir) + "/";
        String guid = ((MailerHttp) mailer).guid;
        String guidPath = guid.replace("-", "/");

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

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
        receiveInternal(mailerLocal, selfReceivedNo + 1, srvAvailableNo);
    }


    // Физически забираем данные
    public void receive() throws Exception {
        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getBoxState("to");

        // Физически отправляем данные
        selfReceivedNo = selfReceivedNo + 1;
        receiveInternal(mailer, selfReceivedNo, srvAvailableNo);
    }


    void receiveInternal(IMailer mailer, long no_from, long no_to) throws Exception {
        log.info("receive, self.wsId: " + wsId);

        //
        long count = 0;
        for (long no = no_from; no <= no_to; no++) {
            log.info("receive, receiving.no: " + no);

            // Информация о реплике с почтового сервера
            ReplicaInfo info = mailer.getReplicaInfo("to", no);

            // Нужно ли скачивать эту реплику с сервера?
            IReplica replica;
            if (info.getWsId() == wsId && info.getReplicaType() == JdxReplicaType.SNAPSHOT) {
                // Свои собственные установочные реплики (snapshot таблиц) можно не скачивать (и не применять)
                log.info("Found self snapshot replica, age: " + info.getAge());
                //
                replica = new ReplicaFile();
                replica.getInfo().setWsId(info.getWsId());
                replica.getInfo().setAge(info.getAge());
                replica.getInfo().setReplicaType(info.getReplicaType());
            } else {
                // Физически забираем данные реплики с сервера
                replica = mailer.receive("to", no);
                // Проверяем целостность скачанного
                String md5file = JdxUtils.getMd5File(replica.getFile());
                if (!md5file.equals(info.getCrc())) {
                    log.error("receive.replica: " + replica.getFile());
                    log.error("receive.replica.md5: " + md5file);
                    log.error("mailer.info.crc: " + info.getCrc());
                    // Неправильно скачанный файл - удаляем, чтобы потом начать снова
                    replica.getFile().delete();
                    // Ошибка
                    throw new XError("receive.replica.md5 <> mailer.info.crc");
                }
                //
                JdxReplicaReaderXml.readReplicaInfo(replica);
            }

            //
            log.debug("replica.age: " + replica.getInfo().getAge() + ", replica.wsId: " + replica.getInfo().getWsId());

            // Помещаем реплику в свою входящую очередь
            queIn.put(replica);

            // Удаляем с почтового сервера
            mailer.delete("to", no);

            //
            count++;
        }


        // Отметить попытку чтения (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.read", "to");
        // Отметить состояние рабочей станции (станция отчитывается о себе для отслеживания активности станции)
        Map info = getInfoWs();
        mailer.setData(info, "ws.info", null);


        //
        if (no_from <= no_to) {
            log.info("receive, self.wsId: " + wsId + ", receive.no: " + no_from + " .. " + no_to + ", done count: " + count);
        } else {
            log.info("receive, self.wsId: " + wsId + ", receive.no: " + no_from + ", nothing to receive");
        }
    }


    public void sendToDir(String cfgFileName, String mailDir, long age_from, long age_to, boolean doMarkDone) throws Exception {
        // Готовим локальный мейлер
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));
        //
        mailDir = UtFile.unnormPath(mailDir) + "/";
        String guid = ((MailerHttp) mailer).guid;
        String guidPath = guid.replace("-", "/");

        // Конфиг для мейлера
        cfgData = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgData.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IMailer mailerLocal = new MailerLocalFiles();
        mailerLocal.init(cfgData);


        // Сколько своего аудита уже отправлено на сервер
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        long srvSendAge = stateManager.getMailSendDone();

        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxAge();

        // От какого возраста отправлять. Если не указано - начнем от ранее отправленного
        if (age_from == 0L) {
            age_from = srvSendAge + 1;
        }

        // До какого возраста отправлять. Если не указано - все у нас что есть в очереди на отправку
        if (age_to == 0L) {
            age_to = selfQueOutAge;
        }


        // Физически отправляем данные
        sendInternal(mailerLocal, age_from, age_to, doMarkDone);
    }

    public void send() throws Exception {
        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxAge();

        // Узнаем сколько своего аудита уже отправлено на сервер
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        long srvSendAge = stateManager.getMailSendDone();

        // Узнаем сколько просит сервер
        long srvRequireSendAge = mailer.getSendRequired("from");
        if (srvRequireSendAge != -1) {
            log.warn("Repeat send required, srvRequireSendAge: " + srvRequireSendAge + ", srvSendAge: " + srvSendAge);
            srvSendAge = srvRequireSendAge;
        } else {
            srvSendAge = srvSendAge + 1;
        }

        // Физически отправляем данные
        sendInternal(mailer, srvSendAge, selfQueOutAge, true);

        // Снимем флаг просьбы сервера
        if (srvRequireSendAge != -1) {
            mailer.setSendRequired("from", -1);
            log.warn("Repeat send done");
        }
    }

    void sendInternal(IMailer mailer, long age_from, long age_to, boolean doMarkDone) throws Exception {
        log.info("send, self.wsId: " + wsId);

        //
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);

        //
        long count = 0;
        for (long age = age_from; age <= age_to; age++) {
            log.info("send, sending.age: " + age);

            // Берем реплику
            IReplica replica = queOut.getByAge(age);

            // Читаем ее getReplicaInfo (нужна для проверки возраста при отправке)
            JdxReplicaReaderXml.readReplicaInfo(replica);

            // Физически отправляем реплику
            mailer.send(replica, "from", age);

            // Отмечаем факт отправки
            if (doMarkDone) {
                stateManager.setMailSendDone(age);
            }

            //
            count++;
        }


        // Отметить попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.write", "from");
        // Отметить состояние рабочей станции (станция отчитывается о себе для отслеживания активности станции)
        Map info = getInfoWs();
        mailer.setData(info, "ws.info", null);


        //
        if (age_from <= age_to) {
            log.info("send, self.wsId: " + wsId + ", send.age: " + age_from + " .. " + age_to + ", done count: " + count);
        } else {
            log.info("send, self.wsId: " + wsId + ", send.age: " + age_from + ", nothing to send");
        }
    }


    public Map getInfoWs() throws Exception {
        Map info = new HashMap<>();

        //
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxStateManagerMail stateMailManager = new JdxStateManagerMail(db);

        //
        long out_auditAgeActual = auditAgeManager.getAuditAge(); // Возраст аудита БД
        long out_queAvailable = stateManager.getAuditAgeDone();  // Возраст аудита, до которого сформирована исходящая очередь
        long out_sendDone = stateMailManager.getMailSendDone();  // Возраст, до которого исходящая очередь отправлена на сервер
        long in_queInNoAvailable = queIn.getMaxNo();             // До какого номера есть реплики во входящей очереди
        long in_queInNoDone = stateManager.getQueInNoDone();     // Номер реплики, до которого обработана (применена) входящая очередь

        //
        info.put("out_auditAgeActual", out_auditAgeActual);
        info.put("out_queAvailable", out_queAvailable);
        info.put("out_sendDone", out_sendDone);
        info.put("in_queInNoAvailable", in_queInNoAvailable);
        info.put("in_queInNoDone", in_queInNoDone);

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
     * Выявить ситуацию "станцию восстановили из бэкапа" и починить ее
     */
    public void repairAfterBackupRestore(boolean doRepair) throws Exception {
        // ---
        // Сколько входящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long noQueInDir = ((JdxQueFile) queIn).getMaxNoFromDir();

        // Сколько входящих получено у нас в "официальной" очереди
        long noQueInMarked = queIn.getMaxNo();


        // ---
        // Сколько исходящих реплик есть у нас "в закромах", т.е. в рабочем каталоге?
        long ageQueOutDir = ((JdxQueFile) queOut).getMaxNoFromDir();

        // Cколько исходящих реплик есть у нас "официально", т.е. в очереди реплик (в базе)
        long ageQueOut = queOut.getMaxAge();

        // Cколько исходящих реплик отметили как выложенные в очередь
        // При работе репликатора - совпадает с базой, но после запуска ПРОЦЕДУРЫ РЕМОНТА repairAfterBackupRestore - может отличаться.
        // Является флагом, помогающим отследить НЕЗАВЕРШЕННОСТЬ РЕМОНТА.
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long ageQueOutMarked = stateManager.getAuditAgeDone();


        // ---
        // Сколько исходящих реплик фактически отправлено на сервер (спросим у почтового сервера)
        long ageSendDone = mailer.getSendDone("from");

        // Сколько исходящих реплик отмечено как отправленое на сервер
        JdxStateManagerMail stateManagerMail = new JdxStateManagerMail(db);
        long ageSendMarked = stateManagerMail.getMailSendDone();


        // Допускается, если в каталоге для QueIn меньше реплик, чем помечено в очереди QueIn (noQueInMarked >= noQueInDir)
        // Это бывает из-за того, что при получении собственных snapshot-реплик, мы ее не скачиваем (она нам не нужна)
        if (noQueInMarked >= noQueInDir && ageQueOut == ageQueOutDir && ageQueOutMarked == ageQueOutDir && ageSendMarked == ageSendDone) {
            return;
        }

        //
        log.warn("Detected restore from backup, self.wsId: " + wsId);
        log.warn("  noQueInDir: " + noQueInDir);
        log.warn("  noQueInMarked: " + noQueInMarked);
        log.warn("  ageQueOutDir: " + ageQueOutDir);
        log.warn("  ageQueOut: " + ageQueOut);
        log.warn("  ageQueOutMarked: " + ageQueOutMarked);
        log.warn("  ageSendDone: " + ageSendDone);
        log.warn("  ageSendMarked: " + ageSendMarked);

        //
        if (!doRepair) {
            throw new XError("Detected restore from backup, repair needed");
        }

        // ---
        // Обнуляем отметку о пополнении исходящей очереди реплик.
        // После этой отметки ремонт считается НАЧАТЫМ, но НЕ ЗАВЕРШЕННЫМ.
        // ---
        stateManager.setAuditAgeDone(-1);


        // ---
        // Сначала чиним получение реплик
        // ---

        // Берем входящие реплики, которые мы пропустили.
        // Кладем их в свою входящую очередь (потом они будут использованы штатным механизмом).
        // Запрос на сервер повторной отправки входящих реплик - НЕ НУЖНО - они у нас уже есть.
        long count = 0;
        for (long no = noQueInMarked + 1; no <= noQueInDir; no++) {
            log.warn("Repair queIn, self.wsId: " + wsId + ", queIn.no: " + no + " (" + count + "/" + (noQueInDir - noQueInMarked) + ")");

            // Извлекаем входящую реплику из закромов
            IReplica replica = ((JdxQueFile) queIn).readByNoFromDir(no);

            // Пополнение (восстановление) входящей очереди
            queIn.put(replica);

            //
            count = count + 1;
        }

        //
        if (noQueInMarked <= noQueInDir) {
            log.warn("Repair queIn, self.wsId: " + wsId + ", queIn: " + noQueInMarked + " .. " + noQueInDir + ", done count: " + count);
        } else {
            log.info("Repair queIn, self.wsId: " + wsId + ", queIn: " + noQueInMarked + ", nothing to do");
        }

        // ---
        // А теперь применяем все входящие реплики штатным механизмом.
        // Важно их применить, т.к. среди входящих есть и НАШИ СОБСТВЕННЫЕ, но еще не примененные.
        ReplicaUseResult handleQueInUseResult = handleQueIn(true);


        // ---
        // Чиним данные на основе собственного аудита
        // ---

        // Применяем их у себя (это нужно).
        // Восстанавливаем исходящую очередь (вообще-то уже не нужно).
        count = 0;
        for (long age = ageQueOut + 1; age <= ageQueOutDir; age++) {
            log.warn("Repair queOut, self.wsId: " + wsId + ", queOut.age: " + age + " (" + count + "/" + (ageQueOutDir - ageQueOut) + ")");

            // Извлекаем свою реплику из закромов
            IReplica replica = ((JdxQueFile) queOut).readByNoFromDir(age);

            // Применяем реплику у себя.
            // Учтем, до которого возраста мы получили и применили НАШИ СОБСТВЕННЫЕ реплики
            // из ВХОДЯЩЕЙ очереди QueIn и применим ТОЛЬКО не примененные
            if (handleQueInUseResult.lastOwnAgeUsed > 0 && age > handleQueInUseResult.lastOwnAgeUsed) {
                // Пробуем применить собственную реплику
                ReplicaUseResult useResult = useReplica(replica, true);

                //
                if (!useResult.replicaUsed) {
                    throw new XError("Repair queOut, useResult.replicaUsed == false");
                }
                if (useResult.doBreak) {
                    throw new XError("Repair queOut, replica useResult.doBreak == true");
                }
                log.warn("Repair queOut, used: " + age);
            } else {
                log.warn("Repair queOut, already used: " + age + ", skipped");
            }

            // Пополнение (восстановление) исходящей очереди реплик
            queOut.put(replica);

            //
            count = count + 1;
        }

        //
        if (ageQueOut < ageQueOutDir) {
            log.warn("Repair queOut, self.wsId: " + wsId + ", queOut: " + ageQueOut + " .. " + ageQueOutDir + ", done count: " + count);
        } else {
            log.info("Repair queOut, self.wsId: " + wsId + ", queOut: " + ageQueOut + ", nothing to do");
        }


        // ---
        // Чиним отметку аудита.
        // После применения собственных реплик таблица возрастов для таблиц (z_z_age) все ещё содержит устаревшее состояние.
        // ---

        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        long auditAge = auditAgeManager.getAuditAge();
        while (auditAge < ageQueOutDir) {
            auditAge = auditAgeManager.incAuditAge();
            log.warn("Repair auditAge, age: " + auditAge);
        }


        // ---
        // Чиним генераторы.
        // После применения собственных реплик генераторы находятся в устаревшем сосоянии.
        // ---

        UtGenerators utGenerators = new UtGenerators_PS(db, struct);
        utGenerators.repairGenerators();


        // ---
        // Если возраст "отправлено на сервер" меньше, чем фактический размер исходящей очереди -
        // чиним отметку об отправке собственных реплик.
        if (ageSendMarked < ageSendDone) {
            stateManagerMail.setMailSendDone(ageSendDone);
            log.warn("Repair mailSendDone, " + ageSendMarked + " -> " + ageSendDone);
        }


        // ---
        // Чиним отметку о пополнении исходящей очереди реплик.
        // После этой отметки ремонт считается завершенным.
        // ---
        stateManager.setAuditAgeDone(ageQueOutDir);


    }

}
