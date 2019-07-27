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
import jdtx.repl.main.ext.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Контекст рабочей станции
 */
public class JdxReplWs {


    // Правила публикации
    private IPublication publicationIn;
    private IPublication publicationOut;

    //
    private JdxQueCommonFile queIn;
    private JdxQuePersonalFile queOut;

    //
    private Db db;
    protected long wsId;
    protected IJdxDbStruct struct;

    //
    private IMailer mailer;

    //
    private String dataRoot;

    //
    private static Log log = LogFactory.getLog("jdtx");

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

    /**
     * Рабочая станция, настройка
     */
    public void init() throws Exception {
        dataRoot = db.getApp().getRt().getChild("app").getValueString("dataRoot");
        dataRoot = UtFile.unnormPath(dataRoot) + "/";

        // Проверка наличия в БД служебных структур и их версии
        UtDbObjectManager ut = new UtDbObjectManager(db);
        ut.checkReplVerDb();

        // Проверка, что инициализация станции прошла
        ut.checkReplDb();

        // Чтение структуры БД
        IJdxDbStructReader structReader = new JdxDbStructReader();
        structReader.setDb(db);
        IJdxDbStruct structActual = structReader.readDbStruct();

        // Код нашей станции
        DataRecord rec = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "workstation").getCurRec();
        if (rec.getValueLong("id") == 0) {
            throw new XError("Invalid workstation id == 0");
        }
        this.wsId = rec.getValueLong("id");

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
        if (cfgDbPublications != null) {
            String cfgPublicationIn = (String) cfgDbPublications.get("in");
            String cfgPublicationOut = (String) cfgDbPublications.get("out");

            JSONObject cfgDbPublicationIn = (JSONObject) cfgDbPublications.get(cfgPublicationIn);
            JSONObject cfgDbPublicationOut = (JSONObject) cfgDbPublications.get(cfgPublicationOut);

            // Правила публикаций: publicationIn
            IPublication publicationIn = new Publication();
            publicationIn.loadRules(cfgDbPublicationIn, structActual);
            this.publicationIn = publicationIn;

            // Правила публикаций: publicationOut
            IPublication publicationOut = new Publication();
            publicationOut.loadRules(cfgDbPublicationOut, structActual);
            this.publicationOut = publicationOut;
        }


/*
        // Фильтрация структуры: убирание того, чего нет в публикации publicationOut
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();
        UtDbComparer.dbStructDiff(structActual, publicationOut.getData(), structDiffCommon, structDiffNew, structDiffRemoved);
        struct = structDiffCommon;
        structFull = structActual;
*/
        struct = structActual;


        // Проверка версии приложения
        UtAppVersion_DbRW appVersionRW = new UtAppVersion_DbRW(db);
        String appVersionAllowed = appVersionRW.getAppVersionAllowed();
        String appVersionActual = UtRepl.getVersion();
        if (appVersionAllowed.length() == 0) {
            log.warn("appVersionAllowed.length == 0, appVersionActual: " + appVersionActual);
        } else if (appVersionActual.compareToIgnoreCase("SNAPSHOT") == 0) {
            log.warn("appVersionActual == SNAPSHOT, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);
        } else if (appVersionAllowed.compareToIgnoreCase(appVersionActual) != 0) {
            log.info("appVersionAllowed != appVersionActual, appVersionAllowed: " + appVersionAllowed + ", appVersionActual: " + appVersionActual);

            //
            File exeFile = new File("install/JadatexSync-update-" + appVersionAllowed + ".exe");
            log.info("start app update, exeFile: " + exeFile);

            // Запуск обновления
            List<String> res = new ArrayList<>();
            int exitCode = UtReplService.run(res, exeFile.getAbsolutePath(), "/SILENT", "/repl-service-install");

            //
            if (exitCode != 0) {
                System.out.println("exitCode: " + exitCode);
                for (String outLine : res) {
                    System.out.println(outLine);
                }

                //
                throw new XError("Failed to update application " + appVersionActual + " -> " + appVersionAllowed);
            }
        }

        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
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

            // Забираем установочную реплику
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
        UtDbStructApprove dbStructRW = new UtDbStructApprove(db);
        IJdxDbStruct structFixed = dbStructRW.getDbStructFixed();
        IJdxDbStruct structAllowed = dbStructRW.getDbStructAllowed();

        // Сравниваем
        IJdxDbStruct structDiffCommon = new JdxDbStruct();
        IJdxDbStruct structDiffNew = new JdxDbStruct();
        IJdxDbStruct structDiffRemoved = new JdxDbStruct();
        //
        boolean equal_Actual_Allowed = UtDbComparer.dbStructIsEqualTables(structActual, structAllowed);
        boolean equal_Actual_Fixed = UtDbComparer.dbStructDiffTables(structActual, structFixed, structDiffCommon, structDiffNew, structDiffRemoved);

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
            struct_rw.saveToFile(structActual, dataRoot + "temp/dbStruct.actual.xml");
            struct_rw.saveToFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
            struct_rw.saveToFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");

            //
            return false;
        }

        // Начинаем фиксацию структуры -
        // реальная совпадает с разрешенной, но отличается от зафиксированной
        log.info("dbStructApplyFixed, start");

        // Обеспечиваем порядок сортировки таблиц с учетом foreign key (при выгрузке snapsot это важно)
        List<IJdxTable> tablesNew = JdxUtils.sortTablesByReference(structDiffNew.getTables());


        // Подгоняем структуру аудита под реальную структуру
        db.startTran();
        try {
            //
            UtDbObjectManager objectManager = new UtDbObjectManager(db, structActual);

            //
            long n;

            // Удаляем аудит для удаленных таблиц
            ArrayList<IJdxTable> tablesRemoved = structDiffRemoved.getTables();
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
     * Отслеживаем и обрабатываем свои изменения,
     * формируем исходящие реплики
     */
    public void handleSelfAudit() throws Exception {
        log.info("handleSelfAudit, wsId: " + wsId);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        UtDbStructApprove dbStructRW = new UtDbStructApprove(db);

        // Если в стостоянии "я замолчал", то молчим
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);
        if (utmm.isMute()) {
            log.warn("handleSelfAudit, workstation is mute");
            return;
        }

        // Проверяем совпадение структур
        IJdxDbStruct structAllowed = dbStructRW.getDbStructAllowed();
        IJdxDbStruct structFixed = dbStructRW.getDbStructFixed();

        // Проверяем совпадает ли реальная структура БД с разрешенной структурой
        if (!UtDbComparer.dbStructIsEqual(struct, structAllowed)) {
            log.warn("handleSelfAudit, database structActual <> structAllowed");
            // Для справки/отладки - структуры в файл
            JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
            struct_rw.saveToFile(struct, dataRoot + "temp/dbStruct.actual.xml");
            struct_rw.saveToFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
            struct_rw.saveToFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");
            //
            return;
        }
        // Проверяем совпадает ли реальная структура БД с фиксированной структурой
        if (!UtDbComparer.dbStructIsEqual(struct, structFixed)) {
            log.warn("handleSelfAudit, database structActual <> structFixed");
            // Для справки/отладки - структуры в файл
            JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
            struct_rw.saveToFile(struct, dataRoot + "temp/dbStruct.actual.xml");
            struct_rw.saveToFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
            struct_rw.saveToFile(structFixed, dataRoot + "temp/dbStruct.fixed.xml");
            //
            return;
        }


        // Формируем реплики (по собственным изменениям)
        db.startTran();
        try {
            long count = 0;

            // Узнаем (и заодно фиксируем) возраст своего аудита
            UtAuditAgeManager uta = new UtAuditAgeManager(db, struct);
            long auditAgeTo = uta.markAuditAge();

            // До какого возраста сформировали реплики для своего аудита
            JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
            long auditAgeFrom = stateManager.getAuditAgeDone();

            //
            // если нет публикаций, то аудит копится, а потом не получается ничего сказать
            for (long age = auditAgeFrom + 1; age <= auditAgeTo; age++) {
                IReplica replica = utRepl.createReplicaFromAudit(wsId, publicationOut, age);

                // Пополнение исходящей очереди реплик
                queOut.put(replica);

                //
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
    public void handleQueIn() throws Exception {
        log.info("handleQueIn, self.wsId: " + wsId);

        //
        UtDbStructApprove dbStructRW = new UtDbStructApprove(db);
        UtAppVersion_DbRW appVersionRW = new UtAppVersion_DbRW(db);
        UtAuditApplyer applyer = new UtAuditApplyer(db, struct);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);

        // Проверяем совпадает ли реальная структура БД с разрешенной структурой
        boolean dbStructIsEqual = true;
        IJdxDbStruct structAllowed = dbStructRW.getDbStructAllowed();
        if (!UtDbComparer.dbStructIsEqual(struct, structAllowed)) {
            dbStructIsEqual = false;
        }

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
            boolean replicaUsed = true;
            boolean doBreak = false;

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
                    // Надо научиться при отстсвии бинаарника снова искать последнюю реплику и распаковывать бинарник непосредственно перед запуском
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
                    appVersionRW.setAppVersionAllowed(appVersionAllowed);


                    // ===
                    // Выкладывание реплики "Я принял обновление"
                    reportMuteDone(JdxReplicaType.UPDATE_APP_DONE);


                    //
                    break;
                }
                case JdxReplicaType.MUTE: {
                    // Реакция на команду - перевод в режим "MUTE"

                    // Последняя обработка собственного аудита
                    handleSelfAudit();

                    // Переход в состояние "Я замолчал"
                    muteManager.muteWorkstation();

                    // Выкладывание реплики "Я замолчал"
                    reportMuteDone(JdxReplicaType.MUTE_DONE);

                    //
                    break;
                }
                case JdxReplicaType.UNMUTE: {
                    // Реакция на команду - отключение режима "MUTE"

                    // Выход из состояния "Я замолчал"
                    muteManager.unmuteWorkstation();

                    // Выкладывание реплики "Я уже не молчу"
                    reportMuteDone(JdxReplicaType.UNMUTE_DONE);

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
                        dbStructRW.setDbStructAllowed(struct);
                    } finally {
                        stream.close();
                    }

                    // Проверяем структуры и пересоздаем аудит
                    if (!dbStructApplyFixed()) {
                        // Если пересоздать аудит не удалось (структуры не обновлены или по иным причинам),
                        // то не метим реплтику как использованную
                        log.warn("handleQueIn, dbStructApplyFixed <> true");
                        replicaUsed = false;
                        doBreak = true;
                        break;
                    }

                    // Выкладывание реплики "структура принята"
                    reportMuteDone(JdxReplicaType.SET_DB_STRUCT_DONE);

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

                    // Пришла конфигурация для нашей станции?
                    if (destinationWsId == wsId) {
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
                        reportMuteDone(JdxReplicaType.SET_CFG_DONE);

                        // Обновление конфигурации требует переинициализацию репликатора, поэтому прерываемся
                        doBreak = true;
                    }

                    //
                    break;
                }
                case JdxReplicaType.IDE:
                case JdxReplicaType.SNAPSHOT: {
                    // Реальная структура базы НЕ совпадает с разрешенной структурой
                    if (!dbStructIsEqual) {
                        log.warn("handleQueIn, structActual <> structAllowed");
                        replicaUsed = false;
                        doBreak = true;
                        // Для справки/отладки - структуры в файл
                        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
                        struct_rw.saveToFile(struct, dataRoot + "temp/dbStruct.actual.xml");
                        struct_rw.saveToFile(structAllowed, dataRoot + "temp/dbStruct.allowed.xml");
                        struct_rw.saveToFile(dbStructRW.getDbStructFixed(), dataRoot + "temp/dbStruct.fixed.xml");
                        //
                        break;
                    }

                    // Свои собственные установочные реплики можно не применять
                    if (replica.getInfo().getWsId() == wsId && replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
                        break;
                    }

                    // Реальная структура базы НЕ совпадает со структурой, с которой была подготовлена реплика
                    JdxReplicaReaderXml.readReplicaInfo(replica);
                    String replicaStructCrc = replica.getInfo().getDbStructCrc();
                    String dbStructActualCrc = UtDbComparer.calcDbStructCrc(struct);
                    if (replicaStructCrc.compareToIgnoreCase(dbStructActualCrc) != 0) {
                        log.error("handleQueIn, database.structCrc <> replica.structCrc, expected: " + dbStructActualCrc + ", actual: " + replicaStructCrc);

                        //
                        return;
                    }

                    // todo: Проверим протокол репликатора, с помощью которого была подготовлена репоика
                    // String protocolVersion = (String) replica.getInfo().getProtocolVersion();
                    // if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
                    //      throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
                    //}


                    // Применение реплик
                    InputStream inputStream = null;
                    try {
                        // Распакуем XML-файл из Zip-архива
                        inputStream = UtRepl.getReplicaInputStream(replica);

                        //
                        JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

                        //
                        applyer.applyReplica(replicaReader, publicationIn, wsId);
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

            // Реплика использованна?
            if (replicaUsed) {
                // Отметим применение реплики
                stateManager.setQueInNoDone(no);
                //
                count++;
            } else {
                // Не отмечаем
                log.info("handleQueIn, replica not used");
            }

            // Надо останавливаться?
            if (doBreak) {
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
    }

    private String loadStringFromSream(InputStream stream) throws Exception {
        StringLoader ldr = new StringLoader();
        UtLoad.fromStream(ldr, stream);
        return ldr.getResult();
    }


    /**
     * Рабочая станция: отправка системной реплики
     * (например, ответа "я замолчал" или "Я уже не молчу")
     * в исходящую очередь
     */
    public void reportMuteDone(int replicaType) throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        UtRepl utRepl = new UtRepl(db, struct);

        // Весь свой аудит предварительно выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        // Искусственно увеличиваем возраст (системная реплика сдвигает возраст БД на 1)
        long age = utRepl.incAuditAge();
        log.info("reportMuteDone, new age: " + age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(replicaType);
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);

        //
        utRepl.createOutputXML(replica);
        utRepl.closeOutput();

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


        // Физически забираем данные
        receiveInternal(mailerLocal);
    }


    // Физически забираем данные
    public void receive() throws Exception {
        receiveInternal(mailer);
    }


    void receiveInternal(IMailer mailer) throws Exception {
        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getSrvState("to");

        //
        long count = 0;
        for (long no = selfReceivedNo + 1; no <= srvAvailableNo; no++) {
            log.info("receive, wsId: " + wsId + ", receiving.no: " + no);

            // Информация о реплике с почтового сервера
            ReplicaInfo info = mailer.getReplicaInfo("to", no);

            // Нужно ли скачивать эту реплику с сервера?
            IReplica replica;
            if (info.getWsId() == wsId && info.getReplicaType() == JdxReplicaType.SNAPSHOT) {
                // Свои собственные установочные реплики можно не скачивать (и не применять)
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


        //
        mailer.pingRead("to");
        //
        Map info = getInfoWs();
        mailer.setWsInfo(info);


        //
        if (selfReceivedNo <= srvAvailableNo) {
            log.info("UtMailer, wsId: " + wsId + ", receive.no: " + selfReceivedNo + " .. " + srvAvailableNo + ", done count: " + count);
        } else {
            log.info("UtMailer, wsId: " + wsId + ", receive.no: " + selfReceivedNo + ", nothing to receive");
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


        // Сколько уже отправлено на сервер
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
        // Узнаем сколько уже отправлено на сервер
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        long srvSendAge = stateManager.getMailSendDone();

        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxAge();

        // Физически отправляем данные
        sendInternal(mailer, srvSendAge + 1, selfQueOutAge, true);
    }

    void sendInternal(IMailer mailer, long age_from, long age_to, boolean doMarkDone) throws Exception {
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);

        //
        long count = 0;
        for (long age = age_from; age <= age_to; age++) {
            log.info("UtMailer, wsId: " + wsId + ", sending.age: " + age);

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

        //
        mailer.pingWrite("from");
        //
        Map info = getInfoWs();
        mailer.setWsInfo(info);

        //
        if (age_from <= age_to) {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + age_from + " .. " + age_to + ", done count: " + count);
        } else {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + age_from + ", nothing to send");
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
            long in_mailAvailable = mailer.getSrvState("to");    // Сколько есть на сервере в ящике для станции
            info.put("in_mailAvailable", in_mailAvailable);
        } catch (Exception e) {
            info.put("in_mailAvailable", e.getMessage());
        }

        //
        return info;
    }


}
