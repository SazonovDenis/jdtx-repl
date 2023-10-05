package jdtx.repl.main.ext

import jandcode.app.*
import jandcode.dbm.*
import jandcode.dbm.data.*
import jandcode.dbm.db.*
import jandcode.jc.*
import jandcode.utils.*
import jandcode.utils.error.*
import jandcode.utils.variant.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.cleaner.*
import jdtx.repl.main.api.mailer.*
import jdtx.repl.main.api.manager.*
import jdtx.repl.main.api.publication.*
import jdtx.repl.main.api.que.*
import jdtx.repl.main.api.ref_manager.*
import jdtx.repl.main.api.repair.*
import jdtx.repl.main.api.replica.*
import jdtx.repl.main.api.struct.*
import jdtx.repl.main.api.util.*
import jdtx.repl.main.gen.*
import jdtx.repl.main.service.*
import org.apache.commons.io.*
import org.apache.log4j.*
import org.json.simple.*

/**
 * Обертка для вызовов утилиты jc с командной строки
 */
class Jdx_Ext extends ProjectExt {


    private AppProjectExt _appProjectExt

    public Jdx_Ext() {
        if (UtFile.exists("_log.properties")) {
            UtLog.loadProperties("_log.properties")
            UtLog.logOn()
        } else if (UtFile.exists("log.properties")) {
            UtLog.loadProperties("log.properties")
            UtLog.logOn()
        } else {
            System.out.println("Файл log.properties или _log.properties не найден, логирование отключено")
        }

        MDC.put("serviceName", "jc")
    }

    /**
     * Расширение для приложения
     */
    AppProjectExt getAppExt() {
        if (_appProjectExt == null) {
            _appProjectExt = createExt(AppProjectExt.class) as AppProjectExt
        }
        return _appProjectExt
    }

    /**
     * Ссылка на текущее приложение
     */
    App getApp() {
        return getAppExt().getInst()
    }


    void repl_info(IVariantMap args) {
        // БД
        Db db = app.service(ModelService.class).model.getDb()

        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        db.connect()

        //
        try {
            // Сервер
            try {
                System.out.println("")
                System.out.println("Сервер")

                //
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()

                //
                System.out.println("srv.dataRoot:" + srv.getDataRoot())

                //
                UtRepl urRepl = new UtRepl(db, null)
                UtData.outTable(urRepl.getInfoSrv())
            } catch (Exception e) {
                System.out.println(e.message)
            }


        } catch (Exception e) {
            System.out.println(jdtx.repl.main.ut.Ut.getExceptionMessage(e))
        } finally {
            db.disconnect()
        }
    }


    void repl_check(IVariantMap args) {
        boolean doPublications = !args.isValueNull("publications")
        boolean doTables = !args.isValueNull("tables")
        boolean doFields = !args.isValueNull("fields")
        String jsonFileName = args.getValueString("file")


        // БД
        Db db = app.service(ModelService.class).model.getDb()

        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        db.connect()

        //
        try {
            IJdxDbStructReader reader = new JdxDbStructReader()
            reader.setDb(db)
            IJdxDbStruct struct = reader.readDbStruct()

            //
            if (doTables) {
                for (IJdxTable t : struct.getTables()) {
                    System.out.println(t.getName())
                    if (doFields) {
                        for (IJdxField f : t.getFields()) {
                            System.out.println("  " + f.getName() + " " + f.getJdxDatatype() + "[" + f.getSize() + "]" + " (" + f.getDbDatatype() + ")")
                        }
                    }
                }
                System.out.println()
            }

            //
            if (doPublications) {
                JSONObject cfg = UtRepl.loadAndValidateJsonFile(jsonFileName)

                //
                System.out.println("Publication: in")
                IPublicationRuleStorage publicationIn = PublicationRuleStorage.loadRules(cfg, struct, "in")
                JSONObject cfgPublicationRulesIn = PublicationRuleStorage.extractRulesByName(cfg, "in")
                UtPublicationRule.checkValid(cfgPublicationRulesIn, publicationIn, struct)

                //
                System.out.println("Publication: out")
                IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfg, struct, "out")
                JSONObject cfgPublicationRulesOut = PublicationRuleStorage.extractRulesByName(cfg, "out")
                UtPublicationRule.checkValid(cfgPublicationRulesOut, publicationOut, struct)
            }

        } catch (Exception e) {
            e.printStackTrace()
            System.out.println(jdtx.repl.main.ut.Ut.getExceptionMessage(e))
        } finally {
            db.disconnect()
        }
    }

    void repl_create(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String guid = args.getValueString("guid")
        String mailUrl = args.getValueString("mail")
        String name = args.getValueString("name")
        //
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        //
        if (guid == null || guid.length() == 0) {
            throw new XError("Не указан [guid] - идентификатор репликационной сети")
        }
        //
        if (mailUrl == null || mailUrl.length() == 0) {
            throw new XError("Не указан [mail] - почтовый сервер")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            //
            IJdxDbStructReader dbStructReader = new JdxDbStructReader()
            dbStructReader.setDb(db)
            IJdxDbStruct struct = dbStructReader.readDbStruct()

            // Создаем базовые объекты рабочей станции
            UtRepl utRepl = new UtRepl(db, struct)
            utRepl.checkNotOwnId()
            utRepl.dropReplication()
            utRepl.createReplication(wsId, guid)

            // Начальный конфиг cfg_ws рабочей станции
            JSONObject cfg = new JSONObject()
            cfg.put("url", mailUrl)
            JSONObject cfgApp = new JSONObject()
            cfgApp.put("autoUseRepairReplica", true)
            cfg.put("app", cfgApp)
            // Записываем конфиг
            CfgManager cfgManager = new CfgManager(db)
            cfgManager.setSelfCfg(cfg, CfgType.WS)

            // Создаем окружение для рабочей станции
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()
            ws.firstSetup()

            // Создаем окружение для сервера и добавляем рабочую станцию для сервера (серверную, wsId = 1)  в общий список
            if (wsId == JdxReplSrv.SERVER_WS_ID) {
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()
                srv.firstSetup()

                // Добавляем рабочую станцию wsId = 1 в список сервера
                srv.addWorkstation(wsId, name)
            }
        } finally {
            db.disconnect()
        }
    }


    void repl_add_ws(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String name = args.getValueString("name")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (name == null || name.length() == 0) {
            throw new XError("Не указано [name] - название рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.addWorkstation(wsId, name)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_restore_ws(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String cfgSnapshotFileName = args.getValueString("cfg_snapshot")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            // Узнаем правила для формирования snapshot
            IPublicationRuleStorage ruleSnapshot
            if (cfgSnapshotFileName != null && cfgSnapshotFileName.length() != 0) {
                ruleSnapshot = srv.getCfgSnapshot(cfgSnapshotFileName)
            } else {
                ruleSnapshot = srv.createCfgSnapshot(wsId)
            }

            // Сформируем snapshot
            srv.restoreWorkstation(wsId, ruleSnapshot)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_ws_enable(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.enableWorkstation(wsId)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_ws_disable(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.disableWorkstation(wsId)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_find_record(IVariantMap args) {
        String recordId = args.getValueString("id")
        String dirsName = args.getValueString("dir")
        String outFileName = args.getValueString("out")
        boolean skipOprDel = args.getValueBoolean("skipDel")
        boolean findLastOne = args.getValueBoolean("lastOne")
        //
        if (recordId == null || recordId.length() == 0) {
            throw new XError("Не указан [id] - id записи")
        }
        if (dirsName == null || dirsName.length() == 0) {
            throw new XError("Не указан [dir] - каталоги для поиска")
        }
        String tableName = recordId.split(":")[0]
        String recordIdRefStr = recordId.substring(tableName.length() + 1)

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()

            // Выполнение команды
            try {
                //
                IRefManager refManager = app.service(RefManagerService.class)

                // Имя таблицы как оно есть в структуре
                tableName = ws.struct.getTable(tableName).getName()

                // Преобразуем recordId в ссылку (в пару ws:id)
                if (!recordIdRefStr.contains(":")) {
                    // Передали просто id - превратим ее в "каноническую" форму (пару ws:id)
                    long tableId = Long.parseLong(recordIdRefStr)
                    JdxRef tableIdRef = refManager.get_ref(tableName, tableId)
                    recordIdRefStr = tableIdRef.toString()
                }
                println("В таблице: " + tableName + " ищем: " + recordIdRefStr)

                // Имя файла-результата
                if (outFileName == null || outFileName.length() == 0) {
                    outFileName = tableName + "_" + recordIdRefStr.replace(":", "_") + ".zip"
                }

                // Ищем запись и формируем реплику на вставку
                UtRepl utRepl = new UtRepl(db, ws.struct)
                IReplica replica = utRepl.findRecordInReplicas(tableName, recordIdRefStr, dirsName, skipOprDel, findLastOne, outFileName)

                //
                System.out.println("Файл с репликой - результатами поиска сформирован: " + replica.data.getAbsolutePath())
            } catch (Exception e) {
                e.printStackTrace()
                throw e
            }

        } finally {
            db.disconnect()
        }
    }

    void repl_replica_request(IVariantMap args) {
        long wsId = args.getValueLong("ws", 0)
        //
        if (!args.isValueNull("from") && !args.isValueNull("no")) {
            throw new XError("Не нужно указывать параметр [no], если указаны параметры [from, to]")
        }
        long no_from
        long no_to
        if (!args.isValueNull("from")) {
            no_from = args.getValueLong("from", 0)
            if (!args.isValueNull("to")) {
                no_to = args.getValueLong("to", 0)
            } else {
                no_to = -1
            }
        } else {
            no_from = args.getValueLong("no", 0)
            no_to = args.getValueLong("no", 0)
        }

        //
        String executor = args.getValueString("executor")
        //
        String box = args.getValueString("box")
        //
        boolean recreate = args.get("recreate", false)

        //
        if (no_from == 0 || no_to == 0) {
            throw new XError("Не указан номер реплики [no|from|from,to]")
        }
        if (executor == null || executor.length() == 0) {
            throw new XError("Не указан [executor] - исполнитель [srv|ws]")
        }
        if (box == null || box.length() == 0) {
            throw new XError("Не указан [box] - ящик")
        }


        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {

            // Выполнение команды
            try {
                IMailer mailer

                if (wsId == 0) {
                    // Рабочая станция и её mailer
                    JdxReplWs ws = new JdxReplWs(db)
                    ws.init()
                    //
                    mailer = ws.getMailer()
                } else {
                    // Сервер и его mailer-ы
                    JdxReplSrv srv = new JdxReplSrv(db)
                    srv.init()
                    mailer = srv.mailerList.get(wsId)
                }

                // Запрос
                RequiredInfo requiredInfo = new RequiredInfo()
                requiredInfo.requiredFrom = no_from
                requiredInfo.requiredTo = no_to
                requiredInfo.executor = executor
                requiredInfo.recreate = recreate

                // Отправляем
                mailer.setSendRequired(box, requiredInfo)

                //
                System.out.println("Запрос отправлен: " + requiredInfo)
            } catch (Exception e) {
                e.printStackTrace()
                throw e
            }

        } finally {
            db.disconnect()
        }
    }

    void repl_allow_repair(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Сервер и его mailer-ы
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()
            IMailer mailer = srv.mailerList.get(wsId)

            // Узнаем guid ремонта
            JdxRepairInfoManager repairInfoManager = new JdxRepairInfoManager(mailer)
            String wsRepairGuid = repairInfoManager.getRepairGuid()

            //
            if (wsRepairGuid == null) {
                throw new XError("Рабочая станция [" + wsId + "] не запрашивала разрешение на ремонт")
            }

            // Разрешиаем
            repairInfoManager.setRepairAllowed(wsRepairGuid)

            //
            println("Ремонт разрешен, wsId: " + wsId + ", guid: " + wsRepairGuid)
        } finally {
            db.disconnect()
        }
    }

    void repl_clean(IVariantMap args) {
        long queInUsedLast = args.getValueLong("used")
        if (queInUsedLast == 0L) {
            queInUsedLast = Long.MAX_VALUE
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            // Сервер
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            // Анализ старых реплик
            Map<Long, JdxCleanTaskWs> cleanupTasks = srv.srvCleanupReplPrepareTask(queInUsedLast)

            // Печатаем
            println("Cleanup tasks:")
            for (long wsId : cleanupTasks.keySet()) {
                JdxCleanTaskWs cleanupTask = cleanupTasks.get(wsId)
                println("  ws: " + wsId + ", cleanupTask: " + cleanupTask)
            }
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_clean_ws(IVariantMap args) {
        long queInUsedLast = args.getValueLong("used")
        if (queInUsedLast == 0L) {
            queInUsedLast = Long.MAX_VALUE
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            // Сервер
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            // Отправка команды удаления на рабочие станции.
            srv.srvCleanupReplWs(queInUsedLast)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_clean_srv(IVariantMap args) {
        long queInUsedLast = args.getValueLong("used")
        if (queInUsedLast == 0L) {
            queInUsedLast = Long.MAX_VALUE
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            // Сервер
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            // Выполнение удаления старых реплик на сервере
            srv.srvCleanupReplSrv(queInUsedLast)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_replica_use(IVariantMap args) {
        String replicaFileName = args.getValueString("file")
        //
        if (replicaFileName == null || replicaFileName.length() == 0) {
            throw new XError("Не указан [file] - файл с репликой")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()

            // Выполнение команды
            try {
                File f = new File(replicaFileName)
                ws.useReplicaFile(f)

            } catch (Exception e) {
                e.printStackTrace()
                throw e
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_replica_recreate(IVariantMap args) {
        String replicaFileName = args.getValueString("file")
        //
        long no = args.getValueLong("no")
        if (no == 0L) {
            throw new XError("Не указан [no] - номер реплики")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()

            // Выполнение команды
            try {
                if (replicaFileName == null || replicaFileName.length() == 0) {
                    // Чиним файл реплики
                    IReplica replica = ws.recreateReplica(no)

                    //
                    JdxQue.infoReplica(replica)

                    // Копируем куда просили
                    File fileDest = new File(replicaFileName)
                    FileUtils.copyFile(replica.getData(), fileDest)
                    System.out.println("Replica file copied: " + fileDest.getCanonicalPath())
                } else {
                    // Чиним файл в очереди
                    IReplica replica = ws.recreateQueOutReplica(no)

                    //
                    JdxQue.infoReplica(replica)
                }

            } catch (Exception e) {
                e.printStackTrace()
                throw e
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_mail_ws(IVariantMap args) {
        String dirName = args.getValueString("dir")
        //long noFrom = args.getValueLong("from", 0)
        //long noTo = args.getValueLong("to", 0)
        //boolean doMarkDone = args.getValueBoolean("mark", false)
        boolean doReceive = args.getValueBoolean("receive", false)
        boolean doSend = args.getValueBoolean("send", false)
        //
        if (dirName == null || dirName.length() == 0) {
            throw new XError("Не указан [dir] - почтовый каталог")
        }
        dirName = UtFile.unnormPath(dirName) + "/"

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()

            //
            if (doReceive) {
                System.out.println("Отправляем свои реплики")
                ws.replicasSendDir(dirName)
            }

            //
            if (doSend) {
                System.out.println("Забираем входящие реплики")
                ws.replicasReceiveDir(dirName)
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_mail_srv(IVariantMap args) {
        String dirName = args.getValueString("dir")
        //long noFrom = args.getValueLong("from", 0)
        //long noTo = args.getValueLong("to", 0)
        //boolean doMarkDone = args.getValueBoolean("mark", false)
        //long destinationWsId = args.getValueLong("ws")
        boolean doReceive = args.getValueBoolean("receive", false)
        boolean doSend = args.getValueBoolean("send", false)
        //
        if (dirName == null || dirName.length() == 0) {
            throw new XError("Не указан [dir] - почтовый каталог")
        }
        UtFile.unnormPath(dirName) + "/"
        //
        //if (doMarkDone && destinationWsId == 0L) {
        //    throw new XError("Не указан [ws] - код рабочей станции, для которой готовятся реплики")
        //}

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            // ---
            // Сервер
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            if (doReceive) {
                System.out.println("Чтение входящих очередей")
                srv.srvReplicasReceiveDir(dirName)
            }

            //
            if (doSend) {
                System.out.println("Рассылка исходящих очередей")
                srv.srvReplicasSendDir(dirName)
            }
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    boolean repl_mail_create(IVariantMap args) {
        boolean result = true

        String pass = args.getValue("pass")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            try {
                MailerHttp srvMailer = (MailerHttp) srv.selfMailer
                srvMailer.createGuid(srv.srvGuid, pass)
                System.out.println("Guid: " + srv.srvGuid + " - created ok")
            } catch (Exception e) {
                if (!UtJdxErrors.errorIs_GuidAlreadyExists(e)) {
                    System.out.println("Guid: " + srv.srvGuid + ", error: " + e.message)
                    result = false
                } else {
                    System.out.println("Guid: " + srv.srvGuid + " - already exists")
                }
            }

            //
            String[] boxes = ["from", "to001", "to"]
            for (Map.Entry<Long, IMailer> en : srv.mailerList.entrySet()) {
                long wsId = en.getKey()
                MailerHttp wsMailer = (MailerHttp) en.getValue()

                for (String box : boxes) {
                    try {
                        wsMailer.createMailBox(box)
                        System.out.println("wsId: " + wsId + ", box: " + box + " - created ok")
                    } catch (Exception e) {
                        if (!UtJdxErrors.errorIs_BoxAlreadyExists(e)) {
                            System.out.println("wsId: " + wsId + ", box: " + box + ", error: " + e.message)
                            result = false
                        } else {
                            System.out.println("wsId: " + wsId + ", box: " + box + " - already exists")
                        }
                    }

                    //
                    System.out.println("")
                }

            }

        } finally {
            db.disconnect()
        }

        //
        return result
    }

    void repl_mail_check(IVariantMap args) {
        String guid = args.getValueString("guid")
        String mailUrl = args.getValueString("mail")
        //
        if (mailUrl == null || mailUrl.length() == 0) {
            throw new XError("Не указан [mail] - почтовый сервер")
        }

        UtMail.checkMailServer(mailUrl, guid)
    }

    //\//////////////////////
    //\//////////////////////
    //\//////////////////////
    //\//////////////////////
    //\//////////////////////
    // todo почему нет команды, чтобы это сделать прямо на рабочей станции (с отчетом на сервер)?
    // todo проверить, чтобы все команды по настройке станции (send***) имели аналог на самой станции (с отчетом на сервер)
    //\//////////////////////
    //\//////////////////////
    //\//////////////////////
    void repl_ws_mute(IVariantMap args) {
        if (args.isValueNull("ws")) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        long destinationWsId = args.getValueLong("ws")
        //
        String queName = args.getValueString("que")
        if (queName == null || queName.length() == 0) {
            queName = UtQue.SRV_QUE_COMMON
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Останавливаем процесс и удаляем службу
            ReplServiceState serviceState = saveServiceState(db, args)
            try {
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()

                //
                srv.srvSendWsMute(destinationWsId, queName)

            } finally {
                restoreServiceState(serviceState, db, args)
                db.disconnect()
            }
        } catch (Exception e) {
            e.printStackTrace()
        }
    }


    void repl_ws_unmute(IVariantMap args) {
        if (args.isValueNull("ws")) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        long destinationWsId = args.getValueLong("ws")
        //
        String queName = args.getValueString("que")
        if (queName == null || queName.length() == 0) {
            queName = UtQue.SRV_QUE_COMMON
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvSendWsUnmute(destinationWsId, queName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    // todo: зачем он, если есть repl-ws-mute
    void repl_mute(IVariantMap args) {
        boolean doWaitMute = !args.isValueNull("wait")
        boolean doWaitAge = !args.isValueNull("age")

        //
        if (doWaitAge && !doWaitMute) {
            throw new XError("Режим [age] доступен только в режиме [wait]")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            // Покажем как сейчас
            srv.srvMuteState(false, false, 0)

            // Запущенный процесс мешает
            UtReplService.stop(false)

            // Отправим команду MUTE (первый раз)
            srv.srvMuteAll()

            // Теперь нужен запущенный процесс
            UtReplService.start()

            // Ждем результат
            long age = 0
            if (doWaitMute) {
                age = srv.srvMuteState(true, false, 0)
            }

            // Еще раз отправим команду MUTE и ждем результат
            if (doWaitAge) {
                age = age + 1

                // Запущенный процесс мешает
                UtReplService.stop(false)

                // Отправим команду
                srv.srvMuteAll()

                // Теперь нужен запущенный процесс
                UtReplService.start()

                // Ждем результат
                srv.srvMuteState(true, false, age)
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_unmute(IVariantMap args) {
        boolean doWaitUnmute = !args.isValueNull("wait")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            // Покажем как сейчас
            srv.srvMuteState(false, false, 0)

            // Запущенный процесс мешает
            UtReplService.stop(false)

            // Отправим команду UNMUTE
            srv.srvUnmuteAll()

            // Теперь нужен запущенный процесс
            UtReplService.start()

            // Ждем результат
            if (doWaitUnmute) {
                srv.srvMuteState(false, true, 0)
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_mute_state(IVariantMap args) {
        boolean doWaitMute = !args.isValueNull("wait")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvMuteState(doWaitMute, false, 0)

        } finally {
            db.disconnect()
        }
    }

    void repl_set_struct(IVariantMap args) {
        String cfgFileName = args.getValueString("file")
        if (cfgFileName == null || cfgFileName.length() == 0) {
            throw new XError("Не указан [file] - конфиг-файл")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvSetDbStruct(cfgFileName, UtQue.SRV_QUE_OUT001)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_send_struct(IVariantMap args) {
        String cfgFileName = args.getValueString("file")
        if (cfgFileName == null || cfgFileName.length() == 0) {
            throw new XError("Не указан [file] - конфиг-файл")
        }
        //
        long destinationWsId = args.getValueLong("ws")
        if (destinationWsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        //
        String queName = args.getValueString("que")
        if (queName == null || queName.length() == 0) {
            queName = UtQue.SRV_QUE_OUT001
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvSetAndSendDbStruct(cfgFileName, destinationWsId, queName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_send_cfg(IVariantMap args) {
        String cfgFileName = args.getValueString("file")
        if (cfgFileName == null || cfgFileName.length() == 0) {
            throw new XError("Не указан [file] - конфиг-файл")
        }
        //
        String cfgType = args.getValueString("cfg")
        if (cfgType == null || cfgType.length() == 0) {
            throw new XError("Не указан [cfg] - вид конфиг-файла")
        }
        CfgType.validateCfgCode(cfgType)
        //
        long destinationWsId = args.getValueLong("ws")
        if (destinationWsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        //
        String queName = args.getValueString("que")
        if (queName == null || queName.length() == 0) {
            queName = UtQue.SRV_QUE_OUT001
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvSetAndSendCfg(cfgFileName, cfgType, destinationWsId, queName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    /**
     * Используется для
     * 1) задания конфигов серврера при инициализации репликации
     * 1) задания конфига CfgType.WS и CfgType.DECODE станции при ее инициализации
     */
    void repl_set_cfg(IVariantMap args) {
        String cfgFileName = args.getValueString("file")
        if (cfgFileName == null || cfgFileName.length() == 0) {
            throw new XError("Не указан [file] - конфиг-файл")
        }
        //
        String cfgType = args.getValueString("cfg")
        if (cfgType == null || cfgType.length() == 0) {
            throw new XError("Не указан [cfg] - вид конфиг-файла")
        }
        CfgType.validateCfgCode(cfgType)

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Обновляем конфиг своей рабочей станции
            JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfgFileName)
            CfgManager cfgManager = new CfgManager(db)
            cfgManager.setSelfCfg(cfg, cfgType)

        } finally {
            db.disconnect()
        }
    }


    void repl_app_version(IVariantMap args) {
        System.out.println("app_version: " + UtRepl.getVersion())
    }

    void repl_app_update(IVariantMap args) {
        System.out.println("app_version: " + UtRepl.getVersion())

        //
        String exeFileName = args.getValueString("file")
        if (exeFileName == null || exeFileName.length() == 0) {
            throw new XError("Не указан [file] - файл для установки обновления")
        }
        //
        String queName = args.getValueString("que")
        if (queName == null || queName.length() == 0) {
            queName = UtQue.SRV_QUE_COMMON
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvAppUpdate(exeFileName, queName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    /**
     * Разослать план-команду на слияние дубликатов
     */
    void repl_merge_request(IVariantMap args) {
        String planFileName = args.getValueString("file")
        if (planFileName == null || planFileName.length() == 0) {
            throw new XError("Не указан [file] - файл с планом слияния")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvMergeRequest(planFileName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_snapshot_request(IVariantMap args) {
        long destinationWsId = args.getValueLong("ws")
        String tableNames = args.getValueString("tables")
        if (destinationWsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (tableNames == null || tableNames.length() == 0) {
            throw new XError("Не указаны [tables] - таблицы в БД")
        }
        String queName = args.getValueString("que")
        if (queName == null || queName.length() == 0) {
            queName = UtQue.SRV_QUE_COMMON
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvRequestSnapshot(destinationWsId, tableNames, queName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_snapshot_create(IVariantMap args) {
        String outFileName = args.getValueString("file")
        if (outFileName == null || outFileName.length() == 0) {
            throw new XError("Не указан [file] - Результирующий файл с репликой")
        }
        String tableNames = args.getValueString("tables")
        if (tableNames == null || tableNames.length() == 0) {
            throw new XError("Не указаны [tables] - таблицы в БД")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()
            ws.wsCreateSnapshot(tableNames, outFileName)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_snapshot_send(IVariantMap args) {
        long destinationWsId = args.getValueLong("ws")
        String tableNames = args.getValueString("tables")
        if (tableNames == null || tableNames.length() == 0) {
            throw new XError("Не указаны [tables] - таблицы в БД")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            if (destinationWsId != 0L) {
                // Запросили для конкретной станции.
                // Отправляем на станцию через сервер, в очередь QUE_OUT001
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()
                srv.srvSendSnapshot(destinationWsId, tableNames)
            } else {
                // Запросили не для конкретной станции
                // Отправляем с любой станции, всем в очередь QUE_COMMON
                JdxReplWs ws = new JdxReplWs(db)
                ws.init()
                ws.wsSendSnapshot(tableNames)
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_repair_backup(IVariantMap args) {
        boolean onlyInfo = args.containsKey("info")
        boolean doRepair = !onlyInfo

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            try {
                //
                JdxReplWs ws = new JdxReplWs(db)
                ws.init()

                //
                ws.repairAfterBackupRestore(doRepair, true)
            } catch (Exception e) {
                e.printStackTrace()
                throw e
            }

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }

    void repl_service_start(IVariantMap args) {
        UtReplService.start()

        //
        Thread.sleep(1000)

        //
        List<ServiceInfo> taskList = UtReplService.serviceList()
        UtReplService.printTaskList(taskList)
        //
        Collection<ProcessInfo> processList = UtReplService.processList()
        UtReplService.printProcessList(processList)
    }

    void repl_service_stop(IVariantMap args) {
        UtReplService.stop(args.containsKey("all"))

        //
        Thread.sleep(1000)

        //
        List<ServiceInfo> taskList = UtReplService.serviceList()
        UtReplService.printTaskList(taskList)
        //
        Collection<ProcessInfo> processList = UtReplService.processList()
        UtReplService.printProcessList(processList)
    }

    void repl_service_state(IVariantMap args) {
        Collection<ServiceInfo> taskList = UtReplService.serviceList()
        UtReplService.printTaskList(taskList)
        //
        Collection<ProcessInfo> processList = UtReplService.processList()
        UtReplService.printProcessList(processList)
    }

    void repl_service_install(IVariantMap args) {
        Db db = null
        try {
            // БД
            db = app.service(ModelService.class).model.getDb()
            db.connect()

            // Выполнение команды
            try {
                UtReplService.install(db)
            } catch (Exception e) {
                e.printStackTrace()
                throw e
            }

        } finally {
            if (db != null && db.connected) {
                db.disconnect()
            }
        }

        //
        Collection<ServiceInfo> taskList = UtReplService.serviceList()
        UtReplService.printTaskList(taskList)
        //
        Collection<ProcessInfo> processList = UtReplService.processList()
        UtReplService.printProcessList(processList)
    }

    void repl_service_remove(IVariantMap args) {
        if (args.containsKey("all")) {
            // Останавливаем все задачи
            UtReplService.stop(true)
            // Удаляем все задачи
            UtReplService.removeAll()
        } else {
            // Удаляем для текущей станции
            Db db = null
            try {
                // БД
                db = app.service(ModelService.class).model.getDb()
                db.connect()

                // Останавливаем процесс
                UtReplService.stop(false)
                // Удаляем службу
                UtReplService.remove(db)
            } finally {
                if (db != null && db.connected) {
                    db.disconnect()
                }
            }
        }

        //
        Collection<ServiceInfo> taskList = UtReplService.serviceList()
        UtReplService.printTaskList(taskList)
        //
        Collection<ProcessInfo> processList = UtReplService.processList()
        UtReplService.printProcessList(processList)
    }

    void gen_setup(IVariantMap args) {
        String inFileName = args.getValueString("in")
        String outDirName = args.getValueString("out")
        //
        if (inFileName == null || inFileName.length() == 0) {
            throw new XError("Не указан [in] - файл со списком станций")
        }
        inFileName = new File(new File(inFileName).getAbsolutePath())
        //
        if (outDirName == null || outDirName.length() == 0) {
            outDirName = new File(inFileName).getParent()
        }
        outDirName = UtFile.unnormPath(outDirName) + "/"

        //
        UtGenSetup utSetup = new UtGenSetup()
        utSetup.app = app
        utSetup.gen(inFileName, outDirName)
    }


    String param_NotSaveServiceState = "notSaveServiceState"

    /**
     * Запоминаем состояние процесса и службы репликатора,
     * останавливаем процесс и удаляем службу
     */
    ReplServiceState saveServiceState(Db db, IVariantMap args) {
        ReplServiceState serviceState = null

        //
        boolean saveServiceState = args.isValueNull(param_NotSaveServiceState)
        if (saveServiceState) {
            serviceState = UtReplService.readServiceState(db)

            try {
                // Останавливаем процесс 
                UtReplService.remove(db)
                // Удаляем службу
                UtReplService.stop(false)
            } catch (Exception e) {
                restoreServiceState(serviceState, db, args)
                throw e
            }

        }

        //
        return serviceState
    }

    /**
     * Восстанавливаем состояние процесса и службы репликатора по данным в replServiceState
     */
    void restoreServiceState(ReplServiceState replServiceState, Db db, IVariantMap args) {
        boolean saveServiceState = args.isValueNull(param_NotSaveServiceState)
        if (saveServiceState) {
            UtReplService.setServiceState(db, replServiceState)
        }
    }

}

