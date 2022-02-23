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
import jdtx.repl.main.api.decoder.*
import jdtx.repl.main.api.mailer.*
import jdtx.repl.main.api.manager.*
import jdtx.repl.main.api.publication.*
import jdtx.repl.main.api.que.*
import jdtx.repl.main.api.replica.*
import jdtx.repl.main.api.struct.*
import jdtx.repl.main.gen.*
import jdtx.repl.main.service.*
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
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        db.connect()

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()
            //
            System.out.println("Рабочая станция, wsId: " + ws.getWsId())

            //
            System.out.println("ws.wsId: " + ws.getWsId())
            System.out.println("  queIn.baseDir: " + ws.queIn.baseDir)
            System.out.println("  queOut.baseDir: " + ws.queOut.baseDir)
            System.out.println("  mailer.remoteUrl: " + ws.mailer.remoteUrl)
            System.out.println("  mailer.guid: " + ws.mailer.guid)

            //
            Map info = ws.getInfoWs()
            System.out.println("ws out:")
            System.out.println("  auditAgeActual: " + info.get("out_auditAgeActual"))
            System.out.println("  queAvailable: " + info.get("out_queAvailable"))
            System.out.println("  sendDone: " + info.get("out_sendDone"))
            System.out.println("ws in:")
            System.out.println("  mailAvailable: " + info.get("in_mailAvailable"))
            System.out.println("  queInNoAvailable: " + info.get("in_queInNoAvailable"))
            System.out.println("  queInNoDone: " + info.get("in_queInNoDone"))

            // Сервер
            try {
                System.out.println("")
                System.out.println("Сервер")

                //
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()

                //
                System.out.println("queCommon.baseDir: " + srv.queCommon.baseDir)
                for (Object obj : srv.mailerList.entrySet()) {
                    Map.Entry entry = (Map.Entry) obj
                    MailerHttp mailer = (MailerHttp) entry.value
                    System.out.println("mailer.wsId: " + entry.key)
                    System.out.println("  remoteUrl: " + mailer.remoteUrl)
                    System.out.println("  guid: " + mailer.guid)
                }

                //
                UtRepl urRepl = new UtRepl(db, null)
                System.out.println("srv:")
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
        System.out.println("База данных: " + db.getDbSource().getDatabase())

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
        String cfgFileName = args.getValueString("file")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Не указан [guid] - guid рабочей станции")
        }
        if (cfgFileName == null || cfgFileName.length() == 0) {
            throw new XError("Не указан [file] - конфиг-файл для рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            //
            IJdxDbStructReader dbStructReader = new JdxDbStructReader()
            dbStructReader.setDb(db)
            IJdxDbStruct struct = dbStructReader.readDbStruct()

            // Создаем базовые объекты
            UtRepl utRepl = new UtRepl(db, struct)
            utRepl.checkNotOwnId()
            utRepl.dropReplication()
            utRepl.createReplication(wsId, guid)

            // Начальный конфиг Ws
            JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfgFileName)
            CfgManager utCfg = new CfgManager(db)
            utCfg.setSelfCfg(cfg, CfgType.WS)

            // Создаем окружение для рабочей станции
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()
            ws.initFirst()

            // Создаем окружение для сервера
            if (wsId == JdxReplSrv.SERVER_WS_ID) {
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()
                srv.initFirst()
            }
        } finally {
            db.disconnect()
        }
    }


    void repl_add_ws(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String name = args.getValueString("name")
        String guid = args.getValueString("guid")
        String cfgPublications = args.getValueString("cfg_publications")
        String cfgDecode = args.getValueString("cfg_decode")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (name == null || name.length() == 0) {
            throw new XError("Не указано [name] - название рабочей станции")
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Не указан [guid] - guid рабочей станции")
        }
        if (cfgPublications == null || cfgPublications.length() == 0) {
            throw new XError("Не указан [cfg_publications] - конфиг-файл для publications рабочей станции")
        }
        if (cfgDecode == null || cfgDecode.length() == 0) {
            throw new XError("Не указан [cfg_decode] - конфиг-файл для decode рабочей станции")
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
            srv.addWorkstation(wsId, name, guid, cfgPublications, cfgDecode)
        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


    void repl_restore_ws(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String cfgSsnapshot = args.getValueString("cfg_snapshot")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (cfgSsnapshot == null || cfgSsnapshot.length() == 0) {
            throw new XError("Не указан [cfg_snapshot] - конфиг-файл для фильтрации snapshot рабочей станции")
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
            srv.restoreWorkstation(wsId, cfgSsnapshot)
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
                // Имя таблицы как оно есть в структуре
                tableName = ws.struct.getTable(tableName).getName()

                // Преобразуем recordId в ссылку (в пару ws:id)
                if (!recordIdRefStr.contains(":")) {
                    // Передали просто id - превратим ее в "каноническую" форму (пару ws:id)
                    long tableId = Long.parseLong(recordIdRefStr)
                    IRefDecoder decoder = new RefDecoder(db, ws.wsId)
                    JdxRef tableIdRef = decoder.get_ref(tableName, tableId)
                    recordIdRefStr = tableIdRef.toString()
                    println("В таблице: " + tableName + " ищем: " + recordIdRefStr)
                }

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

/*
    void repl_sync_ws(IVariantMap args) {
        String mailDir = args.getValueString("dir")
        long age_from = args.getValueLong("from", 0)
        long age_to = args.getValueLong("to", 0)
        boolean doMarkDone = args.getValueBoolean("mark", false)
        //
        if (mailDir == null || mailDir.length() == 0) {
            throw new XError("Не указан [dir] - почтовый каталог")
        }

        //
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()
            System.out.println("Рабочая станция, cfgFileName: " + cfgFileName + ", wsId: " + ws.getWsId())

            //
            System.out.println("Отслеживаем и обрабатываем свои изменения")
            ws.handleSelfAudit()

            //
            System.out.println("Отправляем свои изменения")
            ws.sendToDir(cfgFileName, mailDir, age_from, age_to, doMarkDone)

            //
            System.out.println("Забираем входящие реплики")
            ws.receiveFromDir(cfgFileName, mailDir)

            //
            System.out.println("Применяем входящие реплики")
            ws.handleQueIn()
        } finally {
            db.disconnect()
        }
    }
*/


/*
    void repl_sync_srv(IVariantMap args) {
        String mailDir = args.getValueString("dir")
        long from = args.getValueLong("from", 0)
        long to = args.getValueLong("to", 0)
        boolean doMarkDone = args.getValueBoolean("mark", false)
        long destinationWsId = args.getValueLong("ws")
        //
        if (mailDir == null || mailDir.length() == 0) {
            throw new XError("Не указан [dir] - почтовый каталог")
        }
        if (doMarkDone && destinationWsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции, для которой готовятся реплики")
        }

        //
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName_srv = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // ---
            // Сервер
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init(cfgFileName_srv)
            //
            System.out.println("Сервер, cfgFileName: " + cfgFileName_srv)

            // Формирование общей очереди
            srv.srvHandleCommonQueFrom(cfgFileName_srv, mailDir)

            // Тиражирование реплик
            SendRequiredInfo requiredInfo = new SendRequiredInfo()
            requiredInfo.requiredFrom = from
            requiredInfo.requiredTo = to
            srv.srvDispatchReplicasToDir(cfgFileName_srv, mailDir, requiredInfo, destinationWsId, doMarkDone)
        } finally {
            db.disconnect()
        }
    }
*/


    boolean repl_mail_check(IVariantMap args) {
        boolean doCreate = args.getValueBoolean("create")

        //
        boolean result = true

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            String[] boxes = ["from", "to001", "to"]
            for (Map.Entry en : srv.mailerList.entrySet()) {
                long wsId = (long) en.getKey()

                for (String box : boxes) {
                    MailerHttp mailer = (MailerHttp) en.getValue()
                    try {
                        if (doCreate) {
                            mailer.createMailBox(box)
                        } else {
                            mailer.checkMailBox(box)
                        }
                        System.out.println("wsId: " + wsId + ", box: " + box + " - ok")
                    } catch (Exception e) {
                        if (!e.message.contains("Box already exists")) {
                            System.out.println("wsId: " + wsId + ", box: " + box + ", error: " + e.message)
                            result = false
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

    void repl_ws_mute(IVariantMap args) {
        if (args.isValueNull("ws")) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        long destinationWsId = args.getValueLong("ws")

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
                srv.srvSetWsMute(destinationWsId)

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

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvSetWsUnmute(destinationWsId)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


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

    void repl_dbstruct_finish(IVariantMap args) {
        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        // Останавливаем процесс и удаляем службу
        ReplServiceState serviceState = saveServiceState(db, args)
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init()

            //
            srv.srvDbStructFinish()

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
        //
        long destinationWsId = args.getValueLong("ws")
        if (destinationWsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
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
            srv.srvSendCfg(cfgFileName, cfgType, destinationWsId, queName)

        } finally {
            restoreServiceState(serviceState, db, args)
            db.disconnect()
        }
    }


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

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Обновляем конфиг в своей таблице
            JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfgFileName)
            CfgManager utCfg = new CfgManager(db)
            utCfg.setSelfCfg(cfg, cfgType)

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
                // Запросили на сервере, для конкретной станциию
                // Отправляем на станцию через сервер, в очередь QUE_OUT001
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init()
                srv.srvSendSnapshot(destinationWsId, tableNames)
            } else {
                // Запросили с любой станции, не для конкретной станции
                // Отправляем всем в очередь QUE_COMMON
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

