package jdtx.repl.main.ext

import jandcode.app.App
import jandcode.bgtasks.BgTasksService
import jandcode.dbm.ModelService
import jandcode.dbm.data.UtData
import jandcode.dbm.db.Db
import jandcode.jc.AppProjectExt
import jandcode.jc.ProjectExt
import jandcode.utils.UtFile
import jandcode.utils.UtLog
import jandcode.utils.error.XError
import jandcode.utils.variant.IVariantMap
import jdtx.repl.main.api.JdxReplSrv
import jdtx.repl.main.api.JdxReplWs
import jdtx.repl.main.api.UtRepl
import jdtx.repl.main.api.mailer.MailerHttp
import jdtx.repl.main.api.struct.IJdxDbStruct
import jdtx.repl.main.api.struct.IJdxDbStructReader
import jdtx.repl.main.api.struct.JdxDbStructReader

/**
 * Обертка для вызовов утилиты jc с командной строки
 */
class Jdx_Ext extends ProjectExt {


    private static AppProjectExt _appProjectExt

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
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName_ws = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName")
        String cfgFileName_server = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName_ws)
            //
            System.out.println("Рабочая станция, cfgFileName: " + cfgFileName_ws + ", wsId: " + ws.getWsId())

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
                System.out.println("Сервер, cfgFileName: " + cfgFileName_server)

                //
                JdxReplSrv srv = new JdxReplSrv(db)
                srv.init(cfgFileName_server)

                //
                System.out.println("commonQue.baseDir: " + srv.commonQue.baseDir)
                for (Object obj : srv.mailerList.entrySet()) {
                    Map.Entry entry = (Map.Entry) obj
                    MailerHttp mailer = (MailerHttp) entry.value
                    System.out.println("mailer.wsId: " + entry.key)
                    System.out.println("  remoteUrl: " + mailer.remoteUrl)
                    System.out.println("  guid: " + mailer.guid)
                }
            } catch (Exception e) {
                System.out.println(e.message)
            }

            //
            UtRepl urRepl = new UtRepl(db, null)
            System.out.println("srv:")
            UtData.outTable(urRepl.getInfoSrv());

        } finally {
            db.disconnect()
        }
    }

    void repl_create(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String guid = args.getValueString("guid")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Не указан [guid] - guid рабочей станции")
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
            UtRepl utRepl = new UtRepl(db, struct)

            // Создаем объекты
            utRepl.dropReplication()
            utRepl.createReplication(wsId, guid)

        } finally {
            db.disconnect()
        }
    }


    void repl_add_ws(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        String name = args.getValueString("name")
        String guid = args.getValueString("guid")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }
        if (name == null || name.length() == 0) {
            throw new XError("Не указано [name] - название рабочей станции")
        }
        if (guid == null || guid.length() == 0) {
            throw new XError("Не указан [guid] - guid рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.addWorkstation(wsId, name, guid)
            //

            //
            System.out.println("new wsId: " + wsId)

        } finally {
            db.disconnect()
        }
    }


    void repl_enable(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.enableWorkstation(wsId)
        } finally {
            db.disconnect()
        }
    }


    void repl_disable(IVariantMap args) {
        long wsId = args.getValueLong("ws")
        if (wsId == 0L) {
            throw new XError("Не указан [ws] - код рабочей станции")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.disableWorkstation(wsId)
        } finally {
            db.disconnect()
        }
    }


    void repl_snapshot(IVariantMap args) {
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName)
            //
            System.out.println("Рабочая станция, cfgFileName: " + cfgFileName + ", wsId: " + ws.getWsId())

            // Формируем установочную реплику
            ws.createSnapshotReplica()

        } finally {
            db.disconnect()
        }
    }


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
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName)
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


    void repl_sync_srv(IVariantMap args) {
        String mailDir = args.getValueString("dir")
        long age_from = args.getValueLong("from", 0)
        long age_to = args.getValueLong("to", 0)
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
        System.out.println("База данных: " + db.getDbSource().getDatabase())

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
            srv.srvDispatchReplicasToDir(cfgFileName_srv, mailDir, age_from, age_to, destinationWsId, doMarkDone)
        } finally {
            db.disconnect()
        }
    }


    void repl_mail_check(IVariantMap args) {
        boolean doCreate = args.getValueBoolean("create")

        //
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName_srv = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init(cfgFileName_srv)
            //
            System.out.println("Сервер, cfgFileName: " + cfgFileName_srv)

            //
            String[] boxes = ["from", "to"]
            for (Map.Entry en : srv.mailerList.entrySet()) {
                long wsId = (long) en.getKey()

                for (String box : boxes) {
                    //System.out.println("wsId: " + wsId + ", box: " + box)

                    //
                    MailerHttp mailer = (MailerHttp) en.getValue()
                    try {
                        if (doCreate) {
                            mailer.createMailBox(box)
                        } else {
                            mailer.checkMailBox(box)
                        }
                        System.out.println("wsId: " + wsId + ", box: " + box + " - ok")
                    } catch (Exception e) {
                        System.out.println("wsId: " + wsId + ", box: " + box + ", error: " + e.message)
                    }

                    //
                    System.out.println("")
                }

            }

        } finally {
            db.disconnect()
        }
    }

    void repl_dbstruct_start(IVariantMap args) {
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName_srv = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init(cfgFileName_srv)

            //
            srv.srvDbStructStart();

        } finally {
            db.disconnect()
        }
    }

    void repl_dbstruct_finish(IVariantMap args) {
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName_srv = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init(cfgFileName_srv)

            //
            srv.srvDbStructFinish();

        } finally {
            db.disconnect()
        }
    }

    /**
     * Команда рабочей станции. Проверить необходимость обновить структуру аудита.
     */
    void repl_dbstruct_update(IVariantMap args) {
        BgTasksService bgTasksService = app.service(BgTasksService.class)
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName")

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName)

            //
            ws.dbStructUpdate();

        } finally {
            db.disconnect()
        }
    }

    void repl_version(IVariantMap args) {
        System.out.println("UtRepl.getVersion: " + UtRepl.getVersion())
    }

    void repl_service_start(IVariantMap args) {
        UtReplService.start()
    }

    void repl_service_state(IVariantMap args) {
        UtReplService.list()
    }

    void repl_service_stop(IVariantMap args) {
        UtReplService.stop()
    }

    void repl_service_install(IVariantMap args) {
        UtReplService.install()
    }

    void repl_service_remove(IVariantMap args) {
        UtReplService.stop()
        UtReplService.remove()
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
        UtSetup utSetup = new UtSetup()
        utSetup.app = app
        utSetup.gen(inFileName, outDirName)
    }

}
