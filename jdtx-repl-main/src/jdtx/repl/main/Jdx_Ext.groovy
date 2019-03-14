package jdtx.repl.main

import jandcode.app.App
import jandcode.bgtasks.BgTasksService
import jandcode.dbm.ModelService
import jandcode.dbm.db.Db
import jandcode.jc.AppProjectExt
import jandcode.jc.ProjectExt
import jandcode.utils.UtFile
import jandcode.utils.UtLog
import jandcode.utils.error.XError
import jandcode.utils.variant.IVariantMap
import jdtx.repl.main.api.*
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
        BgTasksService bgTasksService = app.service(BgTasksService.class);
        String cfgFileName_ws = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName");
        String cfgFileName_server = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName");

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase());

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName_ws)
            //
            System.out.println("Рабочая станция, cfgFileName: " + cfgFileName_ws + ", wsId: " + ws.getWsId());

            //
            System.out.println("ws.wsId: " + ws.getWsId())
            System.out.println("  queIn.baseDir: " + ws.queIn.baseDir)
            System.out.println("  queOut.baseDir: " + ws.queOut.baseDir)
            System.out.println("  mailer.remoteUrl: " + ws.mailer.remoteUrl);
            System.out.println("  mailer.guid: " + ws.mailer.guid);

            // Сервер
            System.out.println("");
            System.out.println("Сервер, cfgFileName: " + cfgFileName_server);

            //
            JdxReplSrv srv = new JdxReplSrv(db)
            srv.init(cfgFileName_server)

            //
            System.out.println("commonQue.baseDir: " + srv.commonQue.baseDir)
            for (Object obj : srv.mailerList.entrySet()) {
                Map.Entry entry = (Map.Entry) obj
                UtMailerHttp mailer = (UtMailerHttp) entry.value
                System.out.println("mailer.wsId: " + entry.key)
                System.out.println("  remoteUrl: " + mailer.remoteUrl)
                System.out.println("  guid: " + mailer.guid)
            }

        } finally {
            db.disconnect()
        }
    }

    void repl_create(IVariantMap args) {
        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase());

        //
        try {
            // Создаем объекты
            UtRepl utr = new UtRepl(db)
            utr.dropReplication()
            utr.createReplication()

        } finally {
            db.disconnect()
        }
    }


    void repl_add_ws(IVariantMap args) {
        String name = args.getValueString("name")
        if (name == null || name.length() == 0) {
            throw new XError("Не указано [name] - название рабочей станции ")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase());

        //
        try {
            IJdxDbStructReader reader = new JdxDbStructReader()
            reader.setDb(db)
            IJdxDbStruct struct = reader.readDbStruct()
            //
            UtDbObjectManager ut = new UtDbObjectManager(db, struct)
            long wsId = ut.addWorkstation(name)
            //
            System.out.println("new wsId: " + wsId)

        } finally {
            db.disconnect()
        }
    }


    void repl_snapshot(IVariantMap args) {
        BgTasksService bgTasksService = app.service(BgTasksService.class);
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName");

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase());

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName)
            //
            System.out.println("Рабочая станция, cfgFileName: " + cfgFileName + ", wsId: " + ws.getWsId());

            // Формируем установочную реплику
            ws.createSnapshotReplica()

        } finally {
            db.disconnect()
        }
    }


    void repl_sync(IVariantMap args) {
        String mailDir = args.getValueString("dir")
        long age_from = args.getValueLong("from")
        long age_to = args.getValueLong("to")
        boolean doMarkDone = args.getValueBoolean("mark", false)
        //
        if (mailDir == null || mailDir.length() == 0) {
            throw new XError("Не указан [dir] - почтовый каталог")
        }

        //
        BgTasksService bgTasksService = app.service(BgTasksService.class);
        String cfgFileName = bgTasksService.getRt().getChild("bgtask").getChild("ws").getValueString("cfgFileName");

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase());

        //
        try {
            // Рабочая станция
            JdxReplWs ws = new JdxReplWs(db)
            ws.init(cfgFileName)
            System.out.println("Рабочая станция, cfgFileName: " + cfgFileName + ", wsId: " + ws.getWsId());

            //
            System.out.println("Отслеживаем и обрабатываем свои изменения");
            ws.handleSelfAudit();

            //
            System.out.println("Отправляем свои изменения");
            ws.sendToDir(cfgFileName, mailDir, age_from, age_to, doMarkDone)

            //
            System.out.println("Забираем входящие реплики");
            ws.receiveFromDir(cfgFileName, mailDir)

            //
            System.out.println("Применяем входящие реплики");
            ws.handleQueIn();
        } finally {
            db.disconnect()
        }
    }


    void repl_sync_srv(IVariantMap args) {
        String mailDir = args.getValueString("dir")
        long age_from = args.getValueLong("from")
        long age_to = args.getValueLong("to")
        boolean doMarkDone = args.getValueBoolean("mark", false)
        //
        if (mailDir == null || mailDir.length() == 0) {
            throw new XError("Не указан [dir] - почтовый каталог")
        }

        //
        BgTasksService bgTasksService = app.service(BgTasksService.class);
        String cfgFileName_srv = bgTasksService.getRt().getChild("bgtask").getChild("server").getValueString("cfgFileName");

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase());

        //
        try {
            // ---
            // Сервер
            JdxReplSrv srv = new JdxReplSrv(db);
            srv.init(cfgFileName_srv);
            //
            System.out.println("Сервер, cfgFileName: " + cfgFileName_srv);

            // Формирование общей очереди
            srv.srvHandleCommonQueFrom(cfgFileName_srv, mailDir);

            // Тиражирование реплик
            srv.srvDispatchReplicasToDir(cfgFileName_srv, mailDir, age_from, age_to, doMarkDone);
        } finally {
            db.disconnect()
        }
    }


}
