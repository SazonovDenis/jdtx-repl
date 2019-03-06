package jdtx.repl.main

import jandcode.app.App
import jandcode.dbm.ModelService
import jandcode.dbm.db.Db
import jandcode.jc.AppProjectExt
import jandcode.jc.ProjectExt
import jandcode.utils.UtFile
import jandcode.utils.UtLog
import jandcode.utils.error.XError
import jandcode.utils.variant.IVariantMap
import jdtx.repl.main.api.JdxReplWs
import jdtx.repl.main.api.UtDbObjectManager
import jdtx.repl.main.api.UtRepl
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


    void repl_create(IVariantMap args) {
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Создаем ...
            UtRepl utr = new UtRepl(db)
            utr.dropReplication()
            utr.createReplication()

        } finally {
            db.disconnect()
        }
    }


    void repl_add_ws(IVariantMap args) {
        //println(args)
        //String cubeName = args.getValueString("cube")
        //DateTime intervalDbeg = args.getValueDateTime("dbeg")

        String name = args.getValueString("name")
        if (name == null || name.length() == 0) {
            throw new XError("Не указано name")
        }

        //
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            //
            IJdxDbStructReader reader = new JdxDbStructReader()
            reader.setDb(db)
            IJdxDbStruct struct = reader.readDbStruct()
            //
            UtDbObjectManager ut = new UtDbObjectManager(db, struct)
            long wsId = ut.addWorkstation(name)
            //
            System.out.println("wsId: " + wsId)

        } finally {
            db.disconnect()
        }
    }


    void repl_setup(IVariantMap args) {
        //print(args)

        Db db = app.service(ModelService.class).model.getDb()
        db.connect()

        //
        try {
            // Рабочая станция, настройка
            JdxReplWs ws = new JdxReplWs(db, 1)
            ws.init("test/etalon/ws.json")

            // Забираем установочную реплику
            ws.createSetupReplica()

        } finally {
            db.disconnect()
        }
    }


}
