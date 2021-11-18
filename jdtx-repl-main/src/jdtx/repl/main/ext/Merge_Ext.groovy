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
import jdtx.repl.main.api.data_binder.IJdxDataSerializer
import jdtx.repl.main.api.data_binder.JdxDataSerializer_decode
import jdtx.repl.main.api.rec_merge.*
import jdtx.repl.main.api.struct.*
import org.json.simple.*

/**
 * Обертка для вызовов утилиты jc с командной строки
 */
class Merge_Ext extends ProjectExt {


    private AppProjectExt _appProjectExt

    public Merge_Ext() {
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

    /**
     */
    void rec_merge_find(IVariantMap args) {
        String table = args.getValueString("table")
        String fields = args.getValueString("fields")
        String file = args.getValueString("file")
        String fileCfgGroup = args.getValueString("cfg_group")
        boolean useNull = args.getValueBoolean("use_null", false)
        boolean doPrintResult = args.getValueBoolean("print", false)
        if (UtString.empty(table)) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        if (UtString.empty(fields)) {
            throw new XError("Не указаны [fields] - поля для поиска")
        }
        if (UtString.empty(file)) {
            throw new XError("Не указан [file] - файл с результатом поиска в виде задач на слияние")
        }

        // Не затирать существующий
        File outFile = new File(file)
        if (outFile.exists()) {
            throw new XError("Файл уже существует: " + outFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            //IJdxDataSerializer dataSerializer =
            JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer)
            if (!UtString.empty(fileCfgGroup)) {
                JSONObject cfg = UtRepl.loadAndValidateJsonFile(fileCfgGroup)
                recMerger.groupsStrategyStorage.loadStrategy(cfg, struct)
            }

            // Ищем дубликаты
            Collection<RecDuplicate> duplicates = recMerger.findTableDuplicates(table, fields, useNull)

            // Тупо превращаем дубликаты в задачи на слияние
            Collection<RecMergePlan> mergeTasks = recMerger.prepareMergePlan(table, duplicates)

            // Сериализация
            UtRecMergeRW reader = new UtRecMergeRW()
            reader.writeTasks(mergeTasks, file)
            reader.writeDuplicates(duplicates, file + ".duplicates")

            // Печатаем задачи на слияние
            if (doPrintResult) {
                UtRecMergePrint.printTasks(mergeTasks)
            }

        } finally {
            db.disconnect()
        }
    }

    /**
     */
    void rec_merge_exec(IVariantMap args) {
        String file = args.getValueString("file")
        if (file == null || file.length() == 0) {
            throw new XError("Не указан [file] - файл с задачами на слияние")
        }

        // Не затирать существующий
        File outFile = new File(UtFile.removeExt(file) + ".result.zip")
        if (outFile.exists()) {
            throw new XError("Файл уже существует: " + outFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            //
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            // Читаем задачу на слияние
            UtRecMergeRW reader = new UtRecMergeRW()
            Collection<RecMergePlan> mergeTasks = reader.readTasks(file)

            // Сохраняем результат выполнения задачи
            RecMergeResultWriter recMergeResultWriter = new RecMergeResultWriter()
            recMergeResultWriter.open(outFile)

            // Исполняем
            //IJdxDataSerializer dataSerializer = ;
            JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer)
            recMerger.execMergePlan(mergeTasks, recMergeResultWriter)

            // Сохраняем
            recMergeResultWriter.close()
        } finally {
            db.disconnect()
        }
    }

    /**
     */
    void rec_relocate_check(IVariantMap args) {
        String tableName = args.getValueString("table")
        long idSour = args.getValueLong("sour")
        if (tableName == null || tableName.length() == 0) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        if (idSour == 0) {
            throw new XError("Не указан [sour] - исходный pk")
        }
        String outFileName = args.getValueString("outFile")
        File outFile
        if (outFileName == null || outFileName.length() == 0) {
            outFile = new File("relocateCheck_" + tableName + "_" + idSour + ".zip")
        } else {
            outFile = new File(outFileName)
        }
        // Не затирать существующий
        if (outFile.exists()) {
            throw new XError("Файл уже существует: " + outFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            UtRecRelocator relocator = new UtRecRelocator(db, struct)
            relocator.relocateIdCheck(tableName, idSour, outFile)

            //
            System.out.println("OutFile: " + outFile.getAbsolutePath())

        } finally {
            db.disconnect()
        }
    }

    /**
     */
    void rec_relocate(IVariantMap args) {
        String tableName = args.getValueString("table")
        long idSour = args.getValueLong("sour")
        long idDest = args.getValueLong("dest")
        if (tableName == null || tableName.length() == 0) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        if (idSour == 0) {
            throw new XError("Не указан [sour] - исходный pk")
        }
        if (idDest == 0) {
            throw new XError("Не указан [dest] - конечный pk")
        }
        String outFileName = args.getValueString("outFile")
        File outFile
        if (outFileName == null || outFileName.length() == 0) {
            outFile = new File("relocate_" + tableName + "_" + idSour + "_" + idDest + ".zip")
        } else {
            outFile = new File(outFileName)
        }
        // Не затирать существующий
        if (outFile.exists()) {
            throw new XError("Файл уже существует: " + outFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            System.out.println("Record sour:")
            UtData.outTable(db.loadSql("select * from " + tableName + " where id = " + idSour))

            //
            UtRecRelocator relocator = new UtRecRelocator(db, struct)
            relocator.relocateId(tableName, idSour, idDest, outFile)

            //
            System.out.println("Record dest:")
            UtData.outTable(db.loadSql("select * from " + tableName + " where id = " + idDest))

            //
            System.out.println("OutFile: " + outFile.getAbsolutePath())

        } finally {
            db.disconnect()
        }
    }

    void rec_relocate_all(IVariantMap args) {
        String tableName = args.getValueString("table")
        long idSour = args.getValueLong("sour")
        String dirName = args.getValueString("dir")
        if (tableName == null || tableName.length() == 0) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        if (idSour == 0) {
            throw new XError("Не указан [sour] - значение pk, выше которого нужно перемещать запись")
        }
        if (dirName == null || dirName.length() == 0) {
            throw new XError("Не указан [outDir] - каталог с результатом")
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + db.getDbSource().getDatabase())

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            dirName = UtFile.unnormPath(dirName) + "/"
            UtRecRelocator relocator = new UtRecRelocator(db, struct)
            relocator.relocateIdAll(tableName, idSour, dirName)
        } finally {
            db.disconnect()
        }
    }


}

