package jdtx.repl.main.ext

import jandcode.app.*
import jandcode.dbm.*
import jandcode.dbm.db.*
import jandcode.jc.*
import jandcode.utils.*
import jandcode.utils.error.*
import jandcode.utils.variant.*
import jdtx.repl.main.api.*
import jdtx.repl.main.api.data_serializer.*
import jdtx.repl.main.api.rec_merge.*
import jdtx.repl.main.api.struct.*
import jdtx.repl.main.api.util.*
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
     * Поиск дубликатов
     */
    void rec_merge_find(IVariantMap args) {
        String table = args.getValueString("table")
        String fields = args.getValueString("fields")
        String resultFileName = args.getValueString("file")
        String fileCfgGroup = args.getValueString("cfg_group")
        boolean useNull = args.getValueBoolean("use_null", false)
        boolean doPrintResult = args.getValueBoolean("print", false)
        if (UtString.empty(table)) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        if (UtString.empty(fields)) {
            throw new XError("Не указаны [fields] - поля для поиска")
        }
        if (UtString.empty(resultFileName)) {
            throw new XError("Не указан [file] - файл с результатом поиска в виде задач на слияние")
        }

        //
        File resultFile = new File(resultFileName)

        // Не затирать существующий
        if (resultFile.exists()) {
            throw new XError("Файл уже существует: " + resultFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain()
            JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer)
            if (!UtString.empty(fileCfgGroup)) {
                JSONObject cfg = UtRepl.loadAndValidateJsonFile(fileCfgGroup)
                GroupsStrategyStorage.initInstance(cfg, struct)
            } else {
                GroupsStrategyStorage.initInstance(null, struct)
            }

            // Ищем дубликаты
            Collection<RecDuplicate> duplicates = recMerger.findTableDuplicates(table, fields, useNull)

            // Тупо превращаем дубликаты в задачи на слияние
            Collection<RecMergePlan> mergePlans = recMerger.prepareMergePlan(table, duplicates)

            // Сериализация
            UtRecMergePlanRW reader = new UtRecMergePlanRW()
            reader.writePlans(mergePlans, resultFileName)
            reader.writeDuplicates(duplicates, resultFileName + ".duplicates")

            // Печатаем задачи на слияние
            if (doPrintResult) {
                UtRecMergePrint.printPlans(mergePlans)
            }

            //
            System.out.println("Out file: " + resultFile.getCanonicalPath())
        } finally {
            db.disconnect()
        }
    }

    /**
     * Выполнить слияние дубликатов
     */
    void rec_merge_exec(IVariantMap args) {
        String fileName = args.getValueString("file")
        if (fileName == null || fileName.length() == 0) {
            throw new XError("Не указан [file] - файл с задачами на слияние")
        }
        //
        String resultFileName = args.getValueString("out")
        File resultFile
        if (resultFileName == null || resultFileName.length() == 0) {
            resultFile = new File(UtFile.removeExt(fileName) + ".result.zip")
        } else {
            resultFile = new File(resultFileName);
        }

        // Не затирать существующий
        if (resultFile.exists()) {
            throw new XError("Файл уже существует: " + resultFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            //
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            // Читаем задачу на слияние
            UtRecMergePlanRW reader = new UtRecMergePlanRW()
            Collection<RecMergePlan> mergePlans = reader.readPlans(fileName)

            //
            IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain()
            JdxRecMerger recMerger = new JdxRecMerger(db, struct, dataSerializer)

            // Исполняем
            recMerger.execMergePlan(mergePlans, resultFile)

            //
            System.out.println("Out file: " + resultFile.getAbsolutePath())
        } finally {
            db.disconnect()
        }
    }

    /**
     */
    void rec_remove_cascade(IVariantMap args) {
        String recordIdStr = args.getValueString("id")
        if (recordIdStr == null || recordIdStr.length() == 0) {
            throw new XError("Не указан [id] - id записи")
        }
        String tableName = recordIdStr.split(":")[0]
        long recordId = UtJdxData.longValueOf(recordIdStr.substring(tableName.length() + 1))
        //
        String outFileName = args.getValueString("out")
        File outFile
        if (outFileName == null || outFileName.length() == 0) {
            outFile = new File("remove_cascade_" + tableName + "_" + recordId + ".result.zip")
        } else {
            outFile = new File(outFileName);
        }

        // Не затирать существующий
        if (outFile.exists()) {
            throw new XError("Файл уже существует: " + outFile.getCanonicalPath())
        }

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain()
            JdxRecRemover recRemover = new JdxRecRemover(db, struct, dataSerializer)

            // Исполняем
            recRemover.removeRecCascade(tableName, recordId, outFile)

            //
            System.out.println("Out file: " + outFile.getAbsolutePath())
        } finally {
            db.disconnect()
        }
    }

    /**
     */
    void rec_merge_revert(IVariantMap args) {
        String tableNames = args.getValueString("tables")
        List<String> tables = null
        if (tableNames != null && tableNames.length() != 0) {
            tables = UtCnv.toList(tableNames)
        }
        //
        String resultFileName = args.getValueString("file")
        if (resultFileName == null || resultFileName.length() == 0) {
            throw new XError("Не указан [file] - файл с результатом слияния")
        }
        File resultFile = new File(resultFileName)
        //
        System.out.println("Result file: " + resultFile.getAbsolutePath())

        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            JdxReplWs ws = new JdxReplWs(db)
            ws.init()
            IJdxRecMerger recMerger = ws.getRecMerger();

            //
            recMerger.revertExec(resultFile, tables)
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
            throw new XError("Не указаны [sour] - исходные pk")
        }
        String outFileName = args.getValueString("outFile")
        File outFile
        if (outFileName == null || outFileName.length() == 0) {
            outFile = new File("relocate_check_" + tableName + "_" + idSour + ".result.zip")
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
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()

            //
            IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain()
            JdxRecRelocator relocator = new JdxRecRelocator(db, struct, dataSerializer)
            relocator.relocateIdCheck(tableName, idSour, outFile)

            //
            System.out.println("Out file: " + outFile.getAbsolutePath())

        } finally {
            db.disconnect()
        }
    }

    /**
     */
    void rec_relocate(IVariantMap args) {
        String tableName = args.getValueString("table")
        if (tableName == null || tableName.length() == 0) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        //
        String resultDirName = args.getValueString("outDir")
        if (resultDirName == null || resultDirName.length() == 0) {
            throw new XError("Не указан [outDir] - каталог с результами переноса")
        }

        //
        String fileName = args.getValueString("file")
        //
        String idSourStr = args.getValueString("sour")
        String idDestStr = args.getValueString("dest")
        //
        long idSourFrom = args.getValueLong("sourFrom")
        long idSourTo = args.getValueLong("sourTo")
        long idDestFrom = args.getValueLong("destFrom")

        //
        if (!args.isValueNull("file")) {
            if (fileName == null || fileName.length() == 0) {
                throw new XError("Не указан [file] - файл со списком исходных и конечных primary key")
            }

        } else if (!args.isValueNull("sour")) {
            if (idSourStr == null || idSourStr.length() == 0) {
                throw new XError("Не указаны [sour] - исходные primary key")
            }
            if (idDestStr == null || idDestStr.length() == 0) {
                throw new XError("Не указаны [dest] - конечные primary key")
            }

        } else if (!args.isValueNull("sourFrom")) {
            if (idSourFrom == 0) {
                throw new XError("Не указан [sourFrom] - начало диапазона перемещаемых primary key")
            }
            if (idSourTo == 0) {
                throw new XError("Не указан [sourTo] - конец диапазона перемещаемых primary key")
            }
            if (idDestFrom == 0) {
                throw new XError("Не указан [destFrom] - начальный primary key, куда будут перенесены записи")
            }

        } else {
            throw new XError("Не указаны диапазоны primary key. Укажите либо file, либо sour + dest, либо sourFrom + sourTo + destFrom")
        }


        // БД
        Db db = app.service(ModelService.class).model.getDb()
        db.connect()
        //
        System.out.println("База данных: " + UtJdx.getDbInfoStr(db))

        //
        try {
            resultDirName = UtFile.unnormPath(resultDirName) + "/"
            UtFile.mkdirs(resultDirName)

            //
            IJdxDbStructReader structReader = new JdxDbStructReader()
            structReader.setDb(db)
            IJdxDbStruct struct = structReader.readDbStruct()
            //
            System.out.println("Таблиц в базе: " + struct.getTables().size())

            //
            IJdxDataSerializer dataSerializer = new JdxDataSerializerPlain()
            JdxRecRelocator relocator = new JdxRecRelocator(db, struct, dataSerializer)

            //
            List<String> idsSour = new ArrayList<>()
            List<String> idsDest = new ArrayList<>()
            if (!args.isValueNull("file")) {
                relocator.rec_relocate_paramsFile(fileName, idsSour, idsDest)

            } else if (!args.isValueNull("sour")) {
                relocator.rec_relocate_paramsStr(idSourStr, idDestStr, idsSour, idsDest)

            } else if (!args.isValueNull("sourFrom")) {
                relocator.rec_relocate_paramsRange(tableName, idSourFrom, idSourTo, idDestFrom, idsSour, idsDest)
            }

            //
            relocator.relocateIdList(tableName, idsSour, idsDest, resultDirName)

        } finally {
            db.disconnect()
        }
    }


}

