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
     * rec_merge_find
     * @param args
     */
    void rec_merge_find(IVariantMap args) {
        String table = args.getValueString("table")
        String fields = args.getValueString("fields")
        String file = args.getValueString("file")
        String fileCfgGroup = args.getValueString("cfg_group")
        if (UtString.empty(table)) {
            throw new XError("Не указана [table] - имя таблицы")
        }
        if (UtString.empty(fields)) {
            throw new XError("Не указаны [fields] - поля для поиска")
        }
        if (UtString.empty(file)) {
            throw new XError("Не указан [file] - файл с результатом поиска в виде задач на слияние")
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
            String[] fieldNames = fields.split(",")

            //
            UtRecMerge utRecMerge = new UtRecMerge(db, struct)
            if (!UtString.empty(fileCfgGroup)) {
                JSONObject cfg = UtRepl.loadAndValidateJsonFile(fileCfgGroup)
                utRecMerge.groupsStrategyStorage.loadStrategy(cfg, struct)
            }

            // Ищем дубликаты
            Collection<RecDuplicate> duplicates = utRecMerge.findTableDuplicates(table, fieldNames)

            // Тупо превращаем дубликаты в задачи на слияние
            Collection<RecMergePlan> mergeTasks = utRecMerge.prepareMergePlan(table, duplicates)

            // Сериализация
            UtRecMergeReader reader = new UtRecMergeReader()
            reader.writeTasks(mergeTasks, file)

            // Печатаем задачи
            UtRecMergePrint.printTasks(mergeTasks)

        } finally {
            db.disconnect()
        }
    }

    /**
     * rec_merge_find
     * @param args
     */
    void rec_merge_exec(IVariantMap args) {
        String file = args.getValueString("file")
        boolean delete = args.getValueBoolean("delete", false)
        if (file == null || file.length() == 0) {
            throw new XError("Не указан [file] - файл с задачами на слияние")
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
            UtRecMergeReader reader = new UtRecMergeReader()
            Collection<RecMergePlan> mergeTasks = reader.readTasks(file)

            // Печатаем задачу на слияние
            UtRecMergePrint.printTasks(mergeTasks)

            // Исполняем задачу на слияние
            UtRecMerge utRecMerge = new UtRecMerge(db, struct)
            MergeResultTableMap mergeResults = utRecMerge.execMergePlan(mergeTasks, delete)

            // Сохраняем результат выполнения задачи
            reader = new UtRecMergeReader()
            reader.writeMergeResilts(mergeResults, UtFile.removeExt(file) + ".result.json")
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
            UtRecMerge relocator = new UtRecMerge(db, struct)

            //
            MergeResultTable relocateCheckResult = relocator.recordsRelocateFindRefs(tableName, idSour)
            System.out.println("Record sour:")
            UtData.outTable(relocateCheckResult.recordsDeleted)
            System.out.println("Records updated for tables, referenced to " + "Lic" + ":")
            UtRecMergePrint.printRecordsUpdated(relocateCheckResult.recordsUpdated)

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
            UtRecMerge relocator = new UtRecMerge(db, struct)

            System.out.println("Record sour:")
            UtData.outTable(db.loadSql("select * from " + tableName + " where id = " + idSour))

            //
            relocator.relocateId(tableName, idSour, idDest)

            //
            System.out.println("Record dest:")
            UtData.outTable(db.loadSql("select * from " + tableName + " where id = " + idDest))

        } finally {
            db.disconnect()
        }
    }


}

