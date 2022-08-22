package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.filter.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.io.*;
import org.apache.commons.io.comparator.*;
import org.apache.commons.logging.*;
import org.apache.tools.ant.filters.*;
import org.joda.time.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

/**
 * Утилиты (связанные именно с репликацией) верхнего уровня, готовые команды.
 * todo: Куча всего в одном месте, и еще куча лишнего. Это плохо
 * todo: путается с jdtx.repl.main.api.util.UtJdx
 */
public class UtRepl {

    private Db db;
    private IJdxDbStruct struct;

    //
    protected static Log log = LogFactory.getLog("jdtx.UtRepl");

    //
    public UtRepl(Db db, IJdxDbStruct struct) {
        this.db = db;
        this.struct = struct;
    }

    public static String getGuidWs(String guid, long wsId) {
        return guid + "-" + UtString.padLeft(String.valueOf(wsId), 3, '0');
    }

    /**
     * Инициализация рабочей станции:
     * <p>
     * Создать репликационные структуры
     * - триггеры и таблицы аудита
     * - таблица возрастов таблиц
     * - таблицы перекодировок
     * <p>
     * Поставить метку wsId
     */
    public void createReplication(long wsId, String guid) throws Exception {
        // Создание базовых структур и рабочей станции
        UtDbObjectManager objectManager = new UtDbObjectManager(db);
        String guidWs = getGuidWs(guid, wsId);
        objectManager.createReplBase(wsId, guidWs);

        // Создаем необходимые для перекодировки таблицы
        UtDbObjectDecodeManager decodeManager = new UtDbObjectDecodeManager(db);
        decodeManager.createRefDecodeObject();

        //
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);

        // Для начала "разрешенная" структура будет пустая
        IJdxDbStruct structAllowed = new JdxDbStruct();
        databaseStructManager.setDbStructAllowed(structAllowed);

        // Для начала "фиксированная" структура будет пустая
        IJdxDbStruct structFixed = new JdxDbStruct();
        databaseStructManager.setDbStructFixed(structFixed);

        // Сразу после добавления - в стотоянии MUTE. Сервер разрешит говорить отдельной командой.
        JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);
        muteManager.muteWorkstation();
    }


    /**
     * Удалить репликационные структуры
     */
    public void dropReplication() throws Exception {
        UtDbObjectManager ut = new UtDbObjectManager(db);

        //
        log.info("Удаляем системные объекты");

        // Удаляем связанную с каждой таблицей таблицу журнала изменений
        log.info("dropAudit - журналы");
        long n = 0;
        for (IJdxTable table : struct.getTables()) {
            n++;
            log.info("  dropAudit " + n + "/" + struct.getTables().size() + " " + table.getName());
            //
            ut.dropAudit(table.getName());
        }

        // Удаляем системные таблицы и генераторы
        log.info("dropAudit - системные объекты");
        ut.dropAuditBase();

        // Удаляем необходимые для перекодировки таблицы
        UtDbObjectDecodeManager decodeManager = new UtDbObjectDecodeManager(db);
        decodeManager.dropRefDecodeObject();
    }


    /**
     * Проверяет, что в БД нет недопустимых ID.
     * При некоторых способах разведения id между рабочими станциями
     * id должны быть меньше определенного числа, см. реализацию jdtx.repl.main.api.decoder.RefDecoder
     */
    public void checkNotOwnId() throws Exception {
        log.info("Check not own id");
        boolean foundNotOwnId = false;

        //
        long n = 0;
        for (IJdxTable table : struct.getTables()) {
            n++;
            //
            String tableName = table.getName();
            if (table.getPrimaryKey().size() > 0) {
                // log.info("  checkNotOwnId " + n + "/" + struct.getTables().size() + " " + table.getName());
                String pkFieldName = table.getPrimaryKey().get(0).getName();
                //
                long maxId = db.loadSql("select max(" + pkFieldName + ") max_id from " + tableName).getCurRec().getValueLong("max_id");
                //
                if (maxId > RefDecoder.get_max_own_id()) {
                    log.error("Check not own id, not own id found, table: " + tableName + ", " + pkFieldName + ": " + maxId);
                    foundNotOwnId = true;
                }
            } else {
                log.info("Check not own id, skipped table: " + tableName);
            }
        }

        //
        log.info("Check not own id, done");
        if (foundNotOwnId) {
            throw new XError("Not own id found");
        }
    }

    // todo: в принципе - не нужен, юзается только в jdtx.repl.main.ext.Jdx_Ext.repl_info, уберем при рефакторинге мониторинга
    @Deprecated
    public DataStore getInfoSrv() throws Exception {
        DataStore st = db.loadSql(UtReplSql.sql_srv);
        return st;
    }

    /**
     * Создает snapsot-реплику для выбранных записей idList, из таблицы table,
     * без фильтраци (т.к. указаны конкретные id),
     * по всем полям (игнорируя правила).
     */
    public IReplica createSnapshotByIdList(long wsId, IJdxTable table, Collection<Long> idList) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(-1);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDat();

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, wsId, false);
        String publicationFields = UtJdx.fieldsToString(table.getFields());
        dataSelector.readRecordsByIdList(table.getName(), idList, publicationFields, xmlWriter);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    /**
     * Создает snapsot-реплику для таблицы, упомянутой в правиле publicationRule,
     * без фильтрации записей, по полям, указанным в правиле publicationRule.
     * <p>
     * Используется при включении новой БД в систему, в числе первых реплик для сервера
     * или при добавлении таблицы в БД.
     */
    public IReplica createReplicaSnapshotForTable(long selfWsId, IPublicationRule publicationRule, boolean forbidNotOwnId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(selfWsId);
        replica.getInfo().setAge(-1);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDat();

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, selfWsId, forbidNotOwnId);
        dataSelector.readAllRecords(publicationRule, xmlWriter);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    /**
     * Создает INS-реплику для записи valuesStr
     */
    public IReplica createReplicaInsRecord(String tableName, Map<String, String> valuesStr, long selfWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(selfWsId);
        replica.getInfo().setAge(-1);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDat();

        // Данные помещаем в dataWriter
        xmlWriter.startTable(tableName);

        // Добавляем запись
        xmlWriter.appendRec();

        // Тип операции
        xmlWriter.writeOprType(JdxOprType.INS);

        // Тело записи
        IJdxTable table = struct.getTable(tableName);
        UtXml.recToWriter(valuesStr, UtJdx.fieldsToString(table.getFields()), xmlWriter);

        //
        xmlWriter.flush();

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaSetDbStruct(long destinationWsId, JSONObject cfgPublications, boolean sendSnapshot) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_DB_STRUCT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // ---
        // Открываем запись файла с информацией о команде
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("sendSnapshot", sendSnapshot);
        cfgInfo.put("destinationWsId", destinationWsId);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);


        // ---
        // Открываем запись файла с описанием текущей структуры БД
        zipOutputStream = replicaWriter.newFileOpen("dat.xml");

        // Пишем файл с описанием структуры
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        zipOutputStream.write(struct_rw.getBytes(struct));


        // ---
        // Открываем запись файла с описанием конфига
        zipOutputStream = replicaWriter.newFileOpen("cfg.publications.json");

        // Пишем содержимое конфига
        String cfgStr = UtJson.toString(cfgPublications);
        StringInputStream cfgStrStream = new StringInputStream(cfgStr);
        UtFile.copyStream(cfgStrStream, zipOutputStream);


        // ---
        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaMute(long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.MUTE);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о получателе
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaRepairGenerators(long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.REPAIR_GENERATORS);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о получателе
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaUnmute(long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.UNMUTE);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о получателе
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaSetWsState(long destinationWsId, JdxWsState wsState) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_STATE);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о получателе
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject wsStateJson = new JSONObject();
        wsStateJson.put("destinationWsId", destinationWsId);
        wsState.toJson(wsStateJson);
        String wsStateStr = UtJson.toString(wsStateJson);
        StringInputStream infoStream = new StringInputStream(wsStateStr);
        UtFile.copyStream(infoStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }
    
    public IReplica createReplicaAppUpdate(String exeFileName) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.UPDATE_APP);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        //
        File exeFile = new File(exeFileName);


        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с версией
        OutputStream zipOutputStream = replicaWriter.newFileOpen("version");

        //
        String version = parseExeVersion(exeFile.getName());
        StringInputStream versionStream = new StringInputStream(version);
        UtFile.copyStream(versionStream, zipOutputStream);


        // Открываем запись файла - бинарника для обновления
        zipOutputStream = replicaWriter.newFileOpen(exeFile.getName());

        // Пишем содержимое exe
        InputStream exeFileStream = new FileInputStream(exeFile);
        UtFile.copyStream(exeFileStream, zipOutputStream);


        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaMerge(String taskFileName) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.MERGE);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла
        OutputStream zipOutputStream = replicaWriter.newFileOpen("plan.json");

        // Пишем содержимое
        File taskFile = new File(taskFileName);
        InputStream taskFileStream = new FileInputStream(taskFile);
        UtFile.copyStream(taskFileStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaSetCfg(JSONObject cfg, String cfgType, long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_CFG);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));


        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о конфиге
        OutputStream zipOutputStream = replicaWriter.newFileOpen("cfg.info.json");

        // Информация о конфиге
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        cfgInfo.put("cfgType", cfgType);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);


        // Открываем запись файла - сам конфиг
        zipOutputStream = replicaWriter.newFileOpen("cfg.json");

        // Пишем содержимое конфига
        String cfgStr = UtJson.toString(cfg);
        StringInputStream cfgStrStream = new StringInputStream(cfgStr);
        UtFile.copyStream(cfgStrStream, zipOutputStream);


        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaWsSendSnapshot(long destinationWsId, String tableName) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SEND_SNAPSHOT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Чтобы не возится с регистром
        IJdxTable table = struct.getTable(tableName);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о получателе
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        cfgInfo.put("tableName", table.getName());
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    /**
     * Ищем запись в истории реплик, собираем все операции с этой записью в одну реплику.
     * Метод применяется при восстановлении асинхронно удаленных записей.
     *
     * @param tableName        Таблица, чью запись ищем, например "ABN"
     * @param recordIdStr      Полный id записи, например "10:12345"
     * @param replicasDirsName Каталоги с репликами для поиска, например "d:/temp/"
     * @param skipOprDel       Пропускать реплики на удаление записи
     * @param findLastOne      Найти только один последний вариант записи (тогда поиск идет с последних реплик)
     * @param outFileName      Файл для реплики-результата, например "d:/temp/ABN_10_12345.zip"
     * @return Реплика со всеми операциями, найденными для запрошенной записи
     */
    public IReplica findRecordInReplicas(String tableName, String recordIdStr, String replicasDirsName, boolean skipOprDel, boolean findLastOne, String outFileName) throws Exception {
        if (findLastOne && !skipOprDel) {
            throw new XError("При поиске последннего варианта (lastOne == true) нужно указывать skipDel == true");
        }

        //
        String outFileNameInfo;
        if (outFileName.endsWith(".zip")) {
            outFileNameInfo = outFileName.replace(".zip", ".json");
        } else {
            outFileNameInfo = outFileName + ".json";
        }

        // Список файлов-реплик в каталогах replicasDirsName, ищем в них
        List<File> files = findFilesInDirs(replicasDirsName, findLastOne);

        // Тут копим info-данные по найденным репликам
        JSONArray replicaInfoList = new JSONArray();


        //
        JdxRef findRecordId = JdxRef.parse(recordIdStr);

        //
        IReplica replicaOut = new ReplicaFile();
        replicaOut.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replicaOut.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replicaOut);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDat();

        //
        xmlWriter.startTable(tableName);


        //
        IJdxTable table = null;
        List<IJdxTable> tables = struct.getTables();
        for (IJdxTable t : tables) {
            if (t.getName().compareToIgnoreCase(tableName) == 0) {
                table = t;
                break;
            }
        }
        if (table == null) {
            throw new XError("Таблица не найдена в структуре: " + tableName);
        }
        String pkFieldName = table.getPrimaryKey().get(0).getName();


        //
        int countFile = 0;
        while (countFile < files.size()) {
            File file = files.get(countFile);

            countFile++;
            if (countFile % 100 == 0) {
                log.debug(countFile + "/" + files.size() + ", file: " + file.getCanonicalPath());
            } else {
                log.info(countFile + "/" + files.size() + ", file: " + file.getCanonicalPath());
            }

            //
            try {
                IReplica replica = new ReplicaFile();
                replica.setData(file);
                JdxReplicaReaderXml.readReplicaInfo(replica);

                if (replica.getInfo().getReplicaType() != JdxReplicaType.IDE && replica.getInfo().getReplicaType() != JdxReplicaType.IDE_MERGE && replica.getInfo().getReplicaType() != JdxReplicaType.SNAPSHOT) {
                    log.debug("  skip, replicaType: " + replica.getInfo().getReplicaType());
                    continue;
                }

                //
                JdxReplicaFileInputStream inputStream = null;
                try {
                    // Распакуем XML-файл из Zip-архива
                    inputStream = JdxReplicaReaderXml.createInputStreamData(replica);

                    //
                    JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);


                    // Тут сохраним данные по реплике
                    boolean recordsFoundInReplica = false;
                    Map replicaInfo = new HashMap();
                    replicaInfo.put("wsId", replica.getInfo().getWsId());
                    replicaInfo.put("age", replica.getInfo().getAge());
                    replicaInfo.put("replicaType", replica.getInfo().getReplicaType());
                    replicaInfo.put("dtFrom", replica.getInfo().getDtFrom());
                    replicaInfo.put("dtTo", replica.getInfo().getDtTo());
                    replicaInfo.put("file", replica.getData().getAbsolutePath());
                    JSONArray replicaInfoData = new JSONArray();
                    replicaInfo.put("data", replicaInfoData);

                    //
                    String readerTableName = replicaReader.nextTable();
                    while (readerTableName != null) {
                        //
                        if (readerTableName.compareToIgnoreCase(tableName) == 0) {
                            long countRec = 0;

                            //
                            Map<String, String> recValuesStr = replicaReader.nextRec();
                            while (recValuesStr != null) {
                                String pkValueStr = recValuesStr.get(pkFieldName);
                                JdxRef pkValue = JdxRef.parse(pkValueStr);

                                // Нашли нужный id?
                                if (findRecordId.equals(pkValue)) {
                                    JdxOprType oprType = JdxOprType.valueOfStr(recValuesStr.get(UtJdx.XML_FIELD_OPR_TYPE));
                                    if (oprType == JdxOprType.DEL && skipOprDel) {
                                        log.info("  record found, replica.wsId: " + replica.getInfo().getWsId() + ", OprType == OPR_DEL, skipped");
                                    } else {
                                        log.info("  record found, replica.wsId: " + replica.getInfo().getWsId());
                                        log.debug("  " + recValuesStr);

                                        // В реплике нашлась запись - сохраним данные по реплике
                                        recordsFoundInReplica = true;
                                        replicaInfoData.add(recValuesStr);

                                        // Сохраняем запись
                                        xmlWriter.appendRec();

                                        // Если ищем только последнюю запись - пусть она попадет в реплику как INS
                                        // (так удобнее потом "применять" реплику с командной строки)
                                        if (findLastOne) {
                                            xmlWriter.writeOprType(JdxOprType.INS);
                                        } else {
                                            xmlWriter.writeOprType(oprType);
                                        }

                                        // Запись значений
                                        UtXml.recToWriter(recValuesStr, UtJdx.fieldsToString(table.getFields()), xmlWriter);
                                    }
                                }

                                // Одной записи хватит
                                if (recordsFoundInReplica && findLastOne) {
                                    break;
                                }

                                //
                                countRec++;
                                if (countRec % 200 == 0) {
                                    log.debug("  table: " + readerTableName + ", " + countRec);
                                }

                                //
                                recValuesStr = replicaReader.nextRec();
                            }
                        }

                        // Одной записи хватит
                        if (recordsFoundInReplica && findLastOne) {
                            break;
                        }

                        //
                        readerTableName = replicaReader.nextTable();
                    }


                    // Если в реплике нашлась хоть одна запись - то сохраним данные по реплике
                    if (recordsFoundInReplica) {
                        replicaInfoList.add(replicaInfo);
                    }


                    // Одной записи хватит
                    if (recordsFoundInReplica && findLastOne) {
                        break;
                    }

                } finally {
                    // Закроем читателя Zip-файла
                    if (inputStream != null) {
                        inputStream.close();
                    }
                }

            } catch (Exception e) {
                log.error(Ut.getExceptionMessage(e));
                e.printStackTrace();
            }

        }


        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();


        // Копируем реплику в файл, куда просили
        File outReplicaFile = new File(outFileName);
        FileUtils.copyFile(replicaOut.getData(), outReplicaFile);
        replicaOut.setData(outReplicaFile);

        // Данные по реплике - в info-файл
        UtFile.saveString(UtJson.toString(replicaInfoList), new File(outFileNameInfo));


        //
        return replicaOut;
    }

    static List<File> findFilesInDirs(String replicasDirsName, boolean findLastOne) throws IOException {
        List<File> files = new ArrayList<>();

        //
        String[] replicaFileExtentions = new String[]{"zip"};

        //
        String[] replicasDirsNameArr = replicasDirsName.split(",");
        for (String replicasDirName : replicasDirsNameArr) {
            // Файлы из каталога
            File dir = new File(replicasDirName);
            List<File> filesInDir = new ArrayList<>(FileUtils.listFiles(dir, replicaFileExtentions, true));

            //
            log.info(dir.getCanonicalPath() + ", files: " + filesInDir.size());

            // Отсортируем, чтобы команды в результате появлялись в том порядке, как поступали в очередь реплик (или наоборот - смотря как прпросили)
            if (findLastOne) {
                filesInDir.sort(NameFileComparator.NAME_REVERSE);
            } else {
                filesInDir.sort(NameFileComparator.NAME_COMPARATOR);
            }

            // В список для поиска
            files.addAll(filesInDir);
        }


        return files;
    }


    // Из имени файла извлекает номер версии
    private String parseExeVersion(String exeFileName) {
        // Из "JadatexSync-301.exe" извлекает "301"
        return exeFileName.split("-|\\.")[1];
    }

    public static String getVersion() {
        VersionInfo vi = new VersionInfo("jdtx.repl.main");
        String version = vi.getVersion();
        version = version.replace("SNAPSHOT-", "");
        return version;
    }

    public static JSONObject loadAndValidateJsonFile(String cfgFileName) throws Exception {
        String cfgText = UtFile.loadString(cfgFileName);
        return loadAndValidateJsonStr(cfgText);
    }

    public static JSONObject loadAndValidateJsonStr(String cfgText) throws Exception {
        if (cfgText.length() == 0) {
            throw new XError("cfgText.length() == 0");
        }
        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(cfgText);
        if (cfgData == null) {
            throw new XError("cfgData == null");
        }
        //
        return cfgData;
    }

    public static boolean tableSkipRepl(IJdxTable table) {
        return table.getPrimaryKey().size() == 0;
    }

    /**
     * Фильтрация структуры: убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut
     */
    public static IJdxDbStruct getStructCommon(IJdxDbStruct structActual, IPublicationRuleStorage publicationIn, IPublicationRuleStorage publicationOut) throws Exception {
        IJdxDbStruct structCommon = new JdxDbStruct();
        for (IJdxTable structTable : structActual.getTables()) {
            if (publicationIn.getPublicationRule(structTable.getName()) != null) {
                structCommon.getTables().add(structTable);
            } else if (publicationOut.getPublicationRule(structTable.getName()) != null) {
                structCommon.getTables().add(structTable);
            }
        }

        // Обеспечиваем порядок сортировки таблиц с учетом foreign key
        IJdxDbStruct structCommonSorted = new JdxDbStruct();
        structCommonSorted.getTables().addAll(UtJdx.sortTablesByReference(structCommon.getTables()));

        //
        return structCommonSorted;
    }

    /**
     * Фильтрация структуры: убирание того, чего нет ни в одном из правил публикаций cfgPublications ("in" и "out")
     */
    public static IJdxDbStruct filterStruct(IJdxDbStruct structFull, JSONObject cfgPublications) throws Exception {
        // Правила публикаций
        IPublicationRuleStorage publicationIn = PublicationRuleStorage.loadRules(cfgPublications, structFull, "in");
        IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfgPublications, structFull, "out");

        // Фильтрация структуры
        IJdxDbStruct structFiltered = UtRepl.getStructCommon(structFull, publicationIn, publicationOut);

        //
        return structFiltered;
    }

    /**
     * Возвращает список таблиц из tablesSource, но том порядке,
     * в котором они расположены в описании структуры struct - там список таблиц отсортирован по зависимостям.
     */
    private List<IJdxTable> makeOrderedFromTableList(IJdxDbStruct struct, List<IJdxTable> tablesSource) {
        List<IJdxTable> tablesOrdered = new ArrayList<>();
        //
        for (IJdxTable tableOrderedSample : struct.getTables()) {
            for (IJdxTable tableSource : tablesSource) {
                if (tableOrderedSample.getName().compareTo(tableSource.getName()) == 0) {
                    tablesOrdered.add(tableSource);
                    break;
                }
            }
        }
        //
        return tablesOrdered;
    }

    /**
     * Создаем snapsot-реплики для таблиц tables (фильруем по фильтрам)
     * Делаем обязательно в ОТДЕЛЬНОЙ транзакции (отдельной от изменения структуры).
     * В некоторых СУБД (напр. Firebird) изменение структуры происходит ВНУТРИ транзакций,
     * тогда получится, что пока делается snapshot, аудит не работает.
     * Таким образом данные, вводимые во время подготовки аудита и snapshot-та, не попадут ни в аудит, ни в snapshot,
     * т.к. таблица аудита ЕЩЕ не видна другим транзакциям, а данные, продолжающие поступать в snapshot, УЖЕ не видны нашей транзакции.
     */

    ////////////////////////
    ////////////////////////
    ////////////////////////
    ////////////////////////
    // jdtx.repl.main.api.JdxReplWs.doStructChangesSteps: и разрешаем чужие id - ведь это не инициализация базы, они у нас точно уже есть
    // А вот теперь - зачем теперь параметр forbidNotOwnId - ведь теперь даже при первой инициализации этот парамтер не станет true
    // От чего защищаемся? И как Теперь защищаться?
    ////////////////////////
    ////////////////////////
    ////////////////////////
    public List<IReplica> createSnapshotForTablesFiltered(List<IJdxTable> tables, long selfWsId, long wsIdDestination, IPublicationRuleStorage rulesForSnapshot, boolean forbidNotOwnId) throws Exception {
        log.info("createSendSnapshotForTables, selfWsId: " + selfWsId + ", wsIdDestination: " + wsIdDestination);

        // В tables будет соблюден порядок сортировки таблиц с учетом foreign key.
        // При последующем применении snapsot важен порядок.
        tables = UtJdx.sortTablesByReference(tables);

        // ---
        // Создаем snapshot-реплики (пока без фильтрации записей)

        // Cписок исходных, не фильтрованных реплик
        List<IReplica> replicasSnapshot;

        // Снимок делаем в рамках одной транзакции - чтобы видеть непроитворечивое состояние таблиц
        db.startTran();
        try {
            replicasSnapshot = createSnapshotForTables(tables, selfWsId, rulesForSnapshot, forbidNotOwnId);
            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }


        // ---
        // Фильтруем записи в snapshot-репликах
        // Автор snapshot-реплики, строго говоря, не определен,
        // но чтобы не было ошибок в вычислении выражений, поставим в качестве автора -1.
        long wsIdAuthor = -1;
        List<IReplica> replicasSnapshotFiltered = filterReplicas(replicasSnapshot, wsIdAuthor, wsIdDestination, rulesForSnapshot);


        // ---
        // Не фильтрованные реплики - удаляем файлы
        for (IReplica replica : replicasSnapshot) {
            replica.getData().delete();
        }


        //
        return replicasSnapshotFiltered;
    }


    /**
     * Помещаем реплики replicas в очередь que.
     * Делаем в рамках одной транзакции - чтобы либо все реплики ушли, либо ничего.
     */
    public void sendToQue(List<IReplica> replicas, IJdxReplicaQue que) throws Exception {
        log.info("sendToQue, que: " + ((IJdxQueNamed) que).getQueName());

        db.startTran();
        try {
            for (IReplica replica : replicas) {
                que.push(replica);
            }
            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }

    /**
     * Создаем snapsot-реплики для таблиц tables (без фильтрации записей).
     * Только для таблиц, упомянутых в publicationRules.
     */
    private List<IReplica> createSnapshotForTables(List<IJdxTable> tables, long selfWsId, IPublicationRuleStorage publicationRules, boolean forbidNotOwnId) throws Exception {
        List<IReplica> res = new ArrayList<>();

        //
        for (IJdxTable table : tables) {
            String tableName = table.getName();

            log.info("SnapshotForTables, selfWsId: " + selfWsId + ", table: " + tableName);

            //
            IPublicationRule publicationTableRule = publicationRules.getPublicationRule(tableName);
            if (publicationTableRule == null) {
                // Пропускаем
                log.info("SnapshotForTables, skip createSnapshot, not found in publicationRules, table: " + tableName);
            } else {
                // Создаем snapshot-реплику
                IReplica replicaSnapshot = createReplicaSnapshotForTable(selfWsId, publicationTableRule, forbidNotOwnId);
                res.add(replicaSnapshot);
            }

        }

        //
        log.info("SnapshotForTables, selfWsId: " + selfWsId + ", done");

        //
        return res;
    }

    /**
     * Из списка не фильрованных replicasSnapshot делает
     * список реплик, отфильтрованных по правилам ruleForSnapshot
     */
    private List<IReplica> filterReplicas(List<IReplica> replicasSnapshot, long wsIdAuthor, long wsIdDestination, IPublicationRuleStorage ruleForSnapshot) throws Exception {
        // Результирующий список фильтрованных
        List<IReplica> replicasSnapshotFiltered = new ArrayList<>();

        // Фильтр записей
        IReplicaFilter filter = new ReplicaFilter();

        // Фильтр, параметры: автор реплики
        filter.getFilterParams().put("wsAuthor", String.valueOf(wsIdAuthor));
        // Фильтр, параметры: получатель реплики
        filter.getFilterParams().put("wsDestination", String.valueOf(wsIdDestination));

        // Фильтруем записи
        for (IReplica replicaSnapshot : replicasSnapshot) {
            IReplica replicaForWs = filter.convertReplicaForWs(replicaSnapshot, ruleForSnapshot);
            replicasSnapshotFiltered.add(replicaForWs);
        }

        //
        return replicasSnapshotFiltered;
    }

    /**
     * Очистка файлов, котрорые есть в каталоге, но которых нет в базе.
     * Это бывает по разным причинам.
     */
    public static void clearTrashFiles(IJdxQue que) throws Exception {
        log.info("clearTrashFiles, que: " + que.getQueName() + ", baseDir: " + que.getBaseDir());

        // Сколько реплик есть в рабочем каталоге?
        long trashNo = que.getMaxNoFromDir();

        if (trashNo < 0) {
            throw new XError("que.getMaxNoFromDir() < 0");
        }

        // Сколько реплик есть у нас в базе?
        long clearFromNo = que.getMaxNo();

        // Лишних - убираем
        while (trashNo > clearFromNo) {
            log.warn("clearTrashFiles, replica.no: " + trashNo);

            // Файл реплики
            String actualFileName = JdxStorageFile.getFileName(trashNo);
            File actualFile = new File(que.getBaseDir() + actualFileName);

            // Переносим файл в мусорку
            if (actualFile.exists()) {
                File trashFile = new File(que.getBaseDir() + "/trash/" + getFileNameTrash(trashNo));
                FileUtils.moveFile(actualFile, trashFile);
                log.warn("clearTrashFiles, move, actualFile: " + actualFile.getAbsolutePath() + ", trashFile: " + trashFile.getAbsolutePath());
            }
            //
            trashNo = trashNo - 1;
        }
    }

    private static String getFileNameTrash(long no) {
        DateTime dt = new DateTime();
        return UtString.padLeft(String.valueOf(no), 9, '0') + "-" + dt.toString("YYYY.MM.dd_HH.mm.ss.SSS") + ".zip";
    }


}
