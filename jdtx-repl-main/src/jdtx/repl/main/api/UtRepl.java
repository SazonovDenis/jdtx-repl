package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.io.*;
import org.apache.commons.io.comparator.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.apache.tools.ant.filters.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

/**
 * Утилиты (связанные именно с репликацией) верхнего уровня, готовые команды.
 * todo: Куча всего в одном месте, и еще куча лишнего. Это плохо
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
        // Создание базовых структур
        UtDbObjectManager objectManager = new UtDbObjectManager(db);
        objectManager.createReplBase(wsId, guid);

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

        // Пока в стотоянии MUTE
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
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();

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
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, selfWsId, forbidNotOwnId);
        dataSelector.readAllRecords(publicationRule, xmlWriter);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaSetDbStruct(boolean sendSnapshot) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_DB_STRUCT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о команде
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("sendSnapshot", sendSnapshot);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream infoStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(infoStream, zipOutputStream);


        // Открываем запись файла с описанием текущей структуры БД
        zipOutputStream = replicaWriter.newFileOpen("dat.xml");

        // Пишем файл с описанием структуры
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        zipOutputStream.write(struct_rw.getBytes(struct));

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
    
/*
todo !!!!!!!!!!!!!!!!!!!!!!!! семейство методов createReplica*** свести к одному по аналогии с ReportReplica
*/

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
     * @param replicasDirsName Каталог с репликами для поиска, например "d:/temp/"
     * @param skipOprDel       Пропускать реплики на удаление записи
     * @param findLastOne      Найти только один последжний вариант записи
     * @param outFileName      Файл для реплики-результата, например "d:/temp/ABN_10_12345.zip"
     * @return Реплика со всеми операциями, найденными для запрошенной записи
     * <p>
     * todo - а как насчет ПОРЯДКА реплик? Получу ли я именно ПОСЛЕДНЮЮ версию записи??? СОбирать отдельно, сортировать по дате (аудита), а потом только писать во Writer
     */
    public IReplica findRecordInReplicas(String tableName, String recordIdStr, String replicasDirsName, boolean skipOprDel, boolean findLastOne, String outFileName) throws Exception {
        if (!skipOprDel && findLastOne) {
            throw new XError("При поиске последннего варианта (findLastOne == true) нужно skipOprDel == true");
        }

        //
        String inFileMask = "*.zip";

        //
        String outFileNameInfo;
        if (outFileName.endsWith(".zip")) {
            outFileNameInfo = outFileName.replace(".zip", ".json");
        } else {
            outFileNameInfo = outFileName + ".json";
        }

        // Список файлов-реплик в каталогах replicasDirsName, ищем в них
        List<File> files = new ArrayList<>();
        String[] replicasDirsNameArr = replicasDirsName.split(",");
        for (String replicasDirName : replicasDirsNameArr) {
            // Файлы из каталога
            File dir = new File(replicasDirName);
            File[] filesInDir_arr = dir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));
            if (filesInDir_arr == null) {
                throw new XError("Каталог недоступен: " + dir.getCanonicalPath());
            }
            log.info(dir.getCanonicalPath() + ", files: " + filesInDir_arr.length);
            List<File> filesInDir = Arrays.asList(filesInDir_arr);

            // Отсортируем, чтобы команды в результате появлялись в том порядке, как поступали в очередь реплик (или наоборот - смотря как прпросили)
            if (findLastOne) {
                filesInDir.sort(NameFileComparator.NAME_REVERSE);
            } else {
                filesInDir.sort(NameFileComparator.NAME_COMPARATOR);
            }

            // В список для поиска
            files.addAll(filesInDir);
        }

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
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();

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
        String idFieldName = table.getPrimaryKey().get(0).getName();


        //
        int countFile = 0;
        while (countFile < files.size()) {
            File file = files.get(countFile);

            countFile++;
            log.debug(countFile + "/" + files.size() + ", file: " + file.getName());

            //
            try {
                IReplica replica = new ReplicaFile();
                replica.setFile(file);
                JdxReplicaReaderXml.readReplicaInfo(replica);

                if (replica.getInfo().getReplicaType() != JdxReplicaType.IDE && replica.getInfo().getReplicaType() != JdxReplicaType.SNAPSHOT) {
                    log.debug("  skip, replicaType: " + replica.getInfo().getReplicaType());
                    continue;
                }

                //
                InputStream inputStream = null;
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
                    replicaInfo.put("file", replica.getFile().getAbsolutePath());
                    JSONArray replicaData = new JSONArray();
                    replicaInfo.put("data", replicaData);

                    //
                    String readerTableName = replicaReader.nextTable();
                    while (readerTableName != null) {
                        //
                        if (readerTableName.compareToIgnoreCase(tableName) == 0) {
                            log.info("file: " + file.getName() + ", wsId: " + replica.getInfo().getWsId());

                            //
                            long countRec = 0;

                            //
                            Map recValues = replicaReader.nextRec();
                            while (recValues != null) {
                                Object idValue = recValues.get(idFieldName);
                                JdxRef idValueRef = JdxRef.parse((String) idValue);
                                //
                                if (idValueRef.ws_id == -1 && replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
                                    idValueRef.ws_id = replica.getInfo().getWsId();
                                }

                                // Нашли id?
                                if (idValueRef.equals(findRecordId)) {
                                    int oprType = UtJdx.intValueOf(recValues.get(UtJdx.XML_FIELD_OPR_TYPE));
                                    if (oprType == JdxOprType.OPR_DEL && skipOprDel) {
                                        log.info("  record.OprType == OPR_DEL, skipped");
                                    } else {
                                        log.info("  record found");
                                        log.debug("  " + recValues);

                                        // В реплике нашлась запись - сохраним данные по реплике
                                        recordsFoundInReplica = true;
                                        replicaData.add(recValues);

                                        // Сохраняем запись
                                        xmlWriter.appendRec();

                                        // Если ищем только одну запись - пусть она попадет в реплику как INS
                                        if (findLastOne) {
                                            xmlWriter.setOprType(JdxOprType.OPR_INS);
                                        } else {
                                            xmlWriter.setOprType(oprType);
                                        }

                                        // Запись значения с проверкой/перекодировкой ссылок
                                        for (IJdxField field : table.getFields()) {
                                            String fieldName = field.getName();
                                            Object fieldValue = recValues.get(fieldName);
                                            // Запись значения с проверкой/перекодировкой ссылок
                                            IJdxTable refTable = field.getRefTable();
                                            if (field.isPrimaryKey() || refTable != null) {
                                                // Это значение - ссылка
                                                JdxRef fieldValueRef = JdxRef.parse((String) fieldValue);
                                                // Дополнение ссылки
                                                if (fieldValueRef != null && fieldValueRef.ws_id == -1 && replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
                                                    fieldValueRef.ws_id = replica.getInfo().getWsId();
                                                }
                                                xmlWriter.setRecValue(fieldName, String.valueOf(fieldValueRef));
                                            } else {
                                                // Это просто значение
                                                xmlWriter.setRecValue(fieldName, fieldValue);
                                            }
                                        }
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
                                recValues = replicaReader.nextRec();
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
        FileUtils.copyFile(replicaOut.getFile(), outReplicaFile);
        replicaOut.setFile(outReplicaFile);

        // Данные по реплике - в info-файл
        UtFile.saveString(UtJson.toString(replicaInfoList), new File(outFileNameInfo));


        //
        return replicaOut;
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

    static IJdxDbStruct getStructCommon(IJdxDbStruct structActual, IPublicationRuleStorage publicationIn, IPublicationRuleStorage publicationOut) throws Exception {
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
     * Создаем snapsot-реплики для таблиц tables,
     * без последующей фильтрации записей,
     * но только для таблиц, упомянутых в publicationRules
     */
    public List<IReplica> createSnapshotsForTables(List<IJdxTable> tables, long selfWsId, IPublicationRuleStorage publicationRules, boolean forbidNotOwnId) throws Exception {
        List<IReplica> res = new ArrayList<>();

        //
        UtRepl utRepl = new UtRepl(db, struct);

        //
        for (IJdxTable table : tables) {
            String tableName = table.getName();

            log.info("SnapshotForTables, wsAuthor: " + selfWsId + ", table: " + tableName);

            //
            IPublicationRule publicationTableRule = publicationRules.getPublicationRule(tableName);
            if (publicationTableRule == null) {
                // Пропускаем
                log.info("SnapshotForTables, skip createSnapshot, not found in publicationRules, table: " + tableName);
            } else {
                // Создаем snapshot-реплику
                IReplica replicaSnapshot = utRepl.createReplicaSnapshotForTable(selfWsId, publicationTableRule, forbidNotOwnId);
                res.add(replicaSnapshot);
            }

        }

        //
        log.info("SnapshotForTables, wsAuthor: " + selfWsId + ", done");

        //
        return res;
    }

}
