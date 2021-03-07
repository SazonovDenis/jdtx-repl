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
     * Реплика на вставку всех существующих записей в этой БД.
     * <p>
     * Используется при включении новой БД в систему:
     * В числе первых реплик для сервера.
     */
    public IReplica createReplicaTableSnapshot(long wsId, IPublicationRule publicationRule, long age, boolean forbidNotOwnId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, wsId, forbidNotOwnId);
        dataSelector.readAllRecords(publicationRule, xmlWriter);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaTableByIdList(long wsId, IJdxTable publicationTable, long age, Collection<Long> idList) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Начинаем писать файл с данными
        JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument();

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, wsId, false);
        String publicationFields = UtJdx.fieldsToString(publicationTable.getFields());
        dataSelector.readRecordsByIdList(publicationTable.getName(), idList, publicationFields, xmlWriter);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaSetDbStruct() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_DB_STRUCT);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с описанием текущей структуры БД
        OutputStream zipOutputStream = replicaWriter.newFileOpen("dat.xml");

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
        StringInputStream versionStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(versionStream, zipOutputStream);

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
        StringInputStream versionStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(versionStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    public IReplica createReplicaSetQueInNo(long destinationWsId, long queInNo) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_QUE_IN_NO);
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));

        // Стартуем формирование файла реплики
        UtReplicaWriter replicaWriter = new UtReplicaWriter(replica);
        replicaWriter.replicaFileStart();

        // Открываем запись файла с информацией о получателе
        OutputStream zipOutputStream = replicaWriter.newFileOpen("info.json");

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        cfgInfo.put("queInNo", queInNo);
        String cfgInfoStr = UtJson.toString(cfgInfo);
        StringInputStream versionStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(versionStream, zipOutputStream);

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
        StringInputStream versionStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(versionStream, zipOutputStream);


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
        StringInputStream versionStream = new StringInputStream(cfgInfoStr);
        UtFile.copyStream(versionStream, zipOutputStream);

        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();

        //
        return replica;
    }

    /**
     * Ищем запись в истории реплик, собираем все операции с этой записью в одну реплику.
     * Метод применяется при восстановлении асинхронно удаленных записей.
     */
    public IReplica findRecordInReplicas(String findTableName, String findRecordIdStr, String replicasDirName, long wsId, boolean skipOprDel, String outReplicaInfoFile) throws Exception {
        String inFileMask = "*.zip";

        // Список файлов, ищем в них
        File dir = new File(replicasDirName);
        File[] files = dir.listFiles((FileFilter) new WildcardFileFilter(inFileMask, IOCase.INSENSITIVE));
        //
        if (files == null) {
            return null;
        }
        //
        Arrays.sort(files, new NameFileComparator());


        // Тут сохраним данные по всем репликам
        JSONArray replicaInfoList = new JSONArray();


        //
        JdxRef findRecordId;
        if (findRecordIdStr.contains(":")) {
            findRecordId = JdxRef.parse(findRecordIdStr);
        } else {
            IRefDecoder decoder = new RefDecoder(db, wsId);
            findRecordId = decoder.get_ref(findTableName, Long.parseLong(findRecordIdStr));
            log.info("findRecordId: " + findRecordId);
        }

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
        xmlWriter.startTable(findTableName);

        //
        int countFile = 0;
        while (countFile < files.length) {
            File file = files[countFile];

            countFile++;
            log.info(countFile + "/" + files.length + ", file: " + file.getName());

            //
            try {
                IReplica replica = new ReplicaFile();
                replica.setFile(file);
                JdxReplicaReaderXml.readReplicaInfo(replica);

                if (replica.getInfo().getReplicaType() != JdxReplicaType.IDE && replica.getInfo().getReplicaType() != JdxReplicaType.SNAPSHOT) {
                    log.info("  skip, replicaType: " + replica.getInfo().getReplicaType());
                    continue;
                }

                //
                InputStream inputStream = null;
                try {
                    // Распакуем XML-файл из Zip-архива
                    inputStream = JdxReplicaReaderXml.createInputStreamData(replica);

                    //
                    JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

                    //
                    IJdxTable table = null;
                    List<IJdxTable> tables = struct.getTables();
                    for (IJdxTable t : tables) {
                        if (t.getName().compareToIgnoreCase(findTableName) == 0) {
                            table = t;
                            break;
                        }
                    }
                    if (table == null) {
                        throw new XError("Таблица не найдена в структуре: " + findTableName);
                    }
                    String idFieldName = table.getPrimaryKey().get(0).getName();


                    // Тут сохраним данные по реплике
                    boolean recordsFoundInReplica = false;
                    Map replicaInfo = new HashMap();
                    replicaInfo.put("wsId", replica.getInfo().getWsId());
                    replicaInfo.put("age", replica.getInfo().getAge());
                    replicaInfo.put("replicaType", replica.getInfo().getReplicaType());
                    replicaInfo.put("dtFrom", replica.getInfo().getDtFrom());
                    replicaInfo.put("dtTo", replica.getInfo().getDtTo());
                    JSONArray replicaData = new JSONArray();
                    replicaInfo.put("data", replicaData);

                    //
                    String tableName = replicaReader.nextTable();
                    while (tableName != null) {
                        //
                        if (tableName.compareToIgnoreCase(findTableName) == 0) {
                            log.info("  table: " + tableName + ", wsId: " + replica.getInfo().getWsId());

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

                                //
                                boolean doSkipRec = false;
                                int oprType = UtJdx.intValueOf(recValues.get(UtJdx.XML_FIELD_OPR_TYPE));
                                if (oprType == JdxOprType.OPR_DEL && skipOprDel) {
                                    doSkipRec = true;
                                    log.info("  record OPR_DEL, skipped");
                                }

                                // Нашли id?
                                if (!doSkipRec && idValueRef.equals(findRecordId)) {
                                    log.info("  record found");

                                    // В реплике нашлась запись - сохраним данные по реплике
                                    recordsFoundInReplica = true;
                                    replicaData.add(recValues);

                                    // Сохраняем запись
                                    xmlWriter.appendRec();

                                    //
                                    xmlWriter.setOprType(oprType);

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
                                            if (fieldValueRef.ws_id == -1 && replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
                                                fieldValueRef.ws_id = replica.getInfo().getWsId();
                                            }
                                            xmlWriter.setRecValue(fieldName, fieldValueRef.toString());
                                        } else {
                                            // Это просто значение
                                            xmlWriter.setRecValue(fieldName, fieldValue);
                                        }
                                    }
                                }

                                //
                                countRec++;
                                if (countRec % 200 == 0) {
                                    log.info("  table: " + tableName + ", " + countRec);
                                }

                                //
                                recValues = replicaReader.nextRec();
                            }
                        }

                        //
                        tableName = replicaReader.nextTable();
                    }


                    // Если в реплике нашлась хоть одна запись - то сохраним данные по реплике
                    if (recordsFoundInReplica) {
                        replicaInfoList.add(replicaInfo);
                    }


                } finally {
                    // Закроем читателя Zip-файла
                    if (inputStream != null) {
                        inputStream.close();
                    }
                }

            } catch (Exception e) {
                log.error(Ut.getExceptionMessage(e));
                //e.printStackTrace();
            }

        }


        // Заканчиваем формирование файла реплики
        replicaWriter.replicaFileClose();


        // Данные по реплике - в файл
        if (outReplicaInfoFile != null) {
            UtFile.saveString(UtJson.toString(replicaInfoList), new File(outReplicaInfoFile));
        }


        //
        return replicaOut;
    }

    public IReplica findRecordInReplicas(String findTableName, String findRecordIdStr, String replicasDirName, long wsId, boolean skipOprDel) throws Exception {
        return findRecordInReplicas(findTableName, findRecordIdStr, replicasDirName, wsId, skipOprDel, null);
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

    static IJdxDbStruct getStructCommon(IJdxDbStruct structActual, IPublicationStorage publicationIn, IPublicationStorage publicationOut) throws Exception {
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


}
