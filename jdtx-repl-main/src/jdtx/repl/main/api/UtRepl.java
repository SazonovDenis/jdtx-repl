package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.io.*;
import org.apache.commons.io.comparator.*;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.logging.*;
import org.apache.tools.ant.filters.*;
import org.joda.time.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * Утилитный класс репликатора.
 * todo: Куча всего в одном месте. Зачем он вообще нужен в таком виде - неясно. А еще - есть контекст outputStream - ваще ужоссс
 */
public class UtRepl {

    private Db db;
    private IJdxDbStruct struct;

    protected static Log log = LogFactory.getLog("jdtx.Ut");

    //
    private OutputStream outputStream = null;
    private ZipOutputStream zipOutputStream = null;
    private JdxReplicaWriterXml writerXml = null;


    public UtRepl(Db db, IJdxDbStruct struct) throws Exception {
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
        UtDbStructApprover dbStructRW = new UtDbStructApprover(db);

        // Для начала "разрешенная" структура будет пустая
        IJdxDbStruct structAllowed = new JdxDbStruct();
        dbStructRW.setDbStructAllowed(structAllowed);

        // Для начала "фиксированная" структура будет пустая
        IJdxDbStruct structFixed = new JdxDbStruct();
        dbStructRW.setDbStructFixed(structFixed);
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

    public DataStore getInfoSrv() throws Exception {
        DataStore st = db.loadSql(UtReplSql.sql_srv);
        return st;
    }

    /**
     * Искусственно увеличить возраст рабочей станции
     */
    public long incAuditAge() throws Exception {
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        // Проверяем, что весь свой аудит мы уже выложили в очередь
        long auditAgeDone = stateManager.getAuditAgeDone();
        long auditAgeActual = auditAgeManager.getAuditAge();
        if (auditAgeActual != auditAgeDone) {
            throw new XError("invalid auditAgeActual <> auditAgeDone, auditAgeDone: " + auditAgeDone + ", auditAgeActual: " + auditAgeActual);
        }

        //
        return auditAgeManager.incAuditAge();
    }


    /**
     * Узнать возраст рабочей станции
     */
    public long getAuditAge() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        return ut.getAuditAge();
    }

    // Добавляет файл внутри формируемого Zip-архива
    void addFileToOutput(String fileName) throws Exception {
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOutputStream.putNextEntry(zipEntry);
    }

    void createOutputZipFile(IReplica replica) throws Exception {
        // Файл
        String fileNameTemplate = UtString.padLeft(String.valueOf(replica.getInfo().getWsId()), 3, '0') + "-" + UtString.padLeft(String.valueOf(replica.getInfo().getAge()), 9, '0');
        File outFile = File.createTempFile("~jdx-" + fileNameTemplate + "-", ".zip");
        outputStream = new FileOutputStream(outFile);
        //
        replica.setFile(outFile);

        // Формируем Zip-архив
        zipOutputStream = new ZipOutputStream(outputStream);

        // Файл "dat.info" внутри Zip-архива (заголовок с информацией о реплике, сериализация IReplicaInfo)
        ZipEntry zipEntryHead = new ZipEntry("dat.info");
        zipOutputStream.putNextEntry(zipEntryHead);
        String json = replica.getInfo().toString();
        zipOutputStream.write(json.getBytes("utf-8"));
        zipOutputStream.closeEntry();
    }

    void createOutputXML(IReplica replica) throws Exception {
        createOutputZipFile(replica);

        // Файл "dat.xml" (данные) внутри Zip-архива
        addFileToOutput("dat.xml");

        // Писатель для XML-файла
        writerXml = new JdxReplicaWriterXml(zipOutputStream);
    }

    void closeOutputXML() throws Exception {
        // Заканчиваем запись в XML-файл
        if (writerXml != null) {
            writerXml.close();
        }

        // Заканчиваем запись в в zip-архив
        zipOutputStream.closeEntry();
        zipOutputStream.finish();
        zipOutputStream.close();

        // Закрываем файл
        outputStream.close();
    }

    /**
     * Собрать аудит и подготовить реплику по правилам публикации publication
     * от для возраста age.
     */
    public IReplica createReplicaFromAudit(long wsId, IPublication publication, long age) throws Exception {
        log.info("createReplicaFromAudit, wsId: " + wsId + ", age: " + age);

        //
        UtAuditSelector utrr = new UtAuditSelector(db, struct, wsId);

        // Для выборки из аудита - узнаем интервалы id в таблицах аудита
        Map auditInfo = utrr.loadAutitIntervals(publication, age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setDtFrom((DateTime) auditInfo.get("z_opr_dttm_from"));
        replica.getInfo().setDtTo((DateTime) auditInfo.get("z_opr_dttm_to"));
        replica.getInfo().setReplicaType(JdxReplicaType.IDE);


        // Стартуем запись реплики
        createOutputXML(replica);


        // Пишем заголовок
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replica);


        // Забираем аудит по порядку сортировки таблиц в struct
        for (IJdxTable publicationTable : publication.getData().getTables()) {
            // Интервал id в таблице аудита, который покрывает возраст age
            Map autitInfoTable = (Map) auditInfo.get(publicationTable.getName());
            if (autitInfoTable != null) {
                long fromId = (long) autitInfoTable.get("z_id_from");
                long toId = (long) autitInfoTable.get("z_id_to");

                //
                log.info("createReplicaFromAudit: " + publicationTable.getName() + ", age: " + age + ", z_id: [" + fromId + ".." + toId + "]");

                //
                String publicationFields = Publication.filedsToString(publicationTable.getFields());
                utrr.readAuditData_ByInterval(publicationTable.getName(), publicationFields, fromId, toId, writerXml);
            }
        }

        //
        writerXml.closeDocument();
        closeOutputXML();


        //
        return replica;
    }


    /**
     * Реплика на вставку всех существующих записей в этой БД.
     * <p>
     * Используется при включении новой БД в систему:
     * самая первая (установочная) реплика для сервера.
     */
    public IReplica createReplicaTableSnapshot(long wsId, IJdxTable publicationTable, long age) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);

        // Открываем запись
        createOutputXML(replica);


        // Пишем
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replica);

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, wsId);
        String publicationFields = Publication.filedsToString(publicationTable.getFields());
        dataSelector.readAllRecords(publicationTable.getName(), publicationFields, writerXml);


        // Заканчиваем запись
        writerXml.closeDocument();
        closeOutputXML();


        //
        return replica;
    }

    public IReplica createReplicaTableByIdList(long wsId, IJdxTable publicationTable, long age, Collection<Long> idList) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setDbStructCrc(UtDbComparer.getDbStructCrcTables(struct));
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);
        replica.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);

        // Открываем запись
        createOutputXML(replica);


        // Пишем
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replica);

        // Забираем все данные из таблиц (по порядку сортировки таблиц в struct с учетом foreign key)
        UtDataSelector dataSelector = new UtDataSelector(db, struct, wsId);
        String publicationFields = Publication.filedsToString(publicationTable.getFields());
        dataSelector.readRecordsByIdList(publicationTable.getName(), idList, publicationFields, writerXml);


        // Заканчиваем запись
        writerXml.closeDocument();
        closeOutputXML();


        //
        return replica;
    }

    public IReplica createReplicaSetDbStruct() throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_DB_STRUCT);

        // Открываем запись
        createOutputXML(replica);

        // Файл с описанием текущей структуры БД
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        zipOutputStream.write(struct_rw.getBytes(struct));

        // Заканчиваем запись
        closeOutputXML();

        //
        return replica;
    }

    public IReplica createReplicaUnmute(long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.UNMUTE);

        // Начинаем запись
        // В этой реплике - информация о получателе
        createOutputZipFile(replica);

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);

        // Открываем запись файла с информацией о получателе
        addFileToOutput("info.json");
        String version = UtJson.toString(cfgInfo);
        StringInputStream versionStream = new StringInputStream(version);
        UtFile.copyStream(versionStream, zipOutputStream);

        // Заканчиваем запись
        closeOutputXML();

        //
        return replica;
    }

    public IReplica createReplicaMute(long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.MUTE);

        // Начинаем запись
        // В этой реплике - информация о получателе
        createOutputZipFile(replica);

        // Информация о получателе
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);

        // Открываем запись файла с информацией о получателе
        addFileToOutput("info.json");
        String version = UtJson.toString(cfgInfo);
        StringInputStream versionStream = new StringInputStream(version);
        UtFile.copyStream(versionStream, zipOutputStream);

        // Заканчиваем запись
        closeOutputXML();

        //
        return replica;
    }

    public IReplica createReplicaAppUpdate(String exeFileName) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.UPDATE_APP);

        //
        File exeFile = new File(exeFileName);


        // Начинаем запись
        // В этой реплике - версия приложения и бинарник для обновления (для запуска)
        createOutputZipFile(replica);


        // Открываем запись файла с версией
        addFileToOutput("version");
        String version = parseExeVersion(exeFile.getName());
        StringInputStream versionStream = new StringInputStream(version);
        UtFile.copyStream(versionStream, zipOutputStream);


        // Открываем запись файла - бинарника для обновления
        addFileToOutput(exeFile.getName());

        // Пишем содержимое exe
        InputStream exeFileStream = new FileInputStream(exeFile);
        UtFile.copyStream(exeFileStream, zipOutputStream);


        // Заканчиваем запись
        closeOutputXML();

        //
        return replica;
    }

    public IReplica createReplicaSetCfg(JSONObject cfg, String cfgType, long destinationWsId) throws Exception {
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(JdxReplicaType.SET_CFG);


        // Начинаем запись
        // В этой реплике - информация о конфиге и сам конфиг
        createOutputZipFile(replica);


        // Информация о конфиге
        JSONObject cfgInfo = new JSONObject();
        cfgInfo.put("destinationWsId", destinationWsId);
        cfgInfo.put("cfgType", cfgType);

        // Открываем запись файла с информацией о конфиге
        addFileToOutput("cfg.info.json");
        String version = UtJson.toString(cfgInfo);
        StringInputStream versionStream = new StringInputStream(version);
        UtFile.copyStream(versionStream, zipOutputStream);


        // Открываем запись файла - сам конфиг
        addFileToOutput("cfg.json");

        // Пишем содержимое конфига
        String cfgStr = UtJson.toString(cfg);
        StringInputStream cfgStrStream = new StringInputStream(cfgStr);
        UtFile.copyStream(cfgStrStream, zipOutputStream);


        // Заканчиваем запись
        closeOutputXML();

        //
        return replica;
    }

    /**
     * Ищем запись в истории реплик, собираем все операции с этой записью в одну реплику.
     * Метод применяется при восстановлении асинхронно удаленных записей.
     */
    public IReplica findRecordInReplicas(String findTableName, String findRecordIdStr, String replicasDirName, boolean skipOprDel) throws Exception {
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


        //
        JdxRef findRecordId = JdxRef.parse(findRecordIdStr);

        //
        IReplica replicaOut = new ReplicaFile();
        replicaOut.getInfo().setReplicaType(JdxReplicaType.SNAPSHOT);

        // Стартуем запись реплики
        createOutputXML(replicaOut);

        // Пишем заголовок
        writerXml.startDocument();
        writerXml.writeReplicaHeader(replicaOut);
        //
        writerXml.startTable(findTableName);


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
                    inputStream = UtRepl.getReplicaInputStream(replica);

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
                        //throw new XError("Таблица не найдена: " + findTableName);
                    }
                    String idFieldName = table.getPrimaryKey().get(0).getName();

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
                                int oprType = Integer.valueOf((String) recValues.get("Z_OPR"));
                                if (oprType == JdxOprType.OPR_DEL && skipOprDel) {
                                    doSkipRec = true;
                                    log.info("  record OPR_DEL, skipped");
                                }

                                // Нашли id?
                                if (!doSkipRec && idValueRef.equals(findRecordId)) {
                                    log.info("  record found");

                                    // Сохраняем запись
                                    writerXml.appendRec();

                                    //
                                    writerXml.setOprType(oprType);

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
                                            writerXml.setRecValue(fieldName, fieldValueRef.toString());
                                        } else {
                                            // Это просто значение
                                            writerXml.setRecValue(fieldName, fieldValue);
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


        //
        writerXml.closeDocument();
        closeOutputXML();

        //
        return replicaOut;
    }

    // Из имени файла извлекает номер версии
    private String parseExeVersion(String exeFileName) {
        // Из "JadatexSync-301.exe" извлекает "301"
        return exeFileName.split("-|\\.")[1];
    }

    public static InputStream getReplicaInputStream(IReplica replica) throws IOException {
        return createInputStream(replica, ".xml");
    }

    public static InputStream createInputStream(IReplica replica, String dataFileMask) throws IOException {
        InputStream inputStream = null;
        ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(replica.getFile()));
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
            String name = entry.getName();
            if (name.endsWith(dataFileMask)) {
                inputStream = zipInputStream;
                break;
            }
        }
        if (inputStream == null) {
            throw new XError("Not found [" + dataFileMask + "] in replica: " + replica.getFile());
        }

        return inputStream;
    }

    public static String getVersion() {
        VersionInfo vi = new VersionInfo("jdtx.repl.main");
        String version = vi.getVersion();
        version = version.replace("SNAPSHOT-", "");
        return version;
    }

    public static JSONObject loadAndValidateCfgFile(String cfgFileName) throws Exception {
        String appCfg = UtFile.loadString(cfgFileName);
        JSONObject cfgData = (JSONObject) UtJson.toObject(appCfg);
        return cfgData;
    }

    public static boolean tableSkipRepl(IJdxTable table) {
        return table.getPrimaryKey().size() == 0;
    }

    static IJdxDbStruct getStructCommon(IJdxDbStruct structActual, IPublication publicationIn, IPublication publicationOut) throws Exception {
        IJdxDbStruct structCommon = new JdxDbStruct();
        for (IJdxTable publicationTable : publicationIn.getData().getTables()) {
            if (structCommon.getTable(publicationTable.getName()) == null) {
                IJdxTable structTable = structActual.getTable(publicationTable.getName());
                structCommon.getTables().add(structTable);
            }
        }
        for (IJdxTable publicationTable : publicationOut.getData().getTables()) {
            if (structCommon.getTable(publicationTable.getName()) == null) {
                IJdxTable structTable = structActual.getTable(publicationTable.getName());
                structCommon.getTables().add(structTable);
            }
        }

        // Обеспечиваем порядок сортировки таблиц с учетом foreign key
        IJdxDbStruct structCommonSorted = new JdxDbStruct();
        structCommonSorted.getTables().addAll(JdxUtils.sortTablesByReference(structCommon.getTables()));

        //
        return structCommonSorted;
    }

    static void fillPublications(JSONObject cfgDbPublications, IJdxDbStruct structActual, IPublication publicationIn, IPublication publicationOut) throws Exception {
        if (cfgDbPublications != null) {
            String cfgPublicationIn = (String) cfgDbPublications.get("in");
            String cfgPublicationOut = (String) cfgDbPublications.get("out");

            JSONObject cfgDbPublicationIn = (JSONObject) cfgDbPublications.get(cfgPublicationIn);
            JSONObject cfgDbPublicationOut = (JSONObject) cfgDbPublications.get(cfgPublicationOut);

            // Правила публикаций: publicationIn
            publicationIn.loadRules(cfgDbPublicationIn, structActual);

            // Правила публикаций: publicationOut
            publicationOut.loadRules(cfgDbPublicationOut, structActual);
        }
    }


}
