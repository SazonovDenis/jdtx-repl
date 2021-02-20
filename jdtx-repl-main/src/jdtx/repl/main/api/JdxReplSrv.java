package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;


/**
 * Контекст сервера
 */
public class JdxReplSrv {

    // Общая очередь на сервере
    IJdxQue queCommon;

    // Источник для чтения/отправки сообщений всех рабочих станций
    Map<Long, IMailer> mailerList;

    //
    Db db;
    private IJdxDbStruct struct;

    //
    private String dataRoot;

    //
    protected static Log log = LogFactory.getLog("jdtx.Server");


    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;

        // Общая очередь на сервере
        queCommon = new JdxQueCommon(db, UtQue.QUE_COMMON, UtQue.STATE_AT_WS);

        // Почтовые курьеры для чтения/отправки сообщений, для каждой рабочей станции
        mailerList = new HashMap<>();
    }

    public IMailer getMailer() {
        long wsId = 1; // Ошибки сервера кладем в ящик рабьочей станции №1
        return mailerList.get(wsId);
    }

    /**
     * Сервер, настройка
     */
    public void init() throws Exception {
        MDC.put("serviceName", "srv");

        //
        dataRoot = new File(db.getApp().getRt().getChild("app").getValueString("dataRoot")).getCanonicalPath();
        dataRoot = UtFile.unnormPath(dataRoot) + "/";
        log.info("dataRoot: " + dataRoot);

        // Проверка версии служебных структур в БД
        UtDbObjectManager ut = new UtDbObjectManager(db);
        ut.checkReplVerDb();

        // Проверка, что инициализация станции прошла
        ut.checkReplDb();

        // Чтение своей конфигурации
        UtCfgMarker utCfgMarker = new UtCfgMarker(db);
        JSONObject cfgWs = utCfgMarker.getSelfCfg(UtCfgType.WS);
        JSONObject cfgPublications = utCfgMarker.getSelfCfg(UtCfgType.PUBLICATIONS);


        // Общая очередь
        String queCommon_DirLocal = dataRoot + "srv/que_Common/";
        queCommon.setDataRoot(queCommon_DirLocal);


        // Почтовые курьеры, отдельные для каждой станции
        DataStore st = loadWsList(0);
        for (DataRecord rec : st) {
            long wsId = rec.getValueLong("id");

            // Рабочие каталоги мейлера
            String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
            String mailLocalDirTmp = dataRoot + "srv/ws_" + sWsId + "_tmp/";

            // Конфиг для мейлера
            JSONObject cfgMailer = new JSONObject();
            String guid = rec.getValueString("guid");
            String url = (String) cfgWs.get("url");
            cfgMailer.put("guid", guid);
            cfgMailer.put("url", url);
            cfgMailer.put("localDirTmp", mailLocalDirTmp);

            // Мейлер
            IMailer mailer = new MailerHttp();
            mailer.init(cfgMailer);

            //
            mailerList.put(wsId, mailer);
        }


        // Чтение структуры БД
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct structActual = reader.readDbStruct();


        // Правила публикаций
        IPublicationStorage publicationIn = new PublicationStorage();
        IPublicationStorage publicationOut = new PublicationStorage();
        UtRepl.fillPublications(cfgPublications, structActual, publicationIn, publicationOut);


        // Фильтрация структуры: убирание того, чего нет в публикациях publicationIn и publicationOut
        struct = UtRepl.getStructCommon(structActual, publicationIn, publicationOut);


        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
    }

    // Проверка версии приложения, ошибка при несовпадении
    void checkAppUpdate() throws Exception {
        UtAppUpdate ut = new UtAppUpdate(db, dataRoot);
        ut.checkAppUpdate(false);
    }

    public void addWorkstation(long wsId, String wsName, String wsGuid, String cfgPublicationsFileName, String cfgDecodeFileName) throws Exception {
        log.info("add workstation, wsId: " + wsId + ", name: " + wsName);

        // ---
        // Создадим станцию

        // workstation_list
        Map params = UtCnv.toMap(
                "id", wsId,
                "name", wsName,
                "guid", wsGuid
        );
        String sql = "insert into " + JdxUtils.sys_table_prefix + "workstation_list (id, name, guid) values (:id, :name, :guid)";
        db.execSql(sql, params);

        // state_ws
        JdxDbUtils dbu = new JdxDbUtils(db, null);
        long id = dbu.getNextGenerator(JdxUtils.sys_gen_prefix + "state_ws");
        sql = "insert into " + JdxUtils.sys_table_prefix + "state_ws (id, ws_id, que_common_dispatch_done, que_in_age_done, enabled, mute_age) values (" + id + ", " + wsId + ", 0, 0, 0, 0)";
        db.execSql(sql);


        // ---
        // Инициализационная очередь queOut001
        JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
        queOut001.setDataRoot(dataRoot);

        //Очереди и правила их нумерации, в частности out001
        // ---
        // Отправим системные команды для станции в ее очередь queOut001
        JSONObject cfgPublications = UtRepl.loadAndValidateCfgFile(cfgPublicationsFileName);
        srvSendCfgInternal(queOut001, cfgPublications, UtCfgType.PUBLICATIONS, wsId);
        //
        JSONObject cfgDecode = UtRepl.loadAndValidateCfgFile(cfgDecodeFileName);
        srvSendCfgInternal(queOut001, cfgDecode, UtCfgType.DECODE, wsId);
        //
        srvDbStructFinishInternal(queOut001);


        // ---
        // Подготовим Snapshot для станции

        // Запоминаем возраст входящей очереди.
        JdxStateManagerWs stateManagerWs = new JdxStateManagerWs(db);
        long queInNoDone = stateManagerWs.getQueNoDone("in");

        // Единственное место, где на сервере без экземпляра рабочей станции - не обойтись
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        List<IReplica> snapshotReplicas;

        //
        db.startTran();
        try {
            // В publicationOutTables будет соблюден порядок сортировки таблиц с учетом foreign key
            // (при применении snapsot важен порядок)
            List<IJdxTable> publicationOutTables = makePublicationTables(ws.struct, ws.publicationOut);

            // Создаем реплики
            snapshotReplicas = ws.SnapshotForTables(publicationOutTables, 0, false);

            //
            db.commit();

        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }


        // ---
        // Возраст snapshot рабочей станции.
        long wsSnapshotAge = queInNoDone;

        // Помещаем snapshot-реплику в очередь queOut001
        for (IReplica replica : snapshotReplicas) {
            queOut001.push(replica);
        }

        // Сообщим рабочей станции ее начальный возраст ВХОДЯЩЕЙ очереди
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaQueInNo(wsId, wsSnapshotAge);
        queOut001.push(replica);

        // Отмечаем возраст snapshot рабочей станции.
        JdxStateManagerSrv stateManagerSrv = new JdxStateManagerSrv(db);
        stateManagerSrv.setSnapshotAge(wsId, wsSnapshotAge);

        // Инициализируем возраст отправленных реплик для рабочей станции.
        // Для рабочей станции ее ВХОДЯЩАЯ очередь - это отражение ИСХОДЯЩЕЙ очереди сервера.
        // Если мы начинаем готовить Snapshot в возрасте queInNoDone, то все реплики ПОСЛЕ этого возраста
        // рабочая станция должна будет получить самостоятельно через ее очередь queIn.
        // Поэтому можно взять у "серверной" рабочей станции номер обработанной ВХОДЯЩЕЙ очереди,
        // но пометить НА СЕРВЕРЕ этим возрастом номер ОТПРАВЛЕННЫХ реплик для этой станции.
        stateManagerSrv.setDispatchDoneQueCommon(wsId, wsSnapshotAge);
        // Инициализируем нумерацию реплик в очереди queOut000 этой станции.
        // Для красивой нумерации в queOut000.
        JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
        queOut000.setMaxNo(wsSnapshotAge);
        // Инициализируем нумерацию отправки реплик из очереди queOut000 на этоу станцию.
        IJdxStateManagerMail stateManagerMail = new JdxStateManagerSrvMail(db, wsId, UtQue.QUE_OUT000);
        stateManagerMail.setMailSendDone(wsSnapshotAge);
    }

    private List<IJdxTable> makePublicationTables(IJdxDbStruct struct, IPublicationStorage publicationStorage) {
        List<IJdxTable> publicationTables = new ArrayList<>();
        for (IJdxTable table : struct.getTables()) {
            if (publicationStorage.getPublicationRule(table.getName()) != null) {
                publicationTables.add(table);
            }
        }
        return publicationTables;
    }

    public void enableWorkstation(long wsId) throws Exception {
        log.info("enable workstation, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set enabled = 1 where id = " + wsId;
        db.execSql(sql);
        sql = "update " + JdxUtils.sys_table_prefix + "state set enabled = 1";
        db.execSql(sql);
    }

    public void disableWorkstation(long wsId) throws Exception {
        log.info("disable workstation, wsId: " + wsId);
        //
        String sql = "update " + JdxUtils.sys_table_prefix + "state_ws set enabled = 0 where id = " + wsId;
        db.execSql(sql);
        sql = "update " + JdxUtils.sys_table_prefix + "state set enabled = 0";
        db.execSql(sql);
    }

    /**
     * Сервер: формирование общей очереди.
     * Считывание очередей рабочих станций и формирование общей очереди.
     */
    public void srvHandleCommonQue() throws Exception {
        srvHandleCommonQueInternal(mailerList, queCommon);
    }

    /**
     * Сервер: тиражирование реплик из queCommon
     * Распределение общей очереди по очередям рабочих станций: queCommon -> queOut000
     */
    public void srvReplicasDispatch() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);


        // Все что у нас есть на раздачу
        long commonQueMaxNo = queCommon.getMaxNo();

        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();

            //
            log.info("srvReplicasDispatch (queCommon -> queOut000), to.wsId: " + wsId);

            // Исходящая очередь для станции wsId
            JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
            queOut000.setDataRoot(dataRoot);

            //
            long sendFrom = stateManager.getDispatchDoneQueCommon(wsId) + 1;
            long sendTo = commonQueMaxNo;

            long count = 0;
            for (long no = sendFrom; no <= sendTo; no++) {
                // Берем реплику из queCommon
                IReplica replica = queCommon.get(no);

                // Преобразовываем по фильтрам
                IReplica replicaForWs = prepareReplicaForWs(replica, wsId);

                // Физически переместим реплику
                queOut000.push(replicaForWs);

                // Отметим распределение очередного номера реплики.
                stateManager.setDispatchDoneQueCommon(wsId, no);

                //
                count++;
            }

            //
            if (sendFrom < sendTo) {
                log.info("srvReplicasDispatch (queCommon -> QueOut000) done, wsId: " + wsId + ", queOut001.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
            } else {
                log.info("srvReplicasDispatch (queCommon -> QueOut000) done, wsId: " + wsId + ", queOut001.no: " + sendFrom + ", nothing done");
            }

        }
    }


    /**
     * Рассылка реплик в ящики каждой рабочей станции
     */
    public void srvReplicasSendMail() throws Exception {
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Рассылаем
            try {
                // Рассылаем queOut000 (продукт обработки queCommon) на каждую станцию
                IJdxStateManagerMail stateManagerMail = new JdxStateManagerSrvMail(db, wsId, UtQue.QUE_OUT000);
                JdxQueOut001 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);
                UtMail.sendQueToMail(wsId, queOut000, mailer, "to", stateManagerMail);

                // Рассылаем queOut001 на каждую станцию
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);
                //
                stateManagerMail = new JdxStateManagerSrvMail(db, wsId, UtQue.QUE_OUT001);
                UtMail.sendQueToMail(wsId, queOut001, mailer, "to001", stateManagerMail);

                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                // todo: не переложить отметку ли в sendQueToMail?
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in SrvDispatchReplicas, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
    }

    /**
     * Преобразовываем реплику replica по фильтрам для рабочей станции wsId
     * todo это тупо - вот так копировать и перекладывать файлы из папки в папку???
     */
    private IReplica prepareReplicaForWs(IReplica replica, long wsIdDestination) throws Exception {
        File replicaFile = replica.getFile();

        // Файл должен быть - иначе незачем делать put
        if (replicaFile == null) {
            throw new XError("Invalid replica.file == null");
        }

        // Пока - по тупому, БЕЗ фильтров
        ReplicaFile replicaRes = new ReplicaFile();

        //
        replicaRes.getInfo().assign(replica.getInfo());


        //
        InputStream inputStream = null;
        try {
            // Распакуем XML-файл из Zip-архива
            inputStream = UtRepl.getReplicaInputStream(replica);

            JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

            // Стартуем формирование файла реплики
            UtReplicaWriter replicaWriter = new UtReplicaWriter();
            replicaWriter.replicaFileStart(replica);

            // Начинаем писать файл с данными
            JdxReplicaWriterXml xmlWriter = replicaWriter.replicaWriterStartDocument(replica);

            // Правила публикаций - todo: ГДЕ БЕРЕМ ДЛЯ WsDest?????
            IPublicationStorage publicationOut = new PublicationStorage();

            //
            copyDataWithFilter(publicationOut, replicaReader, xmlWriter);

            // Заканчиваем формирование файла реплики
            replicaWriter.replicaFileClose();

        } finally {
            // Закроем читателя Zip-файла
            if (inputStream != null) {
                inputStream.close();
            }
        }

        //File actualFile = new File(dataRoot + "temp/" + MailerHttp.getFileName(replica.getInfo().getAge()));
        //FileUtils.copyFile(replicaFile, actualFile);
        //replicaRes.setFile(actualFile);

        //
        return replicaRes;
    }

    // ^с отдельный тест на copyDataWithFilter
    private void copyDataWithFilter(IPublicationStorage publicationOut, JdxReplicaReaderXml dataReader, JdxReplicaWriterXml dataWriter) throws Exception {
        String tableName = dataReader.nextTable();

        // Перебираем таблицы
        while (tableName != null) {

            IPublicationRule publicationTable = publicationOut.getPublicationRule(tableName);

            //
            dataWriter.startTable(tableName);

            // Перебираем записи
            long count = 0;
            Map recValues = dataReader.nextRec();
            while (recValues != null) {
                if (useRecord(recValues, publicationTable)) {

                    dataWriter.appendRec();

                    // Тип операции
                    int oprType = (int) recValues.get(JdxUtils.field_opr_type);
                    dataWriter.setOprType(oprType);

                    // Поля
                    for (IJdxField publicationField : publicationTable.getFields()) {
                        String publicationFieldName = publicationField.getName();
                        dataWriter.setRecValue(publicationFieldName, recValues.get(publicationFieldName));
                    }
                }

                //
                recValues = dataReader.nextRec();

                //
                count++;
                if (count % 200 == 0) {
                    log.info("  table: " + tableName + ", " + count);
                }
            }

            //
            log.info("  done: " + tableName + ", total: " + count);

            //
            dataWriter.flush();

            //
            tableName = dataReader.nextTable();
        }
    }

    private boolean useRecord(Map recValues, IPublicationRule publicationTable) {
        return true;
    }

    /**
     * @deprecated Разобраться с репликацией через папку - сейчас полностью сломано
     */
    @Deprecated
    public void srvHandleCommonQueFrom(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, 0);

        // Физически забираем данные
        srvHandleCommonQueInternal(mailerListLocal, queCommon);
    }

    /**
     * @deprecated Разобраться с репликацией через папку - сейчас полностью сломано
     */
    @Deprecated
    public void srvDispatchReplicasToDir(String cfgFileName, String mailDir, SendRequiredInfo requiredInfo, long destinationWsId, boolean doMarkDone) throws Exception {
        // Готовим локальных курьеров (через папку)
        Map<Long, IMailer> mailerListLocal = new HashMap<>();
        fillMailerListLocal(mailerListLocal, cfgFileName, mailDir, destinationWsId);

        // Физически отправляем данные
        srvDispatchReplicas(queCommon, mailerListLocal, requiredInfo, doMarkDone);
    }


    public void srvSetWsMute(long destinationWsId) throws Exception {
        log.info("srvSetWs MUTE, destination.WsId: " + destinationWsId);

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(destinationWsId);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvSetWsUnmute(long destinationWsId) throws Exception {
        log.info("srvSetWs UNMUTE, destination.WsId: " + destinationWsId);

        // Системная команда "UNMUTE" ...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaUnmute(destinationWsId);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvDbStructStart() throws Exception {
        log.info("srvDbStructStart");

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(0);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvAppUpdate(String exeFileName) throws Exception {
        log.info("srvAppUpdate, exeFileName: " + exeFileName);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

        // Системная команда - в исходящую очередь реплик
        queCommon.push(replica);
    }


    public void srvDbStructFinish() throws Exception {
        log.info("srvDbStructFinish");

        // Системные команды в общую исходящую очередь реплик
        srvDbStructFinishInternal(queCommon);
    }


    /**
     * Системные команды в очередь que
     */
    private void srvDbStructFinishInternal(IJdxReplicaQue que) throws Exception {
        IReplica replica;
        UtRepl utRepl = new UtRepl(db, struct);


        // Системная команда "SET_DB_STRUCT"...
        replica = utRepl.createReplicaSetDbStruct();
        // ...в очередь
        que.push(replica);


        // Системная команда "UNMUTE" ...
        replica = utRepl.createReplicaUnmute(0);
        // ...в очередь
        que.push(replica);
    }


    public void srvSendCfg(String cfgFileName, String cfgType, long destinationWsId) throws Exception {
        log.info("srvSendCfg, cfgFileName: " + new File(cfgFileName).getAbsolutePath() + ", cfgType: " + cfgType + ", destination wsId: " + destinationWsId);

        //
        JSONObject cfg = UtRepl.loadAndValidateCfgFile(cfgFileName);
        srvSendCfgInternal(queCommon, cfg, cfgType, destinationWsId);
    }

    private void srvSendCfgInternal(IJdxReplicaQue que, JSONObject cfg, String cfgType, long destinationWsId) throws Exception {
        //
        db.startTran();
        try {
            // Обновляем конфиг в таблицах для рабочих станций (workstation_list)
            UtCfgMarker utCfgMarker = new UtCfgMarker(db);
            utCfgMarker.setWsCfg(cfg, cfgType, destinationWsId);

            // Системная команда ...
            UtRepl utRepl = new UtRepl(db, struct);
            IReplica replica = utRepl.createReplicaSetCfg(cfg, cfgType, destinationWsId);

            // ... в исходящую очередь реплик
            que.push(replica);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback();
            //
            e.printStackTrace();
            //
            throw e;
        }
    }


    /**
     * Сервер: считывание очередей рабочих станций и формирование общей очереди
     * <p>
     * Из очереди личных реплик и очередей, входящих от других рабочих станций, формирует единую очередь.
     * Единая очередь используется как входящая для применения аудита на сервере и как основа для тиражирование реплик подписчикам.
     */
    private void srvHandleCommonQueInternal(Map<Long, IMailer> mailerList, IJdxReplicaQue commonQue) throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Обрабатываем каждую станцию
            try {
                log.info("srvHandleCommonQue, from.wsId: " + wsId);

                //
                long queDoneAge = stateManager.getWsQueInAgeDone(wsId);
                long queMaxAge = mailer.getBoxState("from");

                //
                long count = 0;
                for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                    log.info("receive, wsId: " + wsId + ", receiving.age: " + age);

                    // Информацмия о реплике с почтового сервера
                    ReplicaInfo info = mailer.getReplicaInfo("from", age);

                    // Физически забираем данные с почтового сервера
                    IReplica replica = mailer.receive("from", age);

                    // Проверяем целостность скачанного
                    JdxUtils.checkReplicaCrc(replica, info);

                    // Читаем заголовок
                    JdxReplicaReaderXml.readReplicaInfo(replica);

                    // Помещаем полученные данные в общую очередь
                    db.startTran();
                    try {
                        // Помещаем в очередь
                        long commonQueAge = commonQue.push(replica);

                        // Отмечаем факт скачивания
                        stateManager.setWsQueInAgeDone(wsId, age);

                        // todo: Почему для сервера - сразу ТУТ реагируем, а для станции - потом. И почему ТУТ не проверяется адресат????
                        // Реагируем на системные реплики-сообщения
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.MUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setMuteDone(wsId, commonQueAge);
                        }
                        //
                        if (replica.getInfo().getReplicaType() == JdxReplicaType.UNMUTE_DONE) {
                            JdxMuteManagerSrv utmm = new JdxMuteManagerSrv(db);
                            utmm.setUnmuteDone(wsId);
                        }

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback();
                        throw e;
                    }

                    // Удаляем с почтового сервера
                    mailer.delete("from", age);

                    //
                    count++;
                }


                // Отметить попытку чтения (для отслеживания активности станции, когда нет данных для реальной передачи)
                mailer.setData(null, "ping.read", "from");
                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);


                //
                if (queDoneAge <= queMaxAge) {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + " .. " + queMaxAge + ", done count: " + count);
                } else {
                    log.info("srvHandleCommonQue, from.wsId: " + wsId + ", que.age: " + queDoneAge + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in srvHandleCommonQue, from.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }
        }
    }

    /**
     * Сервер: распределение очереди по рабочим станциям
     *
     * @deprecated Нужно только для репликации через папку - сейчас полностью сломано
     */
    @Deprecated
    private void srvDispatchReplicas(IJdxQue que, Map<Long, IMailer> mailerList, SendRequiredInfo requiredInfo, boolean doMarkDone) throws Exception {
/*
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // Все что у нас есть на раздачу
        long commonQueMaxNo = que.getMaxNo();

        //
        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();
            IMailer mailer = (IMailer) en.getValue();

            // Рассылаем на каждую станцию
            try {
                log.info("SrvDispatchReplicas, to.wsId: " + wsId);

                //
                long sendFrom;
                long sendTo;
                long count;

                // ---
                // queOut001 - очередь Que001

                // Сначала проверим, надо ли отправить queOut001
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);

                //
                sendFrom = stateManager.getDispatchDoneQueOut001(wsId) + 1;
                sendTo = queOut001.getMaxNoFromDir(); // todo: очень некрасиво - путаюися "физический" (getMaxNo) и "логический" (getMaxNoFromDir) возраст

                // Берем реплику - snapshot
                count = 0;
                for (long no = sendFrom; no <= sendTo; no++) {
                    IReplica replica001 = queOut001.get(no);

                    // Физически отправим реплику - snapshot
                    mailer.send(replica001, "to001", no);

                    // Отметим отправку
                    if (doMarkDone) {
                        // Отметим отправку очередного номера реплики.
                        stateManager.setDispatchDoneQueOut001(wsId, no);
                    }

                    //
                    count = count + 1;
                }

                //
                if (sendFrom < sendTo) {
                    log.info("Que001 DispatchReplicas done, wsId: " + wsId + ", queOut001.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
                } else {
                    log.info("Que001 DispatchReplicas done, wsId: " + wsId + ", queOut001.no: " + sendFrom + ", nothing done");
                }


                // ---
                // queCommon - общая очередь

                // Выясняем объем передачи
                // Если никто не просит - узнаем сколько просит станция
                SendRequiredInfo requiredInfoBox;
                if (requiredInfo == null) {
                    requiredInfoBox = mailer.getSendRequired("to");
                } else {
                    requiredInfoBox = requiredInfo;
                }

                //
                if (requiredInfoBox.requiredFrom != -1) {
                    // Попросили повторную отправку
                    log.warn("Repeat send required, from: " + requiredInfoBox.requiredFrom + ", to: " + requiredInfoBox.requiredTo + ", recreate: " + requiredInfoBox.recreate);
                    sendFrom = requiredInfoBox.requiredFrom;
                    sendTo = requiredInfoBox.requiredTo;
                } else {
                    // Не просили - зададим сами (от последней отправленной до послейдней, что у нас есть на раздачу)
                    sendFrom = stateManager.getDispatchDoneQueCommon(wsId) + 1;
                    sendTo = commonQueMaxNo;
                }

                //
                count = 0;
                for (long no = sendFrom; no <= sendTo; no++) {
                    // Берем реплику
                    IReplica replica = que.get(no);

                    //
                    //log.debug("replica.age: " + replica.getInfo().getAge() + ", replica.wsId: " + replica.getInfo().getWsId());

                    // Физически отправим реплику
                    mailer.send(replica, "to", no); // todo это тупо - вот так копировать и перекладывать файлы из папки в папку???

                    // Отметим отправку очередного номера реплики.
                    if (doMarkDone) {
                        stateManager.setMailSendDone(wsId, no);
                        stateManager.setDispatchDoneQueCommon(wsId, no);
                    }

                    //
                    count++;
                }


                // Отметить попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
                mailer.setData(null, "ping.write", "to");
                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);


                // Снимем флаг просьбы сервера
                if (requiredInfoBox.requiredFrom != -1) {
                    mailer.setSendRequired("to", new SendRequiredInfo());
                    log.warn("Repeat send done");
                }

                //
                if (sendFrom < sendTo) {
                    log.info("QueCommon DispatchReplicas, to.wsId: " + wsId + ", queCommon.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
                } else {
                    log.info("QueCommon DispatchReplicas, to.wsId: " + wsId + ", queCommon.no: " + sendFrom + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                log.error("Error in SrvDispatchReplicas, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
*/
    }

    private Map getInfoSrv() {
        return null;
    }


    /**
     * Готовим спосок локальных (через папку) мейлеров, отдельные для каждой станции
     */
    private void fillMailerListLocal(Map<Long, IMailer> mailerListLocal, String cfgFileName, String mailDir, long destinationWsId) throws Exception {
        // Готовим курьеров
        mailDir = UtFile.unnormPath(mailDir) + "/";

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

        // Список активных рабочих станций
        DataStore st = loadWsList(destinationWsId);

        //
        for (DataRecord rec : st) {
            long wdId = rec.getValueLong("id");
            String guid = rec.getValueString("guid");
            String guidPath = guid.replace("-", "/");

            // Конфиг для мейлера
            JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wdId));
            cfgWs.put("mailRemoteDir", mailDir + guidPath);

            // Мейлер
            IMailer mailerLocal = new MailerLocalFiles();
            mailerLocal.init(cfgWs);

            //
            mailerListLocal.put(wdId, mailerLocal);
        }
    }

    /**
     * Список активных рабочих станций (или одна конкретная)
     */
    private DataStore loadWsList(long wsId) throws Exception {
        String sql;
        if (wsId != 0) {
            // Указана конкретная станция-получатель - выгружаем только ее, остальные пропускаем
            sql = "select * from " + JdxUtils.sys_table_prefix + "workstation_list where id = " + wsId;
        } else {
            // Берем только активные
            sql = "select " + JdxUtils.sys_table_prefix + "workstation_list.* " +
                    "from " + JdxUtils.sys_table_prefix + "workstation_list " +
                    "join " + JdxUtils.sys_table_prefix + "state_ws on " +
                    "(" + JdxUtils.sys_table_prefix + "workstation_list.id = " + JdxUtils.sys_table_prefix + "state_ws.ws_id) " +
                    "where " + JdxUtils.sys_table_prefix + "state_ws.enabled = 1";
        }

        //
        DataStore st = db.loadSql(sql);

        //
        return st;
    }


}


