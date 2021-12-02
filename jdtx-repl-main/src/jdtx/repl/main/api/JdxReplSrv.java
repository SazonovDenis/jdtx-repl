package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.filter.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.rec_merge.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.task.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;


/**
 * Контекст сервера
 * todo Разобраться с репликацией через папку - сейчас полностью сломано
 * todo рассмотреть необходимость добавлять каскадно и ВЛИЯЮШИЕ записл для вставляемой эталонной (и далее - каскадно) - их ТОЖЕ может не оказаться на филиале
 */
public class JdxReplSrv {

    public static long SERVER_WS_ID = 1L;

    // Общая очередь на сервере
    IJdxQue queCommon;

    // Почтовые ящики для чтения/отправки сообщений (для каждой рабочей станции)
    Map<Long, IMailer> mailerList;

    // Правила публикации (для каждой рабочей станции)
    Map<Long, IPublicationRuleStorage> publicationsInList;

    //
    Db db;
    private IJdxDbStruct struct;

    //
    private String dataRoot;

    //
    public JdxErrorCollector errorCollector = null;

    //
    protected static Log log = LogFactory.getLog("jdtx.Server");

    //
    public JdxReplSrv(Db db) throws Exception {
        this.db = db;

        // Общая очередь на сервере
        queCommon = new JdxQueCommon(db, UtQue.QUE_COMMON, UtQue.STATE_AT_SRV);

        // Почтовые курьеры для чтения/отправки сообщений (для каждой рабочей станции)
        mailerList = new HashMap<>();

        // Правила публикации (для каждой рабочей станции)
        publicationsInList = new HashMap<>();
    }

    public IMailer getSelfMailer() {
        // Ошибки сервера кладем в ящик рабочей станции №1
        long wsId = SERVER_WS_ID;
        return mailerList.get(wsId);
    }


    /**
     * Проверка версии приложения, ошибка при несовпадении.
     * <p>
     * Рабочая станция вседа обновляет приложение, а сервер - просто ждет пока приложение обновится.
     * Это разделение для того, чтобы на серверной базе
     * сервер и рабчая станция одновременно не кинулись обновлять.
     */
    public void checkAppUpdate() throws Exception {
        String appRoot = new File(db.getApp().getRt().getChild("app").getValueString("appRoot")).getCanonicalPath();
        UtAppUpdate ut = new UtAppUpdate(db, appRoot);
        ut.checkAppUpdate(false);
    }

    /**
     * Сервер, задачи по уходу за сервером
     */
    public void srvHandleRoutineTask() throws Exception {
        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // общая очередь
        UtRepl.clearTrashFiles((JdxQue) queCommon);


        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // очередь Out000 для станции wsId (исходящая из сервера)
        DataStore wsSt = loadWsList();
        for (DataRecord wsRec : wsSt) {
            long wsId = wsRec.getValueLong("id");

            // Исходящая очередь Out000 для станции wsId
            JdxQueOut000 que = new JdxQueOut000(db, wsId);
            que.setDataRoot(dataRoot);

            //
            UtRepl.clearTrashFiles(que);
        }


        // Очистка файлов, котрорые есть в каталоге, но которых нет в базе:
        // очередь queOut001 для станции wsId (инициализационная или для системных команд)
        for (DataRecord wsRec : wsSt) {
            long wsId = wsRec.getValueLong("id");

            //
            JdxQueOut001 que = new JdxQueOut001(db, wsId);
            que.setDataRoot(dataRoot);

            //
            UtRepl.clearTrashFiles(que);
        }
    }


    /**
     * Сервер, запуск
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
        CfgManager cfgManager = new CfgManager(db);
        JSONObject cfgWs = cfgManager.getSelfCfg(CfgType.WS);
        JSONObject cfgPublications = cfgManager.getSelfCfg(CfgType.PUBLICATIONS);


        // Чтение структуры БД
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct structActual = reader.readDbStruct();


        // Читаем код нашей станции
        DataRecord rec = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "WS_INFO").getCurRec();
        // Проверяем код нашей станции
        long selfWsId = rec.getValueLong("ws_id");
        if (selfWsId != SERVER_WS_ID) {
            throw new XError("Invalid server ws_id: " + selfWsId);
        }

        // Общая очередь
        String queCommon_DirLocal = dataRoot + "srv/que_common/";
        queCommon.setDataRoot(queCommon_DirLocal);

        // Почтовые курьеры, правила входящих реплик - отдельные для каждой станции
        DataStore wsSt = loadWsList();
        for (DataRecord wsRec : wsSt) {
            long wsId = wsRec.getValueLong("id");

            // Рабочие каталоги мейлера
            String sWsId = UtString.padLeft(String.valueOf(wsId), 3, "0");
            String mailLocalDirTmp = dataRoot + "srv/ws_" + sWsId + "_tmp/";

            // Конфиг для мейлера
            JSONObject cfgMailer = new JSONObject();
            String guid = wsRec.getValueString("guid");
            String url = (String) cfgWs.get("url");
            cfgMailer.put("guid", guid);
            cfgMailer.put("url", url);
            cfgMailer.put("localDirTmp", mailLocalDirTmp);

            // Мейлер
            IMailer mailer = new MailerHttp();
            mailer.init(cfgMailer);
            //
            mailerList.put(wsId, mailer);


            // Правила входящих реплик для рабочей станции ("in", используем при подготовке реплик)
            JSONObject cfgPublicationsWs = CfgManager.getCfgFromDataRecord(wsRec, CfgType.PUBLICATIONS);
            IPublicationRuleStorage publicationRuleWsIn = PublicationRuleStorage.loadRules(cfgPublicationsWs, structActual, "in");
            publicationsInList.put(wsId, publicationRuleWsIn);
        }


        // Фильтрация структуры: убирание того, чего нет ни в одном из правил публикаций publicationIn и publicationOut

        // Правила публикаций
        IPublicationRuleStorage publicationIn = PublicationRuleStorage.loadRules(cfgPublications, structActual, "in");
        IPublicationRuleStorage publicationOut = PublicationRuleStorage.loadRules(cfgPublications, structActual, "out");

        // Фильтрация структуры
        struct = UtRepl.getStructCommon(structActual, publicationIn, publicationOut);

        // Чтобы были
        UtFile.mkdirs(dataRoot + "temp");
    }

    /**
     * Сервер, инициализация окружения
     */
    public void initFirst() {
        UtFile.mkdirs(queCommon.getBaseDir());
    }


    // todo: Создание workstation идет вне транзакции - это плохо, бывали случаи прерывания
    public void addWorkstation(long wsId, String wsName, String wsGuid, String cfgPublicationsFileName, String cfgDecodeFileName) throws Exception {
        log.info("add workstation, wsId: " + wsId + ", name: " + wsName);

        // ---
        // Создадим станцию

        // SRV_WORKSTATION_LIST
        Map params = UtCnv.toMap(
                "id", wsId,
                "name", wsName,
                "guid", wsGuid
        );
        String sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST (id, name, guid) values (:id, :name, :guid)";
        db.execSql(sql, params);

        // SRV_WORKSTATION_STATE
        sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE (id, ws_id, que_common_dispatch_done, que_in_no_done, enabled, mute_age) values (" + wsId + ", " + wsId + ", 0, 0, 0, 0)";
        db.execSql(sql);


        // ---
        // Очередь queOut001 станции (инициализационная или для системных команд)
        JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
        queOut001.setDataRoot(dataRoot);

        // ---
        // Настройки для станции отметим у себя и отправим на станцию.
        // Берем сейчас из файла, а не из publicationsInList.get(wsId), т.к. новой станции еще в этом списке нет
        JSONObject cfgPublicationsWs = UtRepl.loadAndValidateJsonFile(cfgPublicationsFileName);
        JSONObject cfgDecode = UtRepl.loadAndValidateJsonFile(cfgDecodeFileName);
        srvSetAndSendCfg(queOut001, wsId, cfgPublicationsWs, cfgDecode, true);


        // ---
        // Подготовим snapshot для станции wsId

        // Правила публикаций (фильтры) для подготовки snapshot для wsId.
        // В качестве фильтров на ИНИЦИАЛИЗАЦИОННЫЙ snapshot от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
        IPublicationRuleStorage publicationRuleWsIn = PublicationRuleStorage.loadRules(cfgPublicationsWs, struct, "in");

        // Подготовим snapshot-реплику.
        // Получатель snapshot-реплики - wsId.
        // Автор snapshot-реплики - при подготовке snapshot автор, строго говоря, не определен,
        // но чтобы не было ошибок, поставим в качестве автора - себя (serverWsId).
        long serverWsId = SERVER_WS_ID;
        long wsSnapshotAge = sendReplicasSnapshot(serverWsId, wsId, publicationRuleWsIn, queOut001);


        // ---
        // Команды на установку возраста очередей рабочей станции (начальное состояние)
        JdxWsState wsState = new JdxWsState();
        // Возраст очереди "in" новой рабочей станции - по возрасту очереди "in"
        // "серверной" рабочей станции (que_in_no_done), запомненному при формировании snapshot
        wsState.QUE_IN_NO = wsSnapshotAge;
        wsState.QUE_IN_NO_DONE = wsSnapshotAge;
        //
        wsState.QUE_IN001_NO = 0L;
        wsState.QUE_IN001_NO_DONE = 0L;
        // Новая рабочая станция пока не имела аудита, поэтому возраст ее очереди "out" будет 0, ...
        wsState.QUE_OUT_NO = 0L;
        wsState.QUE_OUT_NO_DONE = 0L;
        // ... отправка тоже ...
        wsState.MAIL_SEND_DONE = 0L;
        // ... и возраст аудита тоже
        wsState.AGE = 0L;
        // Поехали
        wsState.MUTE = 0L;
        //
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replicaSetState = utRepl.createReplicaSetWsState(wsId, wsState);
        queOut001.push(replicaSetState);


        // ---
        // Установка (на сервере) разных возрастов для новой рабочей станции (начальное состояние)
        JdxStateManagerSrv stateManagerSrv = new JdxStateManagerSrv(db);

        // Номер отправленных реплик.
        // Более поздние в snapshot НЕ попали и рабочая станция получит их самостоятельно.
        stateManagerSrv.setDispatchDoneQueCommon(wsId, wsSnapshotAge);

        // Номер реплик в очереди queOut000 новой станции будет совпадать с остальными станциями (так удобнее и правильнее).
        JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
        queOut000.setMaxNo(wsSnapshotAge);

        // Нумерация отправки реплик из очереди queOut000 на эту станцию.
        IJdxMailStateManager mailStateManager = new JdxMailStateManagerSrv(db, wsId, UtQue.QUE_OUT000);
        mailStateManager.setMailSendDone(wsSnapshotAge);

        // Номер последней реплики ОТ новой рабочей станции
        // Станция пока не имела аудита, поэтому ничего еще не отправила на сервер, поэтому 0.
        stateManagerSrv.setWsQueInNoDone(wsId, 0);
    }

    /**
     * Восстановление рабочей станции при потере базы.
     * Отправляется snapshot, инициализируются счетчики очередей.
     *
     * @param wsId код ранее существующей рабочей станции
     */
    public void restoreWorkstation(long wsId, String cfgSnapshotFileName) throws Exception {
        log.info("Restore workstation, wsId: " + wsId);


        // todo: static RefDecodeStrategy instance - ваще капец!
        // Именно за этим тут и нужна ИНИЦИАЛИЗАЦИЯ ws.init, и больше ни для чего!!!
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();


        //
        UtRepl utRepl = new UtRepl(db, struct);

        // ---
        // Очередь queOut001 станции (инициализационная или для системных команд)
        JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
        queOut001.setDataRoot(dataRoot);

        // Что станция успела получить в прошлой жизни?
        long ageQueOut001Ws = queOut001.getMaxNo();

        // Очередь queOut станции (что станция успела нам отправить в прошлой жизни?)
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);
        long wsQueOutNo = stateManager.getWsQueInNoDone(wsId);

        // ---
        // Настройки для станции отметим у себя и отправим на станцию
        DataRecord wsRec = loadWs(wsId);
        JSONObject cfgPublicationsWs = CfgManager.getCfgFromDataRecord(wsRec, CfgType.PUBLICATIONS);
        JSONObject cfgDecode = CfgManager.getCfgFromDataRecord(wsRec, CfgType.DECODE);
        srvSetAndSendCfg(queOut001, wsId, cfgPublicationsWs, cfgDecode, false);


        // ---
        // Подготовим snapshot для станции wsId

        // Правила публикаций (фильтры) для подготовки snapshot для wsId
        JSONObject cfgSnapsot = UtRepl.loadAndValidateJsonFile(cfgSnapshotFileName);
        IPublicationRuleStorage ruleSnapsot = PublicationRuleStorage.loadRules(cfgSnapsot, struct, "snapshot");

        // Подготовим snapshot-реплику.
        // Получатель snapshot-реплики - wsId.
        // Автор snapshot-реплики - при подготовке snapshot автор, строго говоря, не определен,
        // но чтобы не было ошибок, поставим в качестве автора - себя (serverWsId).
        long serverWsId = SERVER_WS_ID;
        long wsSnapshotAge = sendReplicasSnapshot(serverWsId, wsId, ruleSnapsot, queOut001);


        // ---
        // Отправим команду "починить генераторы".
        // После применения snaphot генераторы рабочей станции будут находятся в устаревшем сосоянии.
        IReplica replicaRepairGenerators = utRepl.createReplicaRepairGenerators(wsId);
        queOut001.push(replicaRepairGenerators);


        // ---
        // Установка возрастов очередей рабочей станции (начальное состояние)

        // Самым наглым образом установим номер реплики, разрешенной к отправке, на почтовом сервере.
        // Это нужно, чтобы станция вышла из состояния "Detected restore from backup, repair needed" и получила нашу первую реплику.
        // В этой первой реплике возраст QUE_OUT_NO и QUE_OUT_NO_DONE будет возвращен на место, вместе с остальными возрастами.
        IMailer mailerWs = mailerList.get(wsId);
        SendRequiredInfo requiredInfo = new SendRequiredInfo();
        requiredInfo.requiredFrom = 1;
        requiredInfo.requiredTo = 1;
        mailerWs.setSendRequired("to001", requiredInfo);


        // Реплика на установку возрастов
        JdxWsState wsState = new JdxWsState();

        // Возраст очереди "in" новой рабочей станции - по возрасту очереди "in"
        // "серверной" рабочей станции (que_in_no_done), запомненному при формировании snapshot
        wsState.QUE_IN_NO = wsSnapshotAge;
        wsState.QUE_IN_NO_DONE = wsSnapshotAge;
        //
        wsState.QUE_IN001_NO = ageQueOut001Ws;
        wsState.QUE_IN001_NO_DONE = ageQueOut001Ws;
        //
        wsState.QUE_OUT_NO = wsQueOutNo;
        // ... отправка тоже ...
        wsState.MAIL_SEND_DONE = wsQueOutNo;
        // Возраст аудита станции из ее прошлой жизни
        long wsQueOutAge = ((JdxQueCommon) queCommon).getMaxAgeForWs(wsId);
        wsState.AGE = wsQueOutAge;
        wsState.QUE_OUT_NO_DONE = wsQueOutAge;
        // Поехали
        wsState.MUTE = 0L;

        // Реплика на установку возрастов - отправка.
        // Самым наглым образом, минуя все очереди, тупо под номером 1
        IReplica replicaSetState = utRepl.createReplicaSetWsState(wsId, wsState);
        //
        mailerWs.send(replicaSetState, "to001", 1);


        // Установим номер реплики, разрешенной к отправке, на почтовом сервере.
        // На этот раз это номер, соответсвующий имеющимся репликам в out001.
        requiredInfo.requiredFrom = ageQueOut001Ws + 1;
        requiredInfo.requiredTo = -1;
        mailerWs.setSendRequired("to001", requiredInfo);


        // ---
        // Установка (на сервере) разных возрастов для новой рабочей станции (начальное состояние)
        // Не требуется - станция сохраняет свое состояние

/**
 1) чтение и отправка текущего состяния ("восстановительная" реплика)

 1.1) проработать четкий момент "поехали", когда станция начинает не только читать,
 но и обрабатывать свой аудит и отправлять его - к этому моменту все очереди рабочей станции должны быть заданы правильно - чтобы на станции можно было безопасно вводит данные в любое время, пока идет репликация

 2) прием на станции - сейчас состояние "Detected restore from backup, repair needed"

 3) учесть, получение команды на восстановление на живой базе - проверять на базе рабочей станции, чтобы все номера (age, очередей) двигали станцию ТОЛьКО вперед (или вообще только из нулевого состояния), иначе ошибка

 */
    }

    private void srvSetAndSendCfg(JdxQueOut001 queOut001, long wsId, JSONObject cfgPublicationsWs, JSONObject cfgDecode, boolean sendSnapshot) throws Exception {
        // ---
        // Отправим настройки для станции
        srvSetAndSendCfgInternal(queOut001, cfgPublicationsWs, CfgType.PUBLICATIONS, wsId);
        //
        srvSetAndSendCfgInternal(queOut001, cfgDecode, CfgType.DECODE, wsId);
        //
        srvDbStructFinishInternal(queOut001, sendSnapshot);
    }

    /**
     * Подготовим snapshot для станции wsIdDestination,
     * фильтруем его по правилам rulesForSnapshot,
     * и отправляем в очередь que
     */
    private long sendReplicasSnapshot(long wsIdAuthor, long wsIdDestination, IPublicationRuleStorage rulesForSnapshot, IJdxQue que) throws Exception {
        // Запоминаем возраст входящей очереди "серверной" рабочей станции (que_in_no_done).
        // Если мы начинаем готовить snapshot в этом возрасте, то все реплики ДО этого возраста войдут в snapshot,
        // а все реплики ПОСЛЕ этого возраста в snapshot НЕ попадут,
        // и рабочая станция должна будет получить их самостоятельно, через свою очередь queIn.
        // Поэтому можно взять у "серверной" рабочей станции номер обработанной входящей очереди
        // и считать его возрастом обработанной входящей очереди на новой рабочей станции.
        JdxStateManagerWs stateManagerWs = new JdxStateManagerWs(db);
        long wsSnapshotAge = stateManagerWs.getQueNoDone("in");

        // Передаем тот состав таблиц, который перечислен в правилах исходящих публикаций
        List<IJdxTable> publicationOutTables = makeOrderedFromPublicationRules(struct, rulesForSnapshot);

        // Создаем snapshot-реплики, фильтруем, отправляем
        UtRepl ut = new UtRepl(db, struct);
        ut.createSendSnapshotForTables(publicationOutTables, wsIdAuthor, wsIdDestination, rulesForSnapshot, false, que);

        //
        return wsSnapshotAge;
    }

    /**
     * Возвращает список таблиц из publicationStorage, но том порядке,
     * в котором они расположены в описании структуры struct - там список таблиц отсортирован по зависимостям.
     */
    private List<IJdxTable> makeOrderedFromPublicationRules(IJdxDbStruct struct, IPublicationRuleStorage publicationStorage) {
        List<IJdxTable> tablesOrdered = new ArrayList<>();
        //
        for (IJdxTable tableOrderedSample : struct.getTables()) {
            if (publicationStorage.getPublicationRule(tableOrderedSample.getName()) != null) {
                tablesOrdered.add(tableOrderedSample);
            }
        }
        //
        return tablesOrdered;
    }

    public void enableWorkstation(long wsId) throws Exception {
        log.info("enable workstation, wsId: " + wsId);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set enabled = 1 where id = " + wsId;
        db.execSql(sql);
    }

    public void disableWorkstation(long wsId) throws Exception {
        log.info("disable workstation, wsId: " + wsId);
        //
        String sql = "update " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE set enabled = 0 where id = " + wsId;
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
     * Сервер: тиражирование реплик из common.
     * Распределение общей очереди по очередям рабочих станций: common -> out000
     */
    public void srvReplicasDispatch() throws Exception {
        JdxStateManagerSrv stateManager = new JdxStateManagerSrv(db);

        // Все что у нас есть на раздачу
        long commonQueMaxNo = queCommon.getMaxNo();

        for (Map.Entry en : mailerList.entrySet()) {
            long wsId = (long) en.getKey();

            //
            log.info("srvReplicasDispatch, to.wsId: " + wsId);

            try {

                // Исходящая очередь для станции wsId
                JdxQueOut000 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);

                // Преобразователь по фильтрам
                IReplicaFilter filter = new ReplicaFilter();

                // Правила публикаций (фильтры) для станции wsId.
                // В качестве фильтров на ОТПРАВКУ от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
                IPublicationRuleStorage publicationRule = publicationsInList.get(wsId);

                // Параметры (для правил публикации): получатель реплики (для правил публикации)
                filter.getFilterParams().put("wsDestination", String.valueOf(wsId));

                //
                long sendFrom = stateManager.getDispatchDoneQueCommon(wsId) + 1;
                long sendTo = commonQueMaxNo;

                long countToDo = sendTo - sendFrom + 1;
                long count = 0;
                for (long no = sendFrom; no <= sendTo; no++) {
                    log.info("srvReplicasDispatch, to.wsId: " + wsId + ", no: " + no + ", " + count + "/" + countToDo);

                    // Берем реплику из queCommon
                    IReplica replica = queCommon.get(no);

                    // Читаем заголовок
                    JdxReplicaReaderXml.readReplicaInfo(replica);

                    // Параметры (для правил публикации): автор реплики
                    filter.getFilterParams().put("wsAuthor", String.valueOf(replica.getInfo().getWsId()));

                    // Преобразовываем по правилам публикаций (фильтрам)
                    IReplica replicaForWs = filter.convertReplicaForWs(replica, publicationRule);

                    //
                    db.startTran();
                    try {
                        // Положим реплику в очередь (физически переместим)
                        queOut000.push(replicaForWs);

                        // Отметим распределение очередного номера реплики.
                        stateManager.setDispatchDoneQueCommon(wsId, no);

                        //
                        db.commit();
                    } catch (Exception e) {
                        db.rollback(e);
                        throw e;
                    }

                    //
                    count++;
                }

                //
                if (count > 0) {
                    log.info("srvReplicasDispatch done, to.wsId: " + wsId + ", out001.no: " + sendFrom + " .. " + sendTo + ", done count: " + count);
                } else {
                    log.info("srvReplicasDispatch done, to.wsId: " + wsId + ", out001.no: " + sendFrom + ", nothing done");
                }

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvReplicasDispatch, to.wsId: " + wsId, e);
                //
                //
                log.error("Error in srvReplicasDispatch, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
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
                // Рассылаем очередь out000 (продукт обработки очереди common -> out000) на каждую станцию
                IJdxMailStateManager mailStateManager = new JdxMailStateManagerSrv(db, wsId, UtQue.QUE_OUT000);
                JdxQueOut001 queOut000 = new JdxQueOut000(db, wsId);
                queOut000.setDataRoot(dataRoot);
                UtMail.sendQueToMail(wsId, queOut000, mailer, "to", mailStateManager);

                // Рассылаем очередь queOut001 на каждую станцию
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);
                //
                mailStateManager = new JdxMailStateManagerSrv(db, wsId, UtQue.QUE_OUT001);
                UtMail.sendQueToMail(wsId, queOut001, mailer, "to001", mailStateManager);

                // Отметить состояние сервера, данные сервера (сервер отчитывается о себе для отслеживания активности сервера)
                // todo: не переложить отметку ли в sendQueToMail?
                Map info = getInfoSrv();
                mailer.setData(info, "srv.info", null);

            } catch (Exception e) {
                // Ошибка для станции - пропускаем, идем дальше
                errorCollector.collectError("srvReplicasSendMail, to.wsId: " + wsId, e);
                //
                log.error("Error in srvReplicasSendMail, to.wsId: " + wsId + ", error: " + Ut.getExceptionMessage(e));
                log.error(Ut.getStackTrace(e));
            }

        }
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


    public void srvMuteAll() throws Exception {
        log.info("srvMuteAll");

        // Системная команда "MUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaMute(0);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public void srvUnmuteAll() throws Exception {
        log.info("srvUnmuteAll");

        // Системная команда "UNMUTE"...
        UtRepl utRepl = new UtRepl(db, struct);
        IReplica replica = utRepl.createReplicaUnmute(0);

        // ... в исходящую (общую) очередь реплик
        queCommon.push(replica);
    }


    public long srvMuteState(boolean doWaitMute, boolean doWaitUnmute, long muteAgeWait) throws Exception {
        if (doWaitMute && doWaitUnmute) {
            throw new XError("doWaitMute && doWaitUnmute");
        }

        while (true) {
            // DataStore stDisplay = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE where enabled = 1");
            // UtData.outTable(stDisplay);
            DataStore stDisplay = db.loadSql("select WS_ID, QUE_IN_NO_DONE, MUTE_AGE, '' as MUTE_STATE from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE where enabled = 1");

            // Вычисление состояния
            long count_total = stDisplay.size();
            long countMute = 0;
            long countUnmute = 0;
            long countWaitAge = 0;
            long muteAgeMax = 0;
            long muteAgeMin = Long.MAX_VALUE;
            for (DataRecord recDisplay : stDisplay) {
                long muteAgeWs = recDisplay.getValueLong("mute_age");
                if (muteAgeWs > 0) {
                    countMute = countMute + 1;
                } else {
                    countUnmute = countUnmute + 1;
                }
                if (muteAgeWs >= muteAgeWait) {
                    countWaitAge = countWaitAge + 1;
                }
                if (muteAgeMin > muteAgeWs) {
                    muteAgeMin = muteAgeWs;
                }
                if (muteAgeMax < muteAgeWs) {
                    muteAgeMax = muteAgeWs;
                }
                //
                if (doWaitUnmute) {
                    if (muteAgeWs != 0) {
                        recDisplay.setValue("mute_state", "Wait unmute");
                    } else {
                        recDisplay.setValue("mute_state", "Unmuted ok");
                    }
                } else if (doWaitMute) {
                    if (muteAgeWait == 0) {
                        if (muteAgeWs == 0) {
                            recDisplay.setValue("mute_state", "Wait mute");
                        } else {
                            recDisplay.setValue("mute_state", "Muted ok");
                        }
                    } else {
                        if (muteAgeWs == 0) {
                            recDisplay.setValue("mute_state", "Wait mute");
                        } else if (muteAgeWs < muteAgeWait) {
                            recDisplay.setValue("mute_state", "Wait age");
                        } else {
                            recDisplay.setValue("mute_state", "Age ok");
                        }
                    }
                } else {
                    if (muteAgeWs == 0) {
                        recDisplay.setValue("mute_state", "Unmute");
                    } else {
                        recDisplay.setValue("mute_state", "Mute");
                    }
                }
            }

            // Печать состояния
            UtData.outTable(stDisplay);
            //
            if (countMute == 0) {
                System.out.println("No workstations in MUTE");
            } else if (countMute == count_total) {
                System.out.println("All workstations in MUTE, min age: " + muteAgeMin + ", max age: " + muteAgeMax);
            } else {
                System.out.println("Workstations in MUTE: " + countMute + "/" + count_total);
            }

            // Не делаем ожидания, если не нужно
            if (!doWaitMute && !doWaitUnmute) {
                return muteAgeMax;
            }

            // Выход из ожидания, если все UNMUTE
            if (doWaitUnmute && countUnmute == count_total) {
                return muteAgeMax;
            }

            // Выход из ожидания, если все MUTE и не нужно ждать возраста
            if (doWaitMute && countMute == count_total && muteAgeWait == 0) {
                return muteAgeMax;
            }

            // Выход из ожидания, если если все MUTE и дождались возраста
            if (doWaitMute && countMute == count_total && muteAgeMin >= muteAgeWait) {
                System.out.println("Wait for MUTE age done: " + muteAgeWait);
                return muteAgeMax;
            }

            //
            if (doWaitUnmute) {
                System.out.println("Wait for all UNMUTE");
            } else if (muteAgeWait == 0) {
                System.out.println("Wait for all MUTE");
            } else {
                System.out.println("Wait for MUTE age: " + muteAgeWait + ", done: " + countWaitAge + "/" + count_total);
            }

            //
            Thread.sleep(2000);
        }
    }


    public void srvAppUpdate(String exeFileName, String queName) throws Exception {
        log.info("srvAppUpdate, exe fileName: " + exeFileName);

        UtRepl utRepl = new UtRepl(db, struct);

        // Выбор очереди - общая (queCommon) или личная для станции
        IJdxReplicaQue que;
        if (queName.compareToIgnoreCase(UtQue.QUE_COMMON) == 0) {
            // Очередь queCommon (общая)
            que = queCommon;

            // Команда на обновление
            IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

            // Системная команда - в исходящую очередь реплик
            que.push(replica);

            //
            log.info("srvAppUpdate, to ws all, QUE_COMMON");
        } else if (queName.compareToIgnoreCase(UtQue.QUE_OUT001) == 0) {
            for (long wsId : mailerList.keySet()) {
                // Очередь queOut001 станции (инициализационная или для системных команд)
                JdxQueOut001 queOut001 = new JdxQueOut001(db, wsId);
                queOut001.setDataRoot(dataRoot);
                que = queOut001;

                // Команда на обновление
                IReplica replica = utRepl.createReplicaAppUpdate(exeFileName);

                // Системная команда - в исходящую очередь реплик
                que.push(replica);

                //
                log.info("srvAppUpdate, to ws: " + wsId);
            }
        } else {
            throw new XError("Unknown queName: " + queName);
        }
    }

    public void srvMergeRequest(String planFileName) throws Exception {
        log.info("srvMergeRequest, plan file: " + planFileName);

        //
        UtRepl utRepl = new UtRepl(db, struct);

        //
        UtRecMergePlanRW reader = new UtRecMergePlanRW();
        Collection<RecMergePlan> mergePlans = reader.readPlans(planFileName);

        // Только рабочая станция знает, какая у нас wsId
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();

        //
        IJdxDataSerializer dataSerializer = new JdxDataSerializerDecode(db, ws.wsId);

        //
        for (RecMergePlan mergePlan : mergePlans) {
            // Таблица и поля в Serializer-е
            IJdxTable table = struct.getTable(mergePlan.tableName);
            dataSerializer.setTable(table, UtJdx.fieldsToString(table.getFields()));

            //
            log.info("srvMergeRequest, table: " + mergePlan.tableName + ", records delete: " + mergePlan.recordsDelete.size());

            // Добавим эталонную запись на сервере
            Map<String, Object> values = dataSerializer.prepareValues(mergePlan.recordEtalon);
            // Чтобы вставилось с новым PK
            IJdxField pkField = table.getPrimaryKey().get(0);
            String pkFieldName = pkField.getName();
            values.put(pkFieldName, null);
            // Вставляем эталонную запись
            JdxDbUtils dbu = new JdxDbUtils(db, struct);
            long recordEtalonId = dbu.insertRec(mergePlan.tableName, values);

            // Записываем PK только что вставленной записи
            values.put(pkFieldName, recordEtalonId);

            // Исправляем запись mergePlan.recordEtalon в плане (исправляем ссылки если они в плане не подготовлены с дополнением)
            Map<String, String> valuesStr = dataSerializer.prepareValuesStr(values);
            mergePlan.recordEtalon = valuesStr;

            // Отправим реплику на вставку эталонной записи
            IReplica replicaIns = utRepl.createReplicaInsRecord(mergePlan.tableName, mergePlan.recordEtalon, ws.wsId);
            replicaIns.getInfo().setReplicaType(JdxReplicaType.IDE_MERGE);
            queCommon.push(replicaIns);

            // todo рассмотреть неоходимость добавлять каскадно и ВЛИЯЮШИЕ записи для вставляемой эталонной
            //  (и далее - каскадно) - их ТОЖЕ может не оказаться на филиале

            //
            //log.info("srvMergeRequest, replica etalon ins: " + replicaIns.getData());

            // Исправляем ссылки в mergePlan.recordsDelete (если они в плане не подготовлены с дополнением ссылки)
            // Распаковываем PK удаляемых записей
            Collection<Long> recordsDelete = new ArrayList<>();
            for (String recordDeleteIdStr : mergePlan.recordsDelete) {
                Long recordDeleteId = UtJdxData.longValueOf(dataSerializer.prepareValue(recordDeleteIdStr, pkField));
                recordsDelete.add(recordDeleteId);
            }
            // Обратно запаковываем PK удаляемых записей
            Collection<String> recordsDeleteStr = new ArrayList<>();
            for (Long recordDeletePk : recordsDelete) {
                String recordDeletePkStr = dataSerializer.prepareValueStr(recordDeletePk, pkField);
                recordsDeleteStr.add(recordDeletePkStr);
            }
            // Исправляем PK у recordsDelete в плане
            mergePlan.recordsDelete = recordsDeleteStr;
        }

        // Записываем обновленный план (в нем PK новых записей проставлены как надо)
        String planFileNameRef = dataRoot + "temp/" + "plan.json";
        reader.writePlans(mergePlans, planFileNameRef);

        // Формируем команду на merge
        IReplica replicaMerge = utRepl.createReplicaMerge(planFileNameRef);
        queCommon.push(replicaMerge);

        //
        //log.info("srvMergeRequest, replica merge: " + replicaMerge.getData());
        log.info("srvMergeRequest, replica merge done");
    }

    public void srvRequestSnapshot(long destinationWsId, String tableNames, String queName) throws Exception {
        log.info("srvRequestSnapshot, destination wsId: " + destinationWsId + ", tables: " + tableNames + ", que: " + queName);

        // Разложим в список
        List<IJdxTable> tableList = UtJdx.stringToTables(tableNames, struct);

        // Сортируем список, чтобы несколько snapsot-реплик не сломали ссылки
        List<IJdxTable> tableListSorted = UtJdx.sortTablesByReference(tableList);

        //
        UtRepl utRepl = new UtRepl(db, struct);
        for (IJdxTable table : tableListSorted) {
            log.info("srvRequestSnapshot, table: " + table.getName());

            // Выбор очереди, куда пошлем запрос - общая (queCommon) или личная для станции (queOut001)
            IJdxReplicaQue que;
            if (queName.compareToIgnoreCase(UtQue.QUE_COMMON) == 0) {
                // Очередь queCommon (общая)
                que = queCommon;
            } else if (queName.compareToIgnoreCase(UtQue.QUE_OUT001) == 0) {
                // Очередь queOut001 станции (инициализационная или для системных команд)
                JdxQueOut001 queOut001 = new JdxQueOut001(db, destinationWsId);
                queOut001.setDataRoot(dataRoot);
                que = queOut001;
            } else {
                throw new XError("Unknown queName: " + queName);
            }

            // Реплика-запрос snapshot
            IReplica replica = utRepl.createReplicaWsSendSnapshot(destinationWsId, table.getName());

            // Реплика-запрос snapshot - в очередь реплик
            que.push(replica);
        }

    }

    public void srvSendSnapshot(long destinationWsId, String tableNames) throws Exception {
        log.info("srvSendSnapshot, destination wsId: " + destinationWsId + ", tables: " + tableNames);


        // todo: static RefDecodeStrategy instance - ваще капец!
        // Именно за этим тут и нужна ИНИЦИАЛИЗАЦИЯ ws.init, и больше ни для чего!!!
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();


        // Разложим в список
        List<IJdxTable> tables = UtJdx.stringToTables(tableNames, struct);

        // Очередь queOut001 станции (инициализационная или для системных команд)
        JdxQueOut001 queOut001 = new JdxQueOut001(db, destinationWsId);
        queOut001.setDataRoot(dataRoot);

        // Правила публикаций (фильтры) для станции wsId.
        // В качестве фильтров на ОТПРАВКУ от сервера берем ВХОДЯЩЕЕ правило рабочей станции.
        IPublicationRuleStorage publicationRule = publicationsInList.get(destinationWsId);

        // Создаем снимок таблицы и кладем его в очередь queOut (разрешаем отсылать чужие записи)
        UtRepl ut = new UtRepl(db, struct);
        ut.createSendSnapshotForTables(tables, SERVER_WS_ID, destinationWsId, publicationRule, false, queOut001);
    }


    public void srvDbStructFinish() throws Exception {
        log.info("srvDbStructFinish");

        // Системные команды в общую исходящую очередь реплик
        srvDbStructFinishInternal(queCommon, true);
    }


    /**
     * Системные команды в очередь que
     */
    private void srvDbStructFinishInternal(IJdxReplicaQue que, boolean sendSnapshot) throws Exception {
        IReplica replica;
        UtRepl utRepl = new UtRepl(db, struct);


        // Системная команда "SET_DB_STRUCT"...
        replica = utRepl.createReplicaSetDbStruct(sendSnapshot);
        // ...в очередь
        que.push(replica);


        // Системная команда "UNMUTE" ...
        replica = utRepl.createReplicaUnmute(0);
        // ...в очередь
        que.push(replica);
    }


    public void srvSendCfg(String cfgFileName, String cfgType, long destinationWsId, String queName) throws Exception {
        log.info("srvSendCfg, cfgFileName: " + new File(cfgFileName).getAbsolutePath() + ", cfgType: " + cfgType + ", destination wsId: " + destinationWsId);

        // Выбор очереди - общая (queCommon) или личная для станции
        IJdxReplicaQue que;
        if (queName.compareToIgnoreCase(UtQue.QUE_COMMON) == 0) {
            // Очередь queCommon (общая)
            que = queCommon;
        } else if (queName.compareToIgnoreCase(UtQue.QUE_OUT001) == 0) {
            // Очередь queOut001 станции (инициализационная или для системных команд)
            JdxQueOut001 queOut001 = new JdxQueOut001(db, destinationWsId);
            queOut001.setDataRoot(dataRoot);
            que = queOut001;
        } else {
            throw new XError("Unknown queName: " + queName);
        }

        //
        JSONObject cfg = UtRepl.loadAndValidateJsonFile(cfgFileName);
        srvSetAndSendCfgInternal(que, cfg, cfgType, destinationWsId);
    }

    private void srvSetAndSendCfgInternal(IJdxReplicaQue que, JSONObject cfg, String cfgType, long destinationWsId) throws Exception {
        //
        db.startTran();
        try {
            // Обновляем конфиг в таблицах для рабочих станций (SRV_WORKSTATION_LIST)
            CfgManager cfgManager = new CfgManager(db);
            cfgManager.setWsCfg(cfg, cfgType, destinationWsId);

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
                long queDoneAge = stateManager.getWsQueInNoDone(wsId);
                long queMaxAge = mailer.getBoxState("from");

                //
                long count = 0;
                for (long age = queDoneAge + 1; age <= queMaxAge; age++) {
                    log.info("receive, wsId: " + wsId + ", receiving.age: " + age);

                    // Информацмия о реплике с почтового сервера
                    IReplicaInfo info = mailer.getReplicaInfo("from", age);

                    // Физически забираем данные с почтового сервера
                    IReplica replica = mailer.receive("from", age);

                    // Проверяем целостность скачанного
                    UtJdx.checkReplicaCrc(replica, info.getCrc());

                    // Читаем заголовок
                    JdxReplicaReaderXml.readReplicaInfo(replica);

                    // Помещаем полученные данные в общую очередь
                    db.startTran();
                    try {
                        // Помещаем в очередь
                        long commonQueAge = commonQue.push(replica);

                        // Отмечаем факт скачивания
                        stateManager.setWsQueInNoDone(wsId, age);

                        // todo: Почему для сервера - сразу ТУТ реагируем, а для станции - потом??? И почему ТУТ не проверяется адресат????
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
                if (count > 0) {
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

    private Map getInfoSrv() {
        return null;
    }


    /**
     * Список активных рабочих станций
     */
    private DataStore loadWsList() throws Exception {
        // Берем только активные
        String sql = "select " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST.* " +
                "from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST " +
                "join " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE on " +
                "(" + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST.id = " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE.ws_id) " +
                "where " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_STATE.enabled = 1";

        //
        DataStore st = db.loadSql(sql);

        //
        return st;
    }

    /**
     * Одна конкретная рабочая станция
     */
    private DataRecord loadWs(long wsId) throws Exception {
        String sql = "select * from " + UtJdx.SYS_TABLE_PREFIX + "SRV_WORKSTATION_LIST where id = " + wsId;

        //
        DataStore st = db.loadSql(sql);

        //
        if (wsId != 0 && st.size() == 0) {
            throw new XError("Рабочая станция не найдена: " + wsId);
        }

        //
        return st.getCurRec();
    }


}


