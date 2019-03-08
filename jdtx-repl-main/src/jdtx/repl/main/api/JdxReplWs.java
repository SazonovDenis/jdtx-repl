package jdtx.repl.main.api;

import jandcode.dbm.db.Db;
import jandcode.utils.UtFile;
import jandcode.utils.error.XError;
import jdtx.repl.main.api.struct.IJdxDbStruct;
import jdtx.repl.main.api.struct.IJdxDbStructReader;
import jdtx.repl.main.api.struct.JdxDbStructReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Контекст рабочей станции
 */
public class JdxReplWs {


    // Правила публикации
    List<IPublication> publicationsIn;
    List<IPublication> publicationsOut;

    //
    JdxQueCommonFile queIn;
    JdxQuePersonalFile queOut;

    //
    private Db db;
    private long wsId;
    private IJdxDbStruct struct;

    //
    IJdxMailer mailer;

    //
    protected static Log log = LogFactory.getLog("jdtx");


    //
    public JdxReplWs(Db db) throws Exception {
        this.db = db;
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
        // Строго обязательно REPEATABLE_READ, иначе сохранение в age возраста аудита
        // не синхронно с изменениями в таблицах аудита.
        this.db.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    public long getWsId() {
        return wsId;
    }

    /**
     * Рабочая станция, настройка
     *
     * @param cfgFileName json-файл с конфигурацией
     */
    public void init(String cfgFileName) throws Exception {
        JSONObject cfgData;
        Reader r = new FileReader(cfgFileName);
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }

        // Код нашей станции
        if (cfgData.get("wsId") == null || ((Long) cfgData.get("wsId") == 0)) {
            throw new XError("Invalid wsId");
        }
        this.wsId = (Long) cfgData.get("wsId");

        // Читаем из этой очереди
        queIn = new JdxQueCommonFile(db, JdxQueType.IN);
        queIn.setBaseDir((String) cfgData.get("queIn_DirLocal"));

        // Пишем в эту очередь
        queOut = new JdxQuePersonalFile(db, JdxQueType.OUT);
        queOut.setBaseDir((String) cfgData.get("queOut_DirLocal"));

        // Правила публикации
        publicationsIn = new ArrayList<>();
        publicationsOut = new ArrayList<>();

        // Загружаем правила публикации
        JSONArray publicationsIn = (JSONArray) cfgData.get("publicationsIn");
        for (int i = 0; i < publicationsIn.size(); i++) {
            IPublication publication = new Publication();
            String publicationCfgName = (String) publicationsIn.get(i);
            publicationCfgName = cfgFileName.substring(0, cfgFileName.length() - UtFile.filename(cfgFileName).length()) + publicationCfgName + ".json";
            Reader reader = new FileReader(publicationCfgName);
            try {
                publication.loadRules(reader);
            } finally {
                reader.close();
            }
            this.publicationsIn.add(publication);
        }

        JSONArray publicationsOut = (JSONArray) cfgData.get("publicationsOut");
        for (int i = 0; i < publicationsOut.size(); i++) {
            IPublication publication = new Publication();
            String publicationCfgName = (String) publicationsOut.get(i);
            publicationCfgName = cfgFileName.substring(0, cfgFileName.length() - UtFile.filename(cfgFileName).length()) + publicationCfgName + ".json";
            Reader reader = new FileReader(publicationCfgName);
            try {
                publication.loadRules(reader);
            } finally {
                reader.close();
            }

            this.publicationsOut.add(publication);
        }

        //
        //mailer = new UtMailerLocalFiles();
        mailer = new UtMailerHttp();
        mailer.init(cfgData);

        // Стратегии перекодировки каждой таблицы
        String strategyCfgName = "decode_strategy";
        strategyCfgName = cfgFileName.substring(0, cfgFileName.length() - UtFile.filename(cfgFileName).length()) + strategyCfgName + ".json";
        if (RefDecodeStrategy.instance == null) {
            RefDecodeStrategy.instance = new RefDecodeStrategy();
            RefDecodeStrategy.instance.init(strategyCfgName);
        }
    }

    /**
     * Формируем установочную реплику
     */
    public void createSetupReplica() throws Exception {
        log.info("createSetupReplica, wsId: " + wsId);

        //
        UtRepl utr = new UtRepl(db);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        // Проверяем, что весь свой аудит мы уже выложили в очередь
        long auditAgeDone = stateManager.getAuditAgeDone();
        long auditAgeActual = utr.getAuditAge();
        if (auditAgeActual != auditAgeDone) {
            throw new XError("invalid auditAgeActual != auditAgeDone, auditAgeDone: " + auditAgeDone + ", auditAgeActual: " + auditAgeActual);
        }


        // Увеличиваем возраст (установочная реплика просто сдвигает возраст БД)
        long age = utr.incAuditAge();
        log.info("createSetupReplica, new age: " + age);

        //
        for (IPublication publication : publicationsOut) {
            // Забираем установочную реплику
            IReplica setupReplica = utr.createReplicaFull(wsId, publication, age);

            db.startTran();
            try {

                // Помещаем реплику в очередь
                queOut.put(setupReplica);

                //
                stateManager.setAuditAgeDone(age);

                //
                db.commit();
            } catch (Exception e) {
                db.rollback(e);
                throw e;
            }
        }

        //
        log.info("createSetupReplica, done");
    }

    /**
     * Отслеживаем и обрабатываем свои изменения,
     * формируем исходящие реплики
     */
    public void handleSelfAudit() throws Exception {
        log.info("handleSelfAudit, wsId: " + wsId);

        //
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        UtRepl utr = new UtRepl(db);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long auditAgeDone = stateManager.getAuditAgeDone();

        // Фиксируем возраст своего аудита
        long auditAgeActual = ut.markAuditAge();
        log.info("handleSelfAudit, auditAgeDone: " + auditAgeDone + ", auditAgeActual: " + auditAgeActual);

        // Формируем реплики (по собственным изменениям)
        long n = 0;
        for (long age = auditAgeDone + 1; age <= auditAgeActual; age++) {
            for (IPublication publication : publicationsOut) {
                IReplica replica = utr.createReplicaFromAudit(wsId, publication, age);

                //
                db.startTran();
                try {
                    // Пополнение исходящей очереди реплик
                    queOut.put(replica);

                    //
                    stateManager.setAuditAgeDone(age);

                    //
                    db.commit();
                } catch (Exception e) {
                    db.rollback(e);
                    throw e;
                }

            }

            //
            n++;
        }

        //
        log.info("handleSelfAudit, done: " + n + ", age: " + auditAgeActual);
    }

    /**
     * Применяем входящие реплики
     */
    public void handleQueIn() throws Exception {
        log.info("handleQueIn");

        //
        UtAuditApplyer utaa = new UtAuditApplyer(db, struct);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        long queInNoDone = stateManager.getQueInNoDone();
        long queInNoAvailable = queIn.getMaxNo();

        //
        log.info("handleQueIn, done: " + queInNoDone + ", available: " + queInNoAvailable);

        //
        long n = 0;
        for (long no = queInNoDone + 1; no <= queInNoAvailable; no++) {
            log.info("handleQueIn, no: " + no + " (" + n + "/" + (queInNoAvailable - queInNoDone) + ")");

            //
            IReplica replica = queIn.getByNo(no);
            for (IPublication publication : publicationsIn) {
                utaa.applyReplica(replica, publication, wsId);
            }

            //
            stateManager.setQueInNoDone(no);

            //
            n++;
        }

        //
        log.info("handleQueIn, done: " + n + ", no: " + queInNoAvailable);
    }


    public void receive() throws Exception {
        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getSrvSate("to");

        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        //
        log.info("UtMailer, srv.available: " + srvAvailableNo + ", self.received: " + selfReceivedNo);

        //
        long count = 0;
        for (long no = selfReceivedNo + 1; no <= srvAvailableNo; no++) {
            log.info("UtMailer, receive no: " + no);

            // Физически забираем данные
            IReplica replica = mailer.receive(no, "to");

            // Помещаем полученные данные в свою входящую очередь
            ReplicaFile.readReplicaInfo(replica);
            queIn.put(replica);

            //
            count++;
        }

        //
        log.info("UtMailer, receive done: " + count + ", no: " + srvAvailableNo);
    }


    public void send() throws Exception {
        // Узнаем сколько уже отправлено на сервер
        long srvSendAge = mailer.getSrvSate("from");

        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxAge();

        //
        log.info("UtMailer, send.age: " + srvSendAge + ", que.age: " + selfQueOutAge);

        //
        long count = 0;
        for (long age = srvSendAge + 1; age <= selfQueOutAge; age++) {
            log.info("UtMailer, send.age: " + age);

            // Физически отправляем данные
            mailer.send(queOut.getByAge(age), age, "from");

            //
            count++;
        }

        //
        log.info("UtMailer, send done: " + count + ", age: " + selfQueOutAge);
    }


}
