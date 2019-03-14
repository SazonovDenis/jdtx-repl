package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.sql.*;
import java.util.*;

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
        // будет не синхронно с изменениями в таблицах аудита.
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
    public void createSnapshotReplica() throws Exception {
        log.info("createReplicaSnapshot, wsId: " + wsId);

        //
        UtRepl utr = new UtRepl(db);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        // Проверяем, что весь свой аудит мы уже выложили в очередь
        long auditAgeDone = stateManager.getAuditAgeDone();
        long auditAgeActual = utr.getAuditAge();
        if (auditAgeActual != auditAgeDone) {
            throw new XError("invalid auditAgeActual != auditAgeDone, auditAgeDone: " + auditAgeDone + ", auditAgeActual: " + auditAgeActual);
        }

        //
        db.startTran();
        try {
            // Увеличиваем возраст (установочная реплика просто сдвигает возраст БД)
            long age = utr.incAuditAge();
            log.info("createReplicaSnapshot, new age: " + age);

            //
            for (IPublication publication : publicationsOut) {
                // Забираем установочную реплику
                IReplica replicaSnapshot = utr.createReplicaSnapshot(wsId, publication, age);

                // Помещаем реплику в очередь
                queOut.put(replicaSnapshot);

                //
                stateManager.setAuditAgeDone(age);
            }

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

        //
        log.info("createReplicaSnapshot, wsId: " + wsId + ", done");
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

        // Узнаем (и заодно фиксируем) возраст своего аудита
        long auditAgeActual = ut.markAuditAge();

        // До какого возраста сформировали реплики для своего аудита
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long auditAgeDone = stateManager.getAuditAgeDone();

        // Формируем реплики (по собственным изменениям)
        long count = 0;
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
            count++;
        }

        //
        if (count == 0) {
            log.info("handleSelfAudit, wsId: " + wsId + ", audit.age: " + auditAgeDone + ", nothing to do");
        } else {
            log.info("handleSelfAudit, wsId: " + wsId + ", audit.age: " + auditAgeDone + " -> " + auditAgeActual + ", done count: " + count);
        }
    }

    /**
     * Применяем входящие реплики
     */
    public void handleQueIn() throws Exception {
        log.info("handleQueIn, wsId: " + wsId);

        //
        UtAuditApplyer utaa = new UtAuditApplyer(db, struct);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        long queInNoDone = stateManager.getQueInNoDone();
        long queInNoAvailable = queIn.getMaxNo();

        //
        long count = 0;
        for (long no = queInNoDone + 1; no <= queInNoAvailable; no++) {
            log.info("handleQueIn, wsId: " + wsId + ", no: " + no + " (" + count + "/" + (queInNoAvailable - queInNoDone) + ")");

            //
            IReplica replica = queIn.getByNo(no);

            // Свои собственные установочные реплики можно не применять
            if (replica.getWsId() != wsId || replica.getReplicaType() != JdxReplicaType.EXPORT) {
                for (IPublication publication : publicationsIn) {
                    utaa.applyReplica(replica, publication, wsId);
                }
            }

            //
            stateManager.setQueInNoDone(no);

            //
            count++;
        }

        //
        if (count == 0) {
            log.info("handleQueIn, wsId: " + wsId + ", queIn: " + queInNoDone + ", nothing to do");
        } else {
            log.info("handleQueIn, wsId: " + wsId + ", queIn: " + queInNoDone + " -> " + queInNoAvailable + ", done count: " + count);
        }
    }


    public void receiveFromDir(String cfgFileName, String mailFromDir) throws Exception {
        // Готовим локальный мейлер
        mailFromDir = UtFile.unnormPath(mailFromDir) + "/";
        String guidPath = ((UtMailerHttp) mailer).guid.replace("-", "/");
        mailFromDir = mailFromDir + guidPath;

        //
        JSONObject cfgData = null;
        Reader r = new FileReader(cfgFileName);
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }
        //
        cfgData.put("mailRemoteDir", mailFromDir);

        //
        IJdxMailer mailerLocal = new UtMailerLocalFiles();
        mailerLocal.init(cfgData);


        // Физически забираем данные
        receiveInternal(mailerLocal);
    }


    // Физически забираем данные
    public void receive() throws Exception {
        receiveInternal(mailer);
    }


    void receiveInternal(IJdxMailer mailer) throws Exception {
        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getSrvSate("to");

        //
        long count = 0;
        for (long no = selfReceivedNo + 1; no <= srvAvailableNo; no++) {
            log.info("UtMailer, wsId: " + wsId + ", receiving.no: " + no);

            // Информацмия о реплике с почтового сервера
            JdxReplInfo info = mailer.getInfo(no, "to");

            // Нужно ли скачивать эту реплику с сервера?
            IReplica replica;
            if (info.wsId == wsId && info.replicaType == JdxReplicaType.EXPORT) {
                // Свои собственные установочные реплики можно не скачивать (и не применять)
                log.info("Found self setup replica, age: " + info.age);
                //
                replica = new ReplicaFile();
                replica.setWsId(info.wsId);
                replica.setAge(info.age);
                replica.setReplicaType(info.replicaType);
            } else {
                // Физически забираем данные реплики с сервера
                replica = mailer.receive(no, "to");
                //
                JdxReplicaReaderXml.readReplicaInfo(replica);
            }

            // Помещаем реплику в свою входящую очередь
            queIn.put(replica);

            // Удаляем с почтового сервера
            mailer.delete(no, "to");

            //
            count++;
        }

        //
        if (count == 0) {
            log.info("UtMailer, wsId: " + wsId + ", receive.no: " + selfReceivedNo + ", nothing to receive");
        } else {
            log.info("UtMailer, wsId: " + wsId + ", receive.no: " + selfReceivedNo + " -> " + srvAvailableNo + ", done count: " + count);
        }
    }


    public void sendToDir(String cfgFileName, long age_from, long age_to, String mailToDir, boolean doMarkDone) throws Exception {
        // Готовим локальный мейлер
        mailToDir = UtFile.unnormPath(mailToDir) + "/";
        String guidPath = ((UtMailerHttp) mailer).guid.replace("-", "/");
        mailToDir = mailToDir + guidPath;

        //
        JSONObject cfgData = null;
        Reader r = new FileReader(cfgFileName);
        try {
            JSONParser p = new JSONParser();
            cfgData = (JSONObject) p.parse(r);
        } finally {
            r.close();
        }
        //
        cfgData.put("mailRemoteDir", mailToDir);

        //
        IJdxMailer mailerLocal = new UtMailerLocalFiles();
        mailerLocal.init(cfgData);


        // Физически отправляем данные
        sendInternal(mailerLocal, age_from, age_to, doMarkDone);
    }

    public void send() throws Exception {
        // Узнаем сколько уже отправлено на сервер
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        long srvSendAge = stateManager.getMailSendDone();

        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxAge();

        // Физически отправляем данные
        sendInternal(mailer, srvSendAge + 1, selfQueOutAge, true);
    }

    void sendInternal(IJdxMailer mailer, long srvSendAge, long selfQueOutAge, boolean doMarkDone) throws Exception {
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);

        //
        long count = 0;
        for (long age = srvSendAge; age <= selfQueOutAge; age++) {
            log.info("UtMailer, wsId: " + wsId + ", sending.age: " + age);

            // Физически отправляем данные
            mailer.send(queOut.getByAge(age), age, "from");

            // Отмечаем факт отправки
            if (doMarkDone) {
                stateManager.setMailSendDone(age);
            }

            //
            count++;
        }

        //
        mailer.ping("from");

        //
        if (count == 0) {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + srvSendAge + ", nothing to send");
        } else {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + srvSendAge + " -> " + selfQueOutAge + ", done count: " + count);
        }
    }


}
