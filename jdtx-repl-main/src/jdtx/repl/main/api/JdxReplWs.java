package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;

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
        // Проверка структур аудита
        UtDbObjectManager ut = new UtDbObjectManager(db, struct);
        ut.checkReplVerDb();

        // Код нашей станции
        DataRecord rec = db.loadSql("select * from " + JdxUtils.sys_table_prefix + "db_info").getCurRec();
        if (rec.getValueLong("ws_id") == 0) {
            throw new XError("Invalid rec.ws_id == 0");
        }
        this.wsId = rec.getValueLong("ws_id");

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));
        String url = (String) cfgData.get("url");

        //
        JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
        if (cfgWs == null) {
            throw new XError("JdxReplWs.init: cfgWs == null, wsId: " + wsId + ", cfgFileName: " + cfgFileName);
        }

        // Читаем из этой очереди
        queIn = new JdxQueCommonFile(db, JdxQueType.IN);
        queIn.setBaseDir((String) cfgWs.get("queIn_DirLocal"));

        // Пишем в эту очередь
        queOut = new JdxQuePersonalFile(db, JdxQueType.OUT);
        queOut.setBaseDir((String) cfgWs.get("queOut_DirLocal"));

        // Правила публикации
        publicationsIn = new ArrayList<>();
        publicationsOut = new ArrayList<>();

        // Загружаем правила публикации
        JSONArray publicationsIn = (JSONArray) cfgWs.get("publicationsIn");
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

        JSONArray publicationsOut = (JSONArray) cfgWs.get("publicationsOut");
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

        // Конфиг для мейлера
        cfgWs.put("guid", rec.getValueString("guid"));
        cfgWs.put("url", url);

        // Мейлер
        mailer = new UtMailerHttp();
        mailer.init(cfgWs);

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

        // Весь свой аудит выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        //
        db.startTran();
        try {
            // Искусственно увеличиваем возраст (установочная реплика сдвигает возраст БД на 1)
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
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        UtRepl utRepl = new UtRepl(db);

        // Если в стостоянии "я замолчал", то молчим
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);
        if (utmm.isMute()) {
            log.info("handleSelfAudit, workstation is mute");
            return;
        }

        // Проверяем совпадает ли реальная структура БД с утвержденной структурой
        IJdxDbStruct structStored = utRepl.dbStructLoad();
        if (structStored != null && !DbComparer.dbStructIsEqual(struct, structStored)) {
            log.info("handleSelfAudit, db struct is not match");
            return;
        }

        // Узнаем (и заодно фиксируем) возраст своего аудита
        long auditAgeActual = auditAgeManager.markAuditAge();

        // До какого возраста сформировали реплики для своего аудита
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long auditAgeDone = stateManager.getAuditAgeDone();

        // Формируем реплики (по собственным изменениям)
        long count = 0;
        for (long age = auditAgeDone + 1; age <= auditAgeActual; age++) {
            for (IPublication publication : publicationsOut) {
                IReplica replica = utRepl.createReplicaFromAudit(wsId, publication, age);

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
        UtAuditApplyer applyer = new UtAuditApplyer(db, struct);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);

        //
        long queInNoDone = stateManager.getQueInNoDone();
        long queInNoAvailable = queIn.getMaxNo();

        //
        long count = 0;
        for (long no = queInNoDone + 1; no <= queInNoAvailable; no++) {
            log.info("handleQueIn, wsId: " + wsId + ", no: " + no + " (" + count + "/" + (queInNoAvailable - queInNoDone) + ")");

            //
            IReplica replica = queIn.getByNo(no);

            switch (replica.getReplicaType()) {
                case JdxReplicaType.MUTE: {
                    // Реакция на команду - перевод в режим "MUTE"

                    // Обработка собственного аудита
                    handleSelfAudit();

                    // Переход в состояние "Я замолчал"
                    muteManager.muteWorkstation();

                    // Выкладывание реплики "Я замолчал"
                    reportMuteDone();

                    //
                    break;
                }
                case JdxReplicaType.UNMUTE: {
                    // Реакция на команду - отключение режима "MUTE"

                    // В этой реплике - новая утвержденная структура
                    UtRepl utRepl = new UtRepl(db);
                    utRepl.dbStructSave(replica.getFile());

                    // Выход из состояния "Я замолчал"
                    muteManager.unmuteWorkstation();

                    //
                    break;
                }
                case JdxReplicaType.IDE:
                case JdxReplicaType.SNAPSHOT: {
                    // Свои собственные установочные реплики можно не применять
                    if (replica.getWsId() == wsId && replica.getReplicaType() == JdxReplicaType.SNAPSHOT) {
                        break;
                    }

                    // Применение реплик
                    for (IPublication publication : publicationsIn) {
                        InputStream inputStream = null;
                        try {
                            // Откроем Zip-файл
                            inputStream = UtRepl.createReplicaInputStream(replica);

                            //
                            JdxReplicaReaderXml replicaReader = new JdxReplicaReaderXml(inputStream);

                            //
                            applyer.applyReplica(replicaReader, publication, wsId);
                        } finally {
                            // Закроем читателя Zip-файла
                            if (inputStream != null) {
                                inputStream.close();
                            }
                        }
                    }

                    //
                    break;
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


    /**
     * Сервер: отправка команды "всем молчать" общей очереди
     */
    public void srvMuteAll() throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        UtRepl utr = new UtRepl(db);

        // Весь свой аудит предварительно выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        // Искусственно увеличиваем возраст (системная реплика сдвигает возраст БД на 1)
        long age = utr.incAuditAge();
        log.info("srvMuteAll, new age: " + age);

        //
        IReplica replica = new ReplicaFile();
        replica.setReplicaType(JdxReplicaType.MUTE);
        replica.setWsId(wsId);
        replica.setAge(age);

        //
        utr.createOutput(replica);
        utr.closeOutput();

        //
        db.startTran();
        try {
            // Системная реплика - в исходящую очередь реплик
            queOut.put(replica);

            // Системная реплика - отметка об отправке
            stateManager.setAuditAgeDone(age);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

    }

    /**
     * Рабочая станция: отправка ответа "я замолчал" в исходящую очередь
     */
    public void reportMuteDone() throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        UtRepl utr = new UtRepl(db);

        // Весь свой аудит предварительно выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        // Искусственно увеличиваем возраст (системная реплика сдвигает возраст БД на 1)
        long age = utr.incAuditAge();
        log.info("reportMuteDone, new age: " + age);

        //
        IReplica replica = new ReplicaFile();
        replica.setReplicaType(JdxReplicaType.MUTE_DONE);
        replica.setWsId(wsId);
        replica.setAge(age);

        //
        utr.createOutput(replica);
        utr.closeOutput();

        //
        db.startTran();
        try {
            // Системная реплика - в исходящую очередь реплик
            queOut.put(replica);

            // Системная реплика - отметка об отправке
            stateManager.setAuditAgeDone(age);

            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }
    }

    public void receiveFromDir(String cfgFileName, String mailDir) throws Exception {
        // Готовим локальный мейлер
        mailDir = UtFile.unnormPath(mailDir) + "/";
        String guid = ((UtMailerHttp) mailer).guid;
        String guidPath = guid.replace("-", "/");

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

        // Конфиг для мейлера
        JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgWs.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IJdxMailer mailerLocal = new UtMailerLocalFiles();
        mailerLocal.init(cfgWs);


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
            if (info.wsId == wsId && info.replicaType == JdxReplicaType.SNAPSHOT) {
                // Свои собственные установочные реплики можно не скачивать (и не применять)
                log.info("Found self snapshot replica, age: " + info.age);
                //
                replica = new ReplicaFile();
                replica.setWsId(info.wsId);
                replica.setAge(info.age);
                replica.setReplicaType(info.replicaType);
            } else {
                // Физически забираем данные реплики с сервера
                replica = mailer.receive(no, "to");
                //
                String md5file = JdxUtils.getMd5File(replica.getFile());
                if (!md5file.equals(info.crc)) {
                    log.error("receive.replica: " + replica.getFile());
                    log.error("receive.replica.md5: " + md5file);
                    log.error("mailer.info.crc: " + info.crc);
                    throw new XError("receive.replica.md5 <> mailer.info.crc");
                }
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


    public void sendToDir(String cfgFileName, String mailDir, long age_from, long age_to, boolean doMarkDone) throws Exception {
        // Готовим локальный мейлер
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));
        //
        mailDir = UtFile.unnormPath(mailDir) + "/";
        String guid = ((UtMailerHttp) mailer).guid;
        String guidPath = guid.replace("-", "/");

        // Конфиг для мейлера
        cfgData = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgData.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IJdxMailer mailerLocal = new UtMailerLocalFiles();
        mailerLocal.init(cfgData);


        // Сколько уже отправлено на сервер
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);
        long srvSendAge = stateManager.getMailSendDone();

        // Узнаем сколько есть у нас в очереди на отправку
        long selfQueOutAge = queOut.getMaxAge();

        // От какого возраста отправлять. Если не указано - начнем от ранее отправленного
        if (age_from == 0L) {
            age_from = srvSendAge + 1;
        }

        // До какого возраста отправлять. Если не указано - все у нас что есть в очереди на отправку
        if (age_to == 0L) {
            age_to = selfQueOutAge;
        }


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

    void sendInternal(IJdxMailer mailer, long age_from, long age_to, boolean doMarkDone) throws Exception {
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);

        //
        long count = 0;
        for (long age = age_from; age <= age_to; age++) {
            log.info("UtMailer, wsId: " + wsId + ", sending.age: " + age);

            // Берем реплику
            IReplica replica = queOut.getByAge(age);

            // Физически отправляем реплику
            mailer.send(replica, age, "from");

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
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + age_from + ", nothing to send");
        } else {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + age_from + " -> " + age_to + ", done count: " + count);
        }
    }


}
