package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jandcode.web.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.publication.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
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
    IMailer mailer;

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
        db.getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
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
        mailer = new MailerHttp();
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
        UtRepl utRepl = new UtRepl(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        // Весь свой аудит выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        //
        db.startTran();
        try {
            // Искусственно увеличиваем возраст (установочная реплика сдвигает возраст БД на 1)
            long age = utRepl.incAuditAge();
            log.info("createReplicaSnapshot, new age: " + age);

            //
            for (IPublication publication : publicationsOut) {
                // Забираем установочную реплику
                IReplica replicaSnapshot = utRepl.createReplicaSnapshot(wsId, publication, age);

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
        UtRepl utRepl = new UtRepl(db, struct);

        // Если в стостоянии "я замолчал", то молчим
        JdxMuteManagerWs utmm = new JdxMuteManagerWs(db);
        if (utmm.isMute()) {
            log.info("handleSelfAudit, workstation is mute");
            return;
        }

        // Проверяем совпадает ли реальная структура БД с утвержденной структурой
        IJdxDbStruct structStored = utRepl.dbStructLoad();
        if (structStored != null && !UtDbComparer.dbStructIsEqual(struct, structStored)) {
            log.error("handleSelfAudit, database struct is not match");
            return;
        }


        // Формируем реплики (по собственным изменениям)
        db.startTran();
        try {
            long count = 0;

            // Узнаем (и заодно фиксируем) возраст своего аудита
            UtAuditAgeManager uta = new UtAuditAgeManager(db, struct);
            long auditAgeTo = uta.markAuditAge();

            // До какого возраста сформировали реплики для своего аудита
            JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
            long auditAgeFrom = stateManager.getAuditAgeDone();

            //
            // если нет публикаций, то аудит копится, а потом не получается ничего сказать
            for (long age = auditAgeFrom + 1; age <= auditAgeTo; age++) {
                for (IPublication publication : publicationsOut) {
                    IReplica replica = utRepl.createReplicaFromAudit(wsId, publication, age);

                    // Пополнение исходящей очереди реплик
                    queOut.put(replica);
                }

                //
                stateManager.setAuditAgeDone(age);

                //
                count++;
            }

            //
            if (count == 0) {
                log.info("handleSelfAudit, wsId: " + wsId + ", audit.age: " + auditAgeFrom + ", nothing to do");
            } else {
                log.info("handleSelfAudit, wsId: " + wsId + ", audit.age: " + auditAgeFrom + " -> " + auditAgeTo + ", done count: " + count);
            }


            //
            db.commit();
        } catch (Exception e) {
            db.rollback(e);
            throw e;
        }

    }

    /**
     * Применяем входящие реплики
     */
    public void handleQueIn() throws Exception {
        log.info("handleQueIn, self.wsId: " + wsId);

        //
        UtRepl utRepl = new UtRepl(db, struct);

        //
        UtAuditApplyer applyer = new UtAuditApplyer(db, struct);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxMuteManagerWs muteManager = new JdxMuteManagerWs(db);

        // Проверяем совпадает ли реальная структура БД с утвержденной структурой
        boolean dbStructIsEqual = true;
        IJdxDbStruct dbStructStored = utRepl.dbStructLoad();
        String dbStructStoredCrc = UtDbComparer.calcDbStructCrc(struct);
        if (!UtDbComparer.dbStructIsEqual(struct, dbStructStored)) {
            dbStructIsEqual = false;
        }

        //
        long queInNoDone = stateManager.getQueInNoDone();
        long queInNoAvailable = queIn.getMaxNo();

        //
        long count = 0;
        for (long no = queInNoDone + 1; no <= queInNoAvailable; no++) {
            log.info("handleQueIn, self.wsId: " + wsId + ", no: " + no + " (" + count + "/" + (queInNoAvailable - queInNoDone) + ")");

            //
            IReplica replica = queIn.getByNo(no);

            // Пробуем применить реплику
            boolean replicaUsed = true;
            //
            switch (replica.getInfo().getReplicaType()) {
                case JdxReplicaType.MUTE: {
                    // Реакция на команду - перевод в режим "MUTE"

                    // Последняя обработка собственного аудита
                    handleSelfAudit();

                    // Переход в состояние "Я замолчал"
                    muteManager.muteWorkstation();

                    // Выкладывание реплики "Я замолчал"
                    reportMuteDone(JdxReplicaType.MUTE_DONE);

                    //
                    break;
                }
                case JdxReplicaType.UNMUTE: {
                    // Реакция на команду - отключение режима "MUTE"

                    // Выход из состояния "Я замолчал"
                    muteManager.unmuteWorkstation();

                    // Выкладывание реплики "Я уже не молчу"
                    reportMuteDone(JdxReplicaType.UNMUTE_DONE);

                    //
                    break;
                }
                case JdxReplicaType.SET_DB_STRUCT: {
                    // Реакция на команду - задать разрешенную структуру БД

                    // В этой реплике - новая утвержденная структура
                    InputStream stream = UtRepl.getReplicaInputStream(replica);
                    try {
                        utRepl.dbStructSaveFrom(stream);
                    } finally {
                        stream.close();
                    }

                    // Выкладывание реплики "структура принята"
                    reportMuteDone(JdxReplicaType.SET_DB_STRUCT_DONE);

                    //
                    break;
                }
                case JdxReplicaType.IDE:
                case JdxReplicaType.SNAPSHOT: {
                    // Реальная структура базы НЕ совпадает с утвержденной структурой
                    if (!dbStructIsEqual) {
                        log.error("handleQueIn, database struct is not match");
                        replicaUsed = false;
                        break;
                    }

                    // Свои собственные установочные реплики можно не применять
                    if (replica.getInfo().getWsId() == wsId && replica.getInfo().getReplicaType() == JdxReplicaType.SNAPSHOT) {
                        break;
                    }

                    // Реальная структура базы НЕ совпадает со структурой, с которой была подготовлена реплика
                    JdxReplicaReaderXml.readReplicaInfo(replica);
                    String dbStructCrc = replica.getInfo().getDbStructCrc();
                    if (dbStructCrc.compareToIgnoreCase(dbStructStoredCrc) != 0) {
                        log.error("handleQueIn, database structCrc is not match, expected: " + dbStructStoredCrc + ", actual: " + dbStructCrc);
                        return;
                    }

                    // todo: Проверим протокол репликатора, с помощью которого была подготовлена репоика
                    // String protocolVersion = (String) replica.getInfo().getProtocolVersion();
                    // if (protocolVersion.compareToIgnoreCase(REPL_PROTOCOL_VERSION) != 0) {
                    //      throw new XError("mailer.receive, protocolVersion.expected: " + REPL_PROTOCOL_VERSION + ", actual: " + protocolVersion);
                    //}


                    // Применение реплик
                    for (IPublication publication : publicationsIn) {
                        InputStream inputStream = null;
                        try {
                            // Откроем Zip-файл
                            inputStream = UtRepl.getReplicaInputStream(replica);

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

            // Не использованная реплика - повод для остановки
            if (!replicaUsed) {
                log.info("handleQueIn, break using replica");
                break;
            }

            // Отметим применение реплики
            stateManager.setQueInNoDone(no);


            //
            count++;
        }

        //
        if (count == 0) {
            log.info("handleQueIn, self.wsId: " + wsId + ", queIn: " + queInNoDone + ", nothing to do");
        } else {
            log.info("handleQueIn, self.wsId: " + wsId + ", queIn: " + queInNoDone + " -> " + queInNoAvailable + ", done count: " + count);
        }
    }


    /**
     * Рабочая станция: отправка системной реплики
     * (например, ответа "я замолчал" или "Я уже не молчу")
     * в исходящую очередь
     */
    public void reportMuteDone(int replicaType) throws Exception {
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        UtRepl utRepl = new UtRepl(db, struct);

        // Весь свой аудит предварительно выкладываем в очередь.
        // Это делается потому, что queOut.put() следит за монотонным увеличением возраста,
        // а ним надо сделать искусственное увеличение возраста.
        handleSelfAudit();

        // Искусственно увеличиваем возраст (системная реплика сдвигает возраст БД на 1)
        long age = utRepl.incAuditAge();
        log.info("reportMuteDone, new age: " + age);

        //
        IReplica replica = new ReplicaFile();
        replica.getInfo().setReplicaType(replicaType);
        replica.getInfo().setWsId(wsId);
        replica.getInfo().setAge(age);

        //
        utRepl.createOutput(replica);
        utRepl.closeOutput();

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
        String guid = ((MailerHttp) mailer).guid;
        String guidPath = guid.replace("-", "/");

        //
        JSONObject cfgData = (JSONObject) UtJson.toObject(UtFile.loadString(cfgFileName));

        // Конфиг для мейлера
        JSONObject cfgWs = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgWs.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IMailer mailerLocal = new MailerLocalFiles();
        mailerLocal.init(cfgWs);


        // Физически забираем данные
        receiveInternal(mailerLocal);
    }


    // Физически забираем данные
    public void receive() throws Exception {
        receiveInternal(mailer);
    }


    void receiveInternal(IMailer mailer) throws Exception {
        // Узнаем сколько получено у нас
        long selfReceivedNo = queIn.getMaxNo();

        // Узнаем сколько есть на сервере
        long srvAvailableNo = mailer.getSrvState("to");

        //
        long count = 0;
        for (long no = selfReceivedNo + 1; no <= srvAvailableNo; no++) {
            log.info("receive, wsId: " + wsId + ", receiving.no: " + no);

            // Информация о реплике с почтового сервера
            ReplicaInfo info = mailer.getReplicaInfo("to", no);

            // Нужно ли скачивать эту реплику с сервера?
            IReplica replica;
            if (info.getWsId() == wsId && info.getReplicaType() == JdxReplicaType.SNAPSHOT) {
                // Свои собственные установочные реплики можно не скачивать (и не применять)
                log.info("Found self snapshot replica, age: " + info.getAge());
                //
                replica = new ReplicaFile();
                replica.getInfo().setWsId(info.getWsId());
                replica.getInfo().setAge(info.getAge());
                replica.getInfo().setReplicaType(info.getReplicaType());
            } else {
                // Физически забираем данные реплики с сервера
                replica = mailer.receive("to", no);
                // Проверяем целостность скачанного
                String md5file = JdxUtils.getMd5File(replica.getFile());
                if (!md5file.equals(info.getCrc())) {
                    log.error("receive.replica: " + replica.getFile());
                    log.error("receive.replica.md5: " + md5file);
                    log.error("mailer.info.crc: " + info.getCrc());
                    // Неправильно скачанный файл - удаляем, чтобы потом начать снова
                    replica.getFile().delete();
                    // Ошибка
                    throw new XError("receive.replica.md5 <> mailer.info.crc");
                }
                //
                JdxReplicaReaderXml.readReplicaInfo(replica);
            }

            //
            log.debug("replica.age: " + replica.getInfo().getAge() + ", replica.wsId: " + replica.getInfo().getWsId());

            // Помещаем реплику в свою входящую очередь
            queIn.put(replica);

            // Удаляем с почтового сервера
            mailer.delete("to", no);

            //
            count++;
        }


        //
        mailer.pingRead("to");
        //
        Map info = getInfoWs();
        mailer.setWsInfo(info);


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
        String guid = ((MailerHttp) mailer).guid;
        String guidPath = guid.replace("-", "/");

        // Конфиг для мейлера
        cfgData = (JSONObject) cfgData.get(String.valueOf(wsId));
        cfgData.put("mailRemoteDir", mailDir + guidPath);

        // Мейлер
        IMailer mailerLocal = new MailerLocalFiles();
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

    void sendInternal(IMailer mailer, long age_from, long age_to, boolean doMarkDone) throws Exception {
        JdxStateManagerMail stateManager = new JdxStateManagerMail(db);

        //
        long count = 0;
        for (long age = age_from; age <= age_to; age++) {
            log.info("UtMailer, wsId: " + wsId + ", sending.age: " + age);

            // Берем реплику
            IReplica replica = queOut.getByAge(age);

            // Читаем ее getReplicaInfo (нужна для проверки возраста при отправке)
            JdxReplicaReaderXml.readReplicaInfo(replica);

            // Физически отправляем реплику
            mailer.send(replica, "from", age);

            // Отмечаем факт отправки
            if (doMarkDone) {
                stateManager.setMailSendDone(age);
            }

            //
            count++;
        }

        //
        mailer.pingWrite("from");
        //
        Map info = getInfoWs();
        mailer.setWsInfo(info);

        //
        if (count == 0) {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + age_from + ", nothing to send");
        } else {
            log.info("UtMailer, wsId: " + wsId + ", send.age: " + age_from + " -> " + age_to + ", done count: " + count);
        }
    }


    public Map getInfoWs() throws Exception {
        Map info = new HashMap<>();

        //
        UtAuditAgeManager auditAgeManager = new UtAuditAgeManager(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        JdxStateManagerMail stateMailManager = new JdxStateManagerMail(db);

        //
        long out_auditAgeActual = auditAgeManager.getAuditAge(); // Возраст аудита БД
        long out_queAvailable = stateManager.getAuditAgeDone();  // Возраст аудита, до которого сформирована исходящая очередь
        long out_sendDone = stateMailManager.getMailSendDone();  // Возраст, до которого исходящая очередь отправлена на сервер
        long in_queInNoAvailable = queIn.getMaxNo();             // До какого номера есть реплики во входящей очереди
        long in_queInNoDone = stateManager.getQueInNoDone();     // Номер реплики, до которого обработана (применена) входящая очередь

        //
        info.put("out_auditAgeActual", out_auditAgeActual);
        info.put("out_queAvailable", out_queAvailable);
        info.put("out_sendDone", out_sendDone);
        info.put("in_queInNoAvailable", in_queInNoAvailable);
        info.put("in_queInNoDone", in_queInNoDone);

        //
        try {
            long in_mailAvailable = mailer.getSrvState("to");    // Сколько есть на сервере в ящике для станции
            info.put("in_mailAvailable", in_mailAvailable);
        } catch (Exception e) {
            info.put("in_mailAvailable", e.getMessage());
        }

        //
        return info;
    }


}
