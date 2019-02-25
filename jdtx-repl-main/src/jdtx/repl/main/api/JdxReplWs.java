package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 * Контекст рабочей станции
 */
public class JdxReplWs {

    long wsId; // todo: правила/страегия работы с WsID

    // Правила публикации
    List<IPublication> publicationsIn;
    List<IPublication> publicationsOut;

    //
    private JdxQueCommonFile queIn;
    private JdxQuePersonalFile queOut;

    //
    Db db;
    IJdxDbStruct struct;

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

        // Читаем из этой очереди
        queIn = new JdxQueCommonFile(db, JdxQueType.IN);
        queIn.setBaseDir(UtFile.unnormPath((String) cfgData.get("queIn_DirLocal")) + "/");

        // Пишем в эту очередь
        queOut = new JdxQuePersonalFile(db, JdxQueType.OUT);
        queOut.setBaseDir(UtFile.unnormPath((String) cfgData.get("queOut_DirLocal")) + "/");

        // Правила публикации
        publicationsIn = new ArrayList<>();
        publicationsOut = new ArrayList<>();

        // Загружаем правила публикации
        JSONArray publicationsInCfg = (JSONArray) cfgData.get("publicationsIn");
        for (int i = 0; i < publicationsInCfg.size(); i++) {
            JSONObject publicationCfg = (JSONObject) publicationsInCfg.get(i);
            JSONArray publicationTables = (JSONArray) publicationCfg.get("tables");
            IPublication publication = new Publication();
            publication.setData(publicationTables);
            publicationsIn.add(publication);
        }

        JSONArray publicationsOutCfg = (JSONArray) cfgData.get("publicationsOut");
        for (int i = 0; i < publicationsOutCfg.size(); i++) {
            JSONObject publicationCfg = (JSONObject) publicationsOutCfg.get(i);
            JSONArray publicationTables = (JSONArray) publicationCfg.get("tables");
            IPublication publication = new Publication();
            publication.setData(publicationTables);
            publicationsOut.add(publication);
        }

        //
        mailer = new UtMailerLocalFiles(queIn, queOut);
        mailer.init(cfgData);
    }

    /**
     * Формируем установочную реплику
     */
    public void createSetupReplica() throws Exception {
        log.info("createSetupReplica");

        //
        UtRepl utr = new UtRepl(db);
        for (IPublication publication : publicationsOut) {
            // Забираем установочную реплику
            IReplica setupReplica = utr.createReplicaFull(wsId, publication);

            // Помещаем реплику в очередь
            queOut.put(setupReplica);
        }

        //
        log.info("createSetupReplica done");
    }

    /**
     * Отслеживаем и обрабатываем свои изменения,
     * формируем исходящие реплики
     */
    public void handleSelfAudit() throws Exception {
        log.info("handleSelfAudit");

        //
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        UtRepl utr = new UtRepl(db);

        //
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);
        long auditAgeDone = stateManager.getAuditAgeDone();

        // Фиксируем возраст своего аудита
        long auditAgeActual = ut.markAuditAge();
        log.info("auditAgeDone: " + auditAgeDone + ", auditAgeActual: " + auditAgeActual);

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
        log.info("handleSelfAudit done: " + n + ", age: " + auditAgeActual);
    }

    /**
     * Применяем входящие реплики
     */
    public void handleQueIn() throws Exception {
        log.info("handleQueIn");

        //
        UtAuditApplyer utaa = new UtAuditApplyer(db, struct);
        JdxStateManagerWs stateManager = new JdxStateManagerWs(db);

        //
        long queInNoDone = stateManager.getQueInNoDone();
        long queInNoAvailable = queIn.getMaxNo();

        //
        log.info("handleQueIn, queIn done: " + queInNoDone + ", queIn available: " + queInNoAvailable);

        //
        long n = 0;
        for (long no = queInNoDone + 1; no <= queInNoAvailable; no++) {
            log.info("handleQueIn, queIn no: " + no);

            //
            IReplica replica = queIn.getByNo(no);
            for (IPublication publication : publicationsIn) {
                utaa.applyReplica(replica, publication, null);
            }

            //
            stateManager.setQueInNoDone(no);

            //
            n++;
        }

        //
        log.info("handleQueIn, done: " + n + ", queIn no: " + queInNoAvailable);
    }


    public void receive() throws Exception {
        mailer.receive();

        //
        JdxQueReaderDir rdr = new JdxQueReaderDir();
        rdr.baseDir = queIn.getBaseDir();
        rdr.loadFromDirToQueIn(queIn);
    }


    public void send() throws Exception {
        mailer.send();
    }


}
