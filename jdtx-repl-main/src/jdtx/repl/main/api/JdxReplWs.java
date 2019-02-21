package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;

/**
 * Контекст рабочей станции
 */
public class JdxReplWs {

    // Правила публикации
    List<IPublication> publicationsIn;
    List<IPublication> publicationsOut;

    //
    JdxQueCommon queIn;
    JdxQueCreatorFile queOut;

    //
    Db db;
    IJdxDbStruct struct;

    //
    UtMailer mailerIn;
    UtMailer mailerOut;


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
        queIn = new JdxQueCommon(db);
        queIn.baseDir = UtFile.unnormPath((String) cfgData.get("queInDir")) + "/";
        queIn.queType = JdxQueType.IN;

        // Пишем в эту очередь
        queOut = new JdxQueCreatorFile(db);
        queOut.baseDir = UtFile.unnormPath((String) cfgData.get("queOutDir")) + "/";
        queOut.queType = JdxQueType.OUT;

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
        JSONObject queOutRoute = (JSONObject) cfgData.get("queOutRoute");
        mailerOut = new UtMailer(db);
        mailerOut.localDir = queOut.baseDir;
        mailerOut.remoteDir = UtFile.unnormPath((String) queOutRoute.get("directory"));
        //
        JSONObject queInRoute = (JSONObject) cfgData.get("queInRoute");
        mailerIn = new UtMailer(db);
        mailerIn.localDir = UtFile.unnormPath((String) queInRoute.get("directory"));
        mailerIn.remoteDir = queIn.baseDir;
    }

    /**
     * Отслеживаем и обрабатываем свои изменения,
     * формируем исходящие реплики
     */
    public void handleSelfAudit() throws Exception {
        UtAuditAgeManager ut = new UtAuditAgeManager(db, struct);
        UtRepl utr = new UtRepl(db);

        JdxStateManager stateManager = new JdxStateManager(db);
        long auditAgeDone = stateManager.getAuditAgeDone();

        // Фиксируем возраст своего аудита
        long auditAgeActual = ut.markAuditAge();
        System.out.println("auditAgeActual = " + auditAgeActual);

        // Формируем реплики (по собственным изменениям)
        for (long age = auditAgeDone + 1; age <= auditAgeActual; age++) {
            for (IPublication publication : publicationsOut) {
                IReplica replica = utr.createReplicaFromAudit(publication, age);

                //
                System.out.println(replica.getFile().getAbsolutePath());

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
        }
    }

    public void pullToQueIn() throws Exception {
        JdxQueReaderDir x = new JdxQueReaderDir();
        x.baseDir = queIn.baseDir;
        x.reloadDir(queIn);
    }

    /**
     * Применяем входящие реплики
     */
    public void handleQueIn() throws Exception {
        UtAuditApplyer utaa = new UtAuditApplyer(db, struct);

        JdxStateManager stateManager = new JdxStateManager(db);

        //
        long inIdDone = stateManager.getQueInIdxDone();
        long inIdMax = queIn.getMaxId();

        //
        for (long inId = inIdDone + 1; inId <= inIdMax; inId++) {
            //
            IReplica replica = queIn.getById(inId);
            for (IPublication publication : publicationsIn) {
                utaa.applyReplica(replica, publication, null);
            }

            //
            stateManager.setQueInIdxDone(inId);
        }
    }


    public void receive() throws IOException {
        mailerIn.receive();
    }

    public void send() throws Exception {
        mailerOut.send();
    }

}
