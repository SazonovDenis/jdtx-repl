package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

/**
 * Контекст рабочей станции
 */
public class JdxReplWs {

    //
    List<IPublication> publicationsIn;
    List<IPublication> publicationsOut;

    //
    JdxQueCreatorFile queIn;
    JdxQueCreatorFile queOut;

    //
    Db db;
    IJdxDbStruct struct;


    public JdxReplWs(Db db) throws Exception {
        this.db = db;
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
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
        x.baseFilePath = "../_test-data/queIn/";
        x.reloadDir(queIn);
    }

    /**
     * Применяем входящие реплики
     */
    public void handleInQue() throws Exception {
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


}
