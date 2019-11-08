package jdtx.repl.main.task;

import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;

/**
 * BgTask - сервер
 */
public class SrvBgTask extends BgTaskCustom {


    //
    public void run() throws Exception {
        try {
            errors.clear();
            step_server();
        } catch (Exception e) {
            logError(e, "Сервер");
        }
    }


    //
    void step_server() throws Exception {
        log.info("Сервер");

        //
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            //
            log.info("Сервер, настройка");
            JdxReplSrv srv = new JdxReplSrv(db);
            srv.init();

            //
            log.info("Формирование общей очереди");
            try {
                srv.srvHandleCommonQue();
            } catch (Exception e) {
                logError(e);
                collectError("srv.srvHandleCommonQue", e);
            }

            //
            log.info("Тиражирование реплик");
            try {
                srv.srvDispatchReplicas();
            } catch (Exception e) {
                logError(e);
                collectError("srv.srvDispatchReplicas", e);
            }

            //
            IMailer mailer = srv.getMailer();
            logInfo("Отправка ошибок");
            sendErrors(mailer, "srv.errors");

            //
            log.info("Сервер завершен");
        } finally {
            db.disconnect();
        }
    }


}
