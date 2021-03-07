package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

public class UtMail {


    //
    protected static Log log = LogFactory.getLog("jdtx.UtMail");


    /**
     * Передача очереди que в ящик mailer
     */
    static void sendQueToMail(long wsId, IJdxQue que, IMailer mailer, String box, IJdxStateManagerMail stateManager) throws Exception {
        log.info("sendMail, wsId: " + wsId + ", box: " + box);

        //
        long sendFrom;
        long sendTo;
        boolean doMarkDone;

        // Выясняем объем передачи: узнаем сами, сколько просит станция
        SendRequiredInfo requiredInfo = mailer.getSendRequired(box);

        //
        if (requiredInfo.requiredFrom != -1) {
            // Попросили повторную отправку
            log.warn("Repeat send required, from: " + requiredInfo.requiredFrom + ", to: " + requiredInfo.requiredTo + ", recreate: " + requiredInfo.recreate);
            sendFrom = requiredInfo.requiredFrom;
            sendTo = requiredInfo.requiredTo;
            doMarkDone = false;
        } else {
            // Не просили - зададим сами (от последней отправленной до послейдней, что у нас есть на раздачу)
            sendFrom = stateManager.getMailSendDone() + 1;
            sendTo = que.getMaxNo();
            doMarkDone = true;
        }


        long count = 0;
        for (long no = sendFrom; no <= sendTo; no++) {
            IReplica replica = que.get(no);

            // Физически отправим реплику
            mailer.send(replica, box, no);

            // Отметим отправку
            if (doMarkDone) {
                // Отметим отправку очередного номера реплики.
                stateManager.setMailSendDone(no);
            }

            //
            count = count + 1;
        }


        // Снимем флаг просьбы станции
        if (requiredInfo.requiredFrom != -1) {
            mailer.setSendRequired(box, new SendRequiredInfo());
            log.warn("Repeat send done");
        }


        // Отметим попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.write", box);


        //
        if (sendFrom < sendTo) {
            log.info("sendMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendFrom + " .. " + sendTo + ", done count: " + count);
        } else {
            log.info("sendMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendFrom + ", nothing done");
        }

    }


}
