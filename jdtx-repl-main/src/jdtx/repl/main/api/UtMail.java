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
    static void sendQueToMail(long wsId, IJdxQue que, IMailer mailer, String box, IJdxMailStateManager mailStateManager) throws Exception {
        log.info("sendMail, wsId: " + wsId + ", box: " + box);

        // Выясняем объем передачи: узнаем сами, сколько просит станция
        SendRequiredInfo requiredInfo = mailer.getSendRequired(box);

        // Узнаем, сколько есть у нас
        long queMaxNo = que.getMaxNo();
        // Узнаем, какая последняя отправленная
        long queLastSendNo = mailStateManager.getMailSendDone();

        //
        long sendFrom;
        long sendTo;
        if (requiredInfo.requiredFrom != -1) {
            // Попросили повторную отправку
            sendFrom = requiredInfo.requiredFrom;
            if (requiredInfo.requiredTo != -1) {
                sendTo = requiredInfo.requiredTo;
                log.warn("Repeat send required, from: " + requiredInfo.requiredFrom + ", to: " + requiredInfo.requiredTo + ", recreate: " + requiredInfo.recreate);
            } else {
                // Зададим сами (до последней, что у нас есть на раздачу)
                sendTo = queMaxNo;
                log.warn("Repeat send required, from: " + requiredInfo.requiredFrom + ", recreate: " + requiredInfo.recreate);
            }
        } else {
            // Не просили - зададим сами (от последней отправленной до последней, что у нас есть на раздачу)
            sendFrom = queLastSendNo + 1;
            sendTo = queMaxNo;
        }

        //
        long count = 0;
        for (long no = sendFrom; no <= sendTo; no++) {
            IReplica replica = que.get(no);

            // Читаем заголовок
            JdxReplicaReaderXml.readReplicaInfo(replica);

            // Физически отправим реплику
            mailer.send(replica, box, no);

            // Отметим отправку очередного номера реплики (если отметка двигается вперед)
            if (no > queLastSendNo) {
                mailStateManager.setMailSendDone(no);
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
        if (count > 0) {
            log.info("sendMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendFrom + " .. " + sendTo + ", done count: " + count);
        } else {
            log.info("sendMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendFrom + ", nothing done");
        }

    }


}
