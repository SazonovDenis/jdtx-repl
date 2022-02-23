package jdtx.repl.main.api.util;

import jandcode.utils.error.*;
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
    public static void sendQueToMail(long wsId, IJdxQue que, IMailer mailer, String box, IJdxMailStateManager mailStateManager) throws Exception {
        log.info("sendQueToMail, wsId: " + wsId + ", box: " + box);

        // Выясняем объем передачи
        UtMailSendTask sendTask = getSendTask(que, mailer, box, mailStateManager);

        //
        log.info("sendQueToMail, wsId: " + wsId + ", box: " + box + ", send: " + sendTask.sendFrom + " .. " + sendTask.sendTo);

        // Передаем
        long count = 0;
        for (long no = sendTask.sendFrom; no <= sendTask.sendTo; no++) {
            IReplica replica = que.get(no);

            // Читаем заголовок реплики (чтобы replica.info полностью заполнился - это важно при отправке)
            JdxReplicaReaderXml.readReplicaInfo(replica);

            // Физически отправим реплику
            mailer.send(replica, box, no);

            // Отметим отправку очередного номера реплики (если отметка двигается вперед)
            if (no > sendTask.mailLastSendNo) {
                mailStateManager.setMailSendDone(no);
            }

            //
            count = count + 1;
        }

        // Снимем флаг просьбы станции
        if (sendTask.required) {
            mailer.setSendRequired(box, new SendRequiredInfo());
            log.warn("sendQueToMail, repeat send done");
        }

        // Отметим попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.write", box);

        //
        if (count > 0) {
            log.info("sendQueToMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendTask.sendFrom + " .. " + sendTask.sendTo + ", done count: " + count);
        } else {
            log.info("sendQueToMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendTask.sendTo + ", nothing done");
        }
    }

    /**
     * Выясняем объем передачи
     */
    private static UtMailSendTask getSendTask(IJdxQue que, IMailer mailer, String box, IJdxMailStateManager mailStateManager) throws Exception {
        // Выясняем объем передачи: узнаем, сколько просит станция
        SendRequiredInfo requiredInfo = mailer.getSendRequired(box);

        // Узнаем, сколько есть у нас
        long queNo = que.getMaxNo();
        // Узнаем, какая последняя отправленная
        long mailLastSendNo = mailStateManager.getMailSendDone();

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
                sendTo = queNo;
                log.warn("Repeat send required, from: " + requiredInfo.requiredFrom + ", recreate: " + requiredInfo.recreate);
            }
        } else {
            // Не просили - зададим сами (от последней отправленной до последней, что у нас есть на раздачу)
            sendFrom = mailLastSendNo + 1;
            sendTo = queNo;

            // Проверки: не отправляли ли ранее такую реплику?
            // Защита от ситуации "восстановление БД из бэкапа", а также
            // защита от ситуации "после переноса рабочей станции на старом компьютере проснулась старая копия рабочей станции"
            long srv_no = mailer.getSendDone(box);
            if (sendFrom < srv_no && requiredInfo.requiredFrom == -1) {
                // Рабочая станция сильно отстает от сервера
                throw new XError("invalid replica.no, send.no: " + sendFrom + ", srv.no: " + srv_no + ", server is forward");
            } else if (sendFrom == srv_no && srv_no != 0 && requiredInfo.requiredFrom == -1) {
                // Рабочая станция одинакова с сервером
                IReplicaInfo fileInfo = ((MailerHttp) mailer).getLastReplicaInfo(box);
                // Если последнее письмо совпадает - то считаем это недоразумением ит игнорируем.
                IReplica replica = que.get(sendFrom);
                if (!UtJdx.equalReplicaCrc(replica, fileInfo.getCrc())) {
                    throw new XError("invalid replica.no, send.no: " + sendFrom + ", srv.no: " + srv_no + ", workstation and server have equal replica.no, but different replica.crc");
                } else {
                    log.warn("mailer.send, already sent replica.no: " + sendFrom + ", workstation and server have equal replica.no and equal replica.crc");
                }
            } else if (sendFrom > srv_no + 1 && srv_no != 0 && requiredInfo.requiredFrom == -1) {
                // Рабочая станция сильно опережает сервер
                throw new XError("invalid replica.no, send.no: " + sendFrom + ", srv.no: " + srv_no + ", workstation is forward");
            }
        }

        //
        UtMailSendTask sendTask = new UtMailSendTask();
        sendTask.sendFrom = sendFrom;
        sendTask.sendTo = sendTo;
        sendTask.required = requiredInfo.requiredFrom != -1;
        sendTask.mailLastSendNo = mailLastSendNo;

        //
        return sendTask;
    }


}
