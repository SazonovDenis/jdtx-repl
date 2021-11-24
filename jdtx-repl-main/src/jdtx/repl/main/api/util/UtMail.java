package jdtx.repl.main.api.util;

import jandcode.utils.error.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class UtMail {


    //
    protected static Log log = LogFactory.getLog("jdtx.UtMail");


    /**
     * Передача очереди que в ящик mailer
     */
    public static void sendQueToMail(long wsId, IJdxQue que, IMailer mailer, String box, IJdxMailStateManager mailStateManager) throws Exception {
        log.info("sendMail, wsId: " + wsId + ", box: " + box);

        // Выясняем объем передачи: узнаем, сколько просит станция
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

            // Проверим, что мы не будем отправлять повторно

            /////////////////////////////////
            //getState_internal - public? В интерфейс его?



            long srv_no = mailer.getSendDone(box);

            // Проверки: не отправляли ли ранее такую реплику?
            // Защита от ситуации "восстановление БД из бэкапа", а также
            // защита от ситуации "после переноса рабочей станции на старом компьютере проснулась старая копия рабочей станции"
            //JSONObject resState = getState_internal(box);
            //JSONObject last_info = (JSONObject) resState.get("last_info");
            //long srv_no = UtJdxData.longValueOf(last_info.getOrDefault("no", 0));
            //JSONObject required = (JSONObject) resState.get("required");
            //SendRequiredInfo requiredInfo = new SendRequiredInfo(required);
            if (sendFrom < srv_no && requiredInfo.requiredFrom == -1) {
                // Рабочая станция сильно отстает от сервера
                throw new XError("invalid replica.no, send.no: " + sendFrom + ", srv.no: " + srv_no + ", server is forward");
            } else if (sendFrom == srv_no && srv_no != 0 && requiredInfo.requiredFrom == -1) {
                // Рабочая станция одинакова с сервером
                //IReplicaInfo fileInfo = getLastReplicaInfo(box);
                IReplicaInfo fileInfo =((MailerHttp) mailer).getLastReplicaInfo(box);
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
            /////////////////////////////////
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
