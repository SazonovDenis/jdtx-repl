package jdtx.repl.main.api.util;

import jandcode.utils.error.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import org.apache.commons.logging.*;

import java.util.*;

public class UtMail {


    //
    protected static Log log = LogFactory.getLog("jdtx.UtMail");


    /**
     * Передача того, что не еще передано
     * из очереди que через mailer в ящик box
     */
    public static void sendQueToMail_State(long wsId, IJdxQue que, IMailer mailer, String box, IJdxMailStateManager mailStateManager) throws Exception {
        log.debug("sendQueToMail_State, wsId: " + wsId + ", que: " + que.getQueName() + ", box: " + box);

        // Выясняем объем передачи
        MailSendInfo sendInfo = new MailSendInfo();
        MailSendTask sendTask = getSendTask_State(que, mailer, box, mailStateManager, sendInfo);

        // Собираем реплики, которые запросили
        SortedMap<Long, IReplica> replicasToSend = new TreeMap<>();
        for (long no = sendTask.sendFrom; no <= sendTask.sendTo; no++) {
            // Берем реплику из очереди
            IReplica replica = que.get(no);

            // Читаем заголовок реплики (чтобы replica.info полностью заполнился) -
            // эта информация сообщается на сервер при отправке
            JdxReplicaReaderXml.readReplicaInfo(replica);

            //
            replicasToSend.put(no, replica);
        }

        // Передаем
        UtMail.sendReplicasToMail(sendTask, sendInfo, replicasToSend, wsId, mailer, box, mailStateManager);
    }


    /**
     * Передача запрошенного диапазона
     * из очереди que через mailer в ящик box
     */
    public static void sendQueToMail_Required(MailSendTask sendTask, long wsId, IJdxQue que, IMailer wsMailer, String box) throws Exception {
        log.info("sendQueToMail_Required, wsId: " + wsId + ", que: " + que.getQueName() + ", box: " + box);

        // Есть что запросили передать?
        if (sendTask == null) {
            log.info("sendQueToMail_Required: nothing required, skip");
            return;
        }

        // Собираем реплики, которые запросили
        SortedMap<Long, IReplica> replicasToSend = new TreeMap<>();
        for (long no = sendTask.sendFrom; no <= sendTask.sendTo; no++) {
            // Берем реплику из очереди
            IReplica replica = que.get(no);

            // Читаем заголовок реплики (чтобы replica.info полностью заполнился) -
            // эта информация сообщается на сервер при отправке
            JdxReplicaReaderXml.readReplicaInfo(replica);

            //
            replicasToSend.put(no, replica);
        }

        // Передаем
        UtMail.sendReplicasToMail(sendTask, null, replicasToSend, wsId, wsMailer, box, null);
    }


    /**
     * Передача запрошенного диапазона
     * из общей очереди queCommon через mailer в ящик box
     */
    public static void sendQueToMail_Required_QueCommon(long wsId, IJdxQueCommon queCommon, IMailer wsMailer, String box, String currentExecutor) throws Exception {
        log.info("sendQueToMail_Required_QueCommon, wsId: " + wsId + ", box: " + box);

        // Выясняем, что запросили передать
        RequiredInfo requiredInfo = wsMailer.getSendRequired(box);
        MailSendTask sendTask = UtMail.getRequiredSendTask(queCommon, requiredInfo, currentExecutor);

        // Есть что запросили передать?
        if (sendTask == null) {
            log.info("sendQueToMail_Required_QueCommon: nothing required, skip");
            return;
        }

        // Собираем реплики, которые запросили
        SortedMap<Long, IReplica> replicasToSend = new TreeMap<>();
        for (long no = sendTask.sendFrom; no <= sendTask.sendTo; no++) {
            // Сейчас номер no - это номер автора в ящике box.
            // Выясняем noCommon - номер реплики no в очереди queCommon.
            long noCommon = queCommon.getNoByAuthorNo(no, wsId);

            // Берем реплику из очереди
            IReplica replica = queCommon.get(noCommon);

            // Читаем заголовок реплики (чтобы replica.info полностью заполнился) -
            // эта информация сообщается на сервер при отправке
            JdxReplicaReaderXml.readReplicaInfo(replica);

            //
            replicasToSend.put(no, replica);
        }

        // Передаем
        UtMail.sendReplicasToMail(sendTask, null, replicasToSend, wsId, wsMailer, box, null);
    }


    /**
     * Передача списка реплик replicasToSend через mailer в ящик box
     */
    private static void sendReplicasToMail(MailSendTask sendTask, MailSendInfo sendInfo, Map<Long, IReplica> replicasToSend, long wsId, IMailer mailer, String box, IJdxMailStateManager mailStateManager) throws Exception {
        log.info("sendReplicasToMail, wsId: " + wsId + ", box: " + box + ", send: " + sendTask.sendFrom + " .. " + sendTask.sendTo);

        // Передаем
        long count = 0;
        for (Map.Entry<Long, IReplica> en : replicasToSend.entrySet()) {
            long no = en.getKey();
            IReplica replica = en.getValue();

            // Физически отправим реплику
            mailer.send(replica, box, no);

            // Отметим отправку очередного номера реплики (если отметка двигается вперед)
            if (sendInfo != null && no > sendInfo.mailLastSendNo) {
                mailStateManager.setMailSendDone(no);
            }

            // Двигаем флаг просьбы станции (если просили повторную передачу)
            if (sendTask.required && no >= sendTask.sendFrom) {
                RequiredInfo requiredInfo = new RequiredInfo();
                requiredInfo.requiredFrom = sendTask.sendFrom;
                requiredInfo.requiredTo = sendTask.sendTo;
                requiredInfo.recreate = sendTask.recreate;
                requiredInfo.executor = sendTask.executor;
                // Двигаем флаг, как будто просили следующую - чтобы после сбоя продолжить с того же места
                requiredInfo.requiredFrom = no + 1;
                // Отмечаем на сервере
                mailer.setSendRequired(box, requiredInfo);
                log.warn("sendReplicasToMail, repeat send step, no: " + no);
            }

            //
            log.info("sendReplicasToMail, no: " + no + "/" + replicasToSend.size());

            //
            count = count + 1;
        }

        // Снимем флаг просьбы станции (если просили повторную передачу)
        if (sendTask.required) {
            mailer.setSendRequired(box, new RequiredInfo());
            log.warn("sendReplicasToMail, repeat send done");
        }

        // Отметим попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.write", box);

        //
        if (count > 0) {
            log.info("sendReplicasToMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendTask.sendFrom + " .. " + sendTask.sendTo + ", done count: " + count);
        } else {
            log.info("sendReplicasToMail done, wsId: " + wsId + ", box: " + box + ", send: " + sendTask.sendTo + ", nothing done");
        }
    }


    /**
     * Выясняем объем передачи по требованию
     */
    public static MailSendTask getRequiredSendTask(IJdxReplicaQue que, RequiredInfo requiredInfo, String currentExecutor) throws Exception {
        log.warn("Required: " + requiredInfo);

        // НЕ попросили повторную отправку
        if (requiredInfo.requiredFrom == -1) {
            return null;
        }

        // Кого просили передать? Если не currentExecutor, значит выполнит кто-то другой.
        if (!currentExecutor.equalsIgnoreCase(requiredInfo.executor)) {
            log.info("Requirerd.executor: " + requiredInfo.executor + ", current.executor: " + currentExecutor + ", no send task");
            return null;
        }

        // Попросили повторную отправку
        MailSendTask sendTask = new MailSendTask();
        sendTask.required = true;
        sendTask.sendFrom = requiredInfo.requiredFrom;
        sendTask.sendTo = requiredInfo.requiredTo;
        sendTask.recreate = requiredInfo.recreate;
        sendTask.executor = requiredInfo.executor;

        // Конец диапазона не указан - зададим сами (до последней реплики, что есть в очереди que)
        if (requiredInfo.requiredTo == -1) {
            sendTask.sendTo = que.getMaxNo();
        }

        //
        return sendTask;
    }

    /**
     * Выясняем объем передач, по состоянию очереди
     */
    private static MailSendTask getSendTask_State(IJdxQue que, IMailer mailer, String box, IJdxMailStateManager mailStateManager, MailSendInfo sendInfo) throws Exception {
        MailSendTask sendTask = new MailSendTask();
        sendTask.required = false;

        // Узнаем, сколько есть у нас
        long queNo = que.getMaxNo();
        // Узнаем, какая последняя отправленная
        long mailLastSendNo = mailStateManager.getMailSendDone();

        //
        long sendFrom;
        long sendTo;

        // Зададим от последней отправленной до последней, что у нас есть на раздачу
        sendFrom = mailLastSendNo + 1;
        sendTo = queNo;

        // todo проверка "не отправляли ли ранее такую реплику?" дублируется
        // Проверки: не отправляли ли ранее такую реплику?
        // Защита от ситуации "восстановление БД из бэкапа", а также
        // защита от ситуации "после переноса рабочей станции на старом компьютере проснулась старая копия рабочей станции"
        long srv_no = mailer.getSendDone(box);
        if (sendFrom < srv_no) {
            // Отправка сильно отстает от сервера
            throw new XError("invalid replica.no, box: " + box + ", send.no: " + sendFrom + ", srv.no: " + srv_no + ", server is forward");
        } else if (sendFrom == srv_no && srv_no != 0) {
            // Отправка одинакова с сервером
            IReplicaInfo fileInfo = ((MailerHttp) mailer).getLastReplicaInfo(box);
            String crc = fileInfo.getCrc();
            // Если последнее письмо совпадает - то считаем это недоразумением ит игнорируем.
            IReplica replica = que.get(sendFrom);
            if (!UtJdx.equalReplicaCrc(replica, crc)) {
                throw new XError("invalid replica.no, box: " + box + ", send.no: " + sendFrom + ", srv.no: " + srv_no + ", workstation and server have equal replica.no, but different replica.crc");
            } else {
                log.warn("mailer.send, already sent box: " + box + ", replica.no: " + sendFrom + ", workstation and server have equal replica.no and equal replica.crc");
            }
        } else if (sendFrom > srv_no + 1 && srv_no != 0) {
            // Отправка сильно опережает сервер
            throw new XError("invalid replica.no, box: " + box + ", send.no: " + sendFrom + ", srv.no: " + srv_no + ", workstation is forward");
        }

        //
        sendTask.sendFrom = sendFrom;
        sendTask.sendTo = sendTo;

        //
        sendInfo.mailLastSendNo = mailLastSendNo;

        //
        return sendTask;
    }


}
