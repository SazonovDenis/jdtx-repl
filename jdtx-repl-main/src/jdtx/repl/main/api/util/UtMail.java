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
    public static void sendQueToMail_State(long wsId, IJdxQue que, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
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
     * для рабочей станции wsId
     * из очереди que через mailer в ящик box
     */
    public static void sendQueToMail_Required(MailSendTask sendTask, long wsId, IJdxQue que, IMailer wsMailer, String box) throws Exception {
        log.info("sendQueToMail_Required, destination WsId: " + wsId + ", que: " + que.getQueName() + ", box: " + box);

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
    public static void sendQueToMail_Required_QueCommon(MailSendTask sendTask, long wsId, IJdxQueCommon queCommon, IMailer wsMailer, String box) throws Exception {
        log.info("sendQueToMail_Required_QueCommon, wsId: " + wsId + ", box: " + box);

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
    private static void sendReplicasToMail(MailSendTask sendTask, MailSendInfo sendInfo, Map<Long, IReplica> replicasToSend, long wsId, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
        log.info("sendReplicasToMail, destination wsId: " + wsId + ", box: " + box + ", sendTask: " + sendTask + ", count: " + replicasToSend.size());

        // Передаем
        long count = 0;
        for (Map.Entry<Long, IReplica> en : replicasToSend.entrySet()) {
            long no = en.getKey();
            IReplica replica = en.getValue();

            // Физически отправим реплику
            mailer.send(replica, box, no);

            // Отметим отправку очередного номера реплики (если отметка двигается вперед)
            if (sendInfo != null && no > sendInfo.lastSendNoMarked) {
                mailStateManager.setMailSendDone(no);
            }

            // Двигаем вперед запрошенный номер (если просили повторную передачу)
            if (sendTask.required) {
                // Двигаем номер так, чтобы он двигался только вперед от sendTask.sendFrom, и чтобы не стал больше sendTask.sendTo
                if (no >= sendTask.sendFrom && no < sendTask.sendTo) {
                    RequiredInfo requiredInfo = new RequiredInfo();
                    // Двигаем номер вперед так, чтобы в случае сбоя продолжить после номера, который уже передали
                    requiredInfo.requiredFrom = no + 1;
                    requiredInfo.requiredTo = sendTask.sendTo;
                    requiredInfo.recreate = sendTask.recreate;
                    requiredInfo.executor = sendTask.executor;
                    // Отмечаем на сервере
                    mailer.setSendRequired(box, requiredInfo);
                    log.warn("sendReplicasToMail, move forward sendRequired, no: " + no);
                }
            }

            //
            log.info("sendReplicasToMail, destination wsId: " + wsId + ", box: " + box + ", no: " + no + ", " + count + "/" + replicasToSend.size());

            //
            count = count + 1;
        }

        // Снимем флаг просьбы станции (если просили повторную передачу)
        if (sendTask.required) {
            mailer.setSendRequired(box, new RequiredInfo());
            log.warn("sendReplicasToMail, set sendRequired: none");
        }

        // Отметим попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
        mailer.setData(null, "ping.write", box);

        //
        if (count > 0) {
            log.info("sendReplicasToMail done, destination wsId: " + wsId + ", box: " + box + ", sendTask: " + sendTask.sendFrom + " .. " + sendTask.sendTo + ", done count: " + count);
        } else {
            log.info("sendReplicasToMail done, destination wsId: " + wsId + ", box: " + box + ", nothing done");
        }
    }


    /**
     * Выясняем объем передачи по требованию
     */
    public static MailSendTask getRequiredSendTask(IJdxMailSendStateManager mailStateManager, RequiredInfo requiredInfo, String currentExecutor) throws Exception {
        log.info("Required: " + requiredInfo);

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

        // Конец диапазона не указан - зададим сами, его берем до последней реплики, которая отмечена как отправленная.
        // Берем именно возраст ранее отправленной реплики, а не все, что есть в очереди, иначе возможна ситуация,
        // что возраст отмеченных реплик (в базе) начнет отставать от возраста последней отправки (на почтовом сервере).
        //
        // Это может возникнуть из-за того, что "рассылка по требованию" выполняется независимо от "рассылки по расписанию".
        // Метку "последняя отправленная реплика" ставит именно процесс "рассылка по расписанию", и этот возраст должен
        // соответствовать отметке "номер последнего письма" на почтовом сервере (он двигается вперед при отправке почты).
        // Если сейчас по требованию разослать больше, чем ранее разослал процесс "рассылка по расписанию",
        // то получится, что возраст отмеченных реплик (в базе) станет меньше возраста последней отправки (на почтовом сервере),
        // что позже будет распознано как аварийная ситуация.
        if (requiredInfo.requiredTo == -1) {
            // Узнаем, какая последняя отправленная помечена, её и берем как конец требования на отправку.
            sendTask.sendTo = mailStateManager.getMailSendDone();
        }

        //
        return sendTask;
    }


    /**
     * Выясняем объем передач, по состоянию очереди
     */
    private static MailSendTask getSendTask_State(IJdxQue que, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager, MailSendInfo sendInfo) throws Exception {
        MailSendTask sendTask = new MailSendTask();
        sendTask.required = false;

        // Узнаем, сколько есть у нас
        long noQue = que.getMaxNo();
        // Узнаем, какая последняя отправленная помечена
        long noQueSendMarked = mailStateManager.getMailSendDone();

        // Проверка и ремонт отметки "отправлено на сервер"
        repairSendTaskBySrvState(noQueSendMarked, que, mailer, box, mailStateManager);

        // Зададим от последней отправленной до последней, что у нас есть на раздачу
        sendTask.sendFrom = noQueSendMarked + 1;
        sendTask.sendTo = noQue;

        //
        sendInfo.lastSendNoMarked = noQueSendMarked;

        //
        return sendTask;
    }


    /**
     * Проверка и ремонт отметки "отправлено на сервер": не отправляли ли ранее такую реплику?
     * Защита от ситуации "восстановление БД из бэкапа", а также
     * защита от ситуации "после переноса рабочей станции на старом компьютере проснулась старая копия рабочей станции"
     */
    public static void repairSendTaskBySrvState(long noQueSendMarked, IJdxQue que, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
        // Сколько реплик фактически отправлено на сервер (спросим у почтового сервера)
        long noQueSendSrv = mailer.getSendDone(box);

        // Отметка совпадает с состоянием сервера?
        if (noQueSendMarked == noQueSendSrv) {
            // Ничего делать не надо
            return;
        }

        //
        log.warn("Need repair marked, que: " + que.getQueName() + ", box: " + box + ", noQueSendMarked: " + noQueSendMarked + ", noQueSendSrv: " + noQueSendSrv);

        //
        if (noQueSendMarked > noQueSendSrv && noQueSendSrv != 0) {
            // Отметка опережает сервер (а сервер уже какое то время работал)
            throw new XError("Unable to repair marked, noQueSendMarked > noQueSendSrv");
        }

        if (noQueSendMarked == (noQueSendSrv - 1)) {
            // Помеченное (noQueSendMarked) на 1 меньше отправленного (noQueSendSrv)
            // Сравним CRC реплик: в своей очереди и последнего отправленнного письма на сервере.

            // Читаем С СЕРВЕРА информацию о реплике, которую последней отправили на сервер
            IReplicaInfo replicaInfoSrv = ((MailerHttp) mailer).getLastReplicaInfo(box);
            String crcSrv = replicaInfoSrv.getCrc();

            // Берем ИЗ СВОЕЙ очереди ту реплику, которую последней отправили на сервер
            IReplica replicaWs = que.get(noQueSendSrv);

            // Сравниваем CRC реплик
            if (UtJdx.equalReplicaCrc(replicaWs, crcSrv)) {
                // Реплика совпадает с последним письмом - считаем это недоразумением:
                // успели отправить, но не успели отметить.
                // Это бывает, если прерывается процесс передачи реплик на этапе отметки.
                log.warn("Last replica already sent");
                // Просто исправляем отметку "отправлено на сервер".
                mailStateManager.setMailSendDone(noQueSendSrv);
                log.warn("Repair marked, setMailSendDone, " + noQueSendMarked + " -> " + noQueSendSrv);
            } else {
                // Реплика НЕ совпадает с последним письмом - это ошибка
                throw new XError("Unable to repair marked, ws.replica.crc <> mail.replica.crc, ws.replica.crc: " + replicaWs.getInfo().getCrc() + ", mail.replica.crc: " + crcSrv);
            }
        } else {
            // Отметка noQueSendMarked сильно отстает от сервера noQueSendSrv.
            // В отличие от процедуры ремонта repairAfterBackupRestore тут не передвигаем вперед.

            // Это ошибка, не чинится
            throw new XError("Unable to repair marked, noQueSendMarked < noQueSendSrv");
        }
    }


}
