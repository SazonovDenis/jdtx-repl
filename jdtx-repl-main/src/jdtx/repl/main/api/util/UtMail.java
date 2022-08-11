package jdtx.repl.main.api.util;

import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.manager.*;
import jdtx.repl.main.api.que.*;
import jdtx.repl.main.api.replica.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;
import org.joda.time.*;
import org.json.simple.*;

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
        MailSendTask sendTask = getSendTask_State(que, mailer, box, mailStateManager);

        // Собираем реплики, которые запросили
        SortedMap<Long, IReplica> replicasToSend = new TreeMap<>();
        for (long no = sendTask.sendFrom; no <= sendTask.sendTo; no++) {
            // Берем реплику из очереди
            IReplica replica = que.get(no);

            // Читаем заголовок реплики чтобы replica.info полностью заполнился -
            // эта информация сообщается на сервер при отправке
            JdxReplicaReaderXml.readReplicaInfo(replica);

            //
            replicasToSend.put(no, replica);
        }

        // Передаем
        UtMail.sendReplicasToMail(sendTask, replicasToSend, wsId, mailer, box, mailStateManager);
    }


    /**
     * Передача запрошенного диапазона
     * для рабочей станции wsId
     * из очереди que через mailer в ящик box
     */
    public static void sendQueToMail_Required(MailSendTask sendTask, long wsId, IJdxQue que, IMailer wsMailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
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
        UtMail.sendReplicasToMail(sendTask, replicasToSend, wsId, wsMailer, box, mailStateManager);
    }


    /**
     * Передача запрошенного диапазона
     * из общей очереди queCommon через mailer в ящик box.
     * Выполняется на сервере.
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

        // Отправляем почту.
        // Не передаем mailStateManager, т.к. при рассылке из queCommon нам не нужно делать отметку - это рассылка в норме не выполняется вообще.
        UtMail.sendReplicasToMail(sendTask, replicasToSend, wsId, wsMailer, box, null);
    }


    /**
     * Передача списка реплик replicasToSend через mailer в ящик box
     */
    private static void sendReplicasToMail(MailSendTask sendTask, Map<Long, IReplica> replicasToSend, long wsId, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
        log.info("sendReplicasToMail, wsId: " + wsId + ", box: " + box + ", sendTask: " + sendTask + ", count: " + replicasToSend.size());

        // Узнаем, какой номер помечен как отправленный
        long lastNoMailSendMarked = -1;
        if (mailStateManager != null) {
            lastNoMailSendMarked = mailStateManager.getMailSendDone();
        }

        // Передаем
        long count = 0;
        for (Map.Entry<Long, IReplica> en : replicasToSend.entrySet()) {
            long no = en.getKey();
            IReplica replica = en.getValue();

            // Физически отправим реплику
            mailer.send(replica, box, no);

            // Отметим отправку очередного номера реплики (если отметка двигается вперед)
            if (mailStateManager != null && no > lastNoMailSendMarked) {
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
            log.info("sendReplicasToMail, wsId: " + wsId + ", box: " + box + ", no: " + no + ", " + count + "/" + replicasToSend.size());

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
            log.info("sendReplicasToMail done, wsId: " + wsId + ", box: " + box + ", sendTask: " + sendTask.sendFrom + " .. " + sendTask.sendTo + ", done count: " + count);
        } else {
            log.info("sendReplicasToMail done, wsId: " + wsId + ", box: " + box + ", nothing done");
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
        // соответствовать отметке "номер последнего письма" на почтовом сервере (он двигается вперед при отправке новой почты).
        // Если сейчас по требованию разослать больше, чем ранее разослал процесс "рассылка по расписанию",
        // то получится, что возраст отмеченных реплик (в базе) станет меньше возраста последней отправки (на почтовом сервере),
        // что позже будет распознано как аварийная ситуация.
        if (requiredInfo.requiredTo == -1) {
            if (mailStateManager != null) {
                // Узнаем, какая последняя отправленная помечена, её и берем как конец требования на отправку.
                sendTask.sendTo = mailStateManager.getMailSendDone();
            } else {
                // Если не передали mailStateManager, то нужно просить точный диапазон самомостоятельно
                throw new XError("getRequiredSendTask: required.requiredTo == -1 && mailStateManager == null");
            }
        }

        //
        return sendTask;
    }


    /**
     * Выясняем объем передач, по состоянию очереди
     */
    private static MailSendTask getSendTask_State(IJdxQue que, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
        MailSendTask sendTask = new MailSendTask();
        sendTask.required = false;

        // Узнаем, сколько есть у нас
        long noQue = que.getMaxNo();
        // Узнаем, какая последняя отправленная помечена
        long lastNoMailSendMarked = mailStateManager.getMailSendDone();

        // Зададим от последней отправленной до последней, что у нас есть на раздачу
        sendTask.sendFrom = lastNoMailSendMarked + 1;
        sendTask.sendTo = noQue;

        //
        return sendTask;
    }

    /**
     * Проверка и ремонт отметки "отправлено на сервер": не отправляли ли ранее такую реплику?
     * Защита от ситуаций:
     * <p>
     * - "восстановление БД из бэкапа"
     * <p>
     * - "после переноса рабочей станции на старом компьютере проснулась старая копия рабочей станции"
     */
    public static void repairSendMarkedBySendDone(IJdxQue que, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
        // Узнаем, какая последняя отправленная помечена
        long noQueSendMarked = mailStateManager.getMailSendDone();

        // Сколько реплик фактически отправлено на сервер (спросим у почтового сервера)
        long noQueSendDone = mailer.getSendDone(box);

        if (!wasRepairedSendMarked(noQueSendMarked, noQueSendDone, que, mailer, box, mailStateManager)) {
            throw new XError("Unable to repair noQueSendMarked <> noQueSendDone");
        }
    }

    public static boolean wasRepairedSendMarked(long noQueSendMarked, long noQueSendDone, IJdxQue que, IMailer mailer, String box, IJdxMailSendStateManager mailStateManager) throws Exception {
        // Отметка совпадает с состоянием сервера?
        if (noQueSendMarked == noQueSendDone) {
            // Ничего чинить не надо
            return true;
        }

        //
        log.warn("RepairSendMarked, need repair marked, que: " + que.getQueName() + ", box: " + box + ", noQueSendMarked: " + noQueSendMarked + ", noQueSendDone: " + noQueSendDone);

        // Отметка опережает почтовый сервер
        if (noQueSendMarked > noQueSendDone) {
            // На почтовый сервер ранее уже что-то отправляли? Учет этого важен:
            // при добавлении новой рабочей станции отметка почтового сервера noQueSendDone будет равна 0,
            // а отметка noQueSendMarked будет установлена не равной 0.
            if (noQueSendDone == 0) {
                log.warn("RepairSendMarked, noQueSendDone == 0, que: " + que.getQueName() + ", box: " + box + ", noQueSendMarked: " + noQueSendMarked + ", noQueSendDone: " + noQueSendDone);
                return true;
            } else {
                log.error("RepairSendMarked, unable to repair marked, noQueSendMarked > noQueSendDone, que: " + que.getQueName() + ", box: " + box + ", noQueSendMarked: " + noQueSendMarked + ", noQueSendDone: " + noQueSendDone);
                return false;
            }
        }

        if (noQueSendMarked == (noQueSendDone - 1)) {
            // Помеченное (noQueSendMarked) на 1 меньше отправленного (noQueSendDone)
            // Сравним CRC и номер реплик: в своей очереди и последнего отправленнного письма на сервере.
            if (equalLastSend(mailer, box, que, noQueSendDone)) {
                // Просто исправляем отметку "отправлено на сервер".
                mailStateManager.setMailSendDone(noQueSendDone);
                //
                log.warn("RepairSendMarked, repair marked, setMailSendDone, " + noQueSendMarked + " -> " + noQueSendDone);
                return true;
            } else {
                // Реплика НЕ совпадает с последним письмом - ошибка
                log.error("RepairSendMarked, unable to repair marked, ws.replica.crc <> mail.replica.crc");
                return false;
            }
        }

        // Отметка noQueSendMarked сильно отстает от сервера noQueSendDone.
        // Это ошибка, не чинится
        log.error("RepairSendMarked, unable to repair marked, noQueSendMarked < noQueSendDone");
        return false;
    }

    static boolean equalLastSend(IMailer mailer, String box, IJdxQue que, long noQueSendDone) throws Exception {
        // Читаем С СЕРВЕРА информацию о реплике, которую последней отправили на сервер
        IReplicaInfo replicaInfoSrv = ((MailerHttp) mailer).getLastReplicaInfo(box);
        String crcSrvLast = replicaInfoSrv.getCrc();
        long noSrvLast = replicaInfoSrv.getNo();

        // Последний раз отправляли на сервер вовсе не номер noQueSendDone
        if (noSrvLast != noQueSendDone) {
            log.error("Last send replica no, last send no: " + noQueSendDone + ", que last replicaInfo.no: " + noSrvLast);
            return false;
        }

        // Берем ИЗ ОЧЕРЕДИ ту реплику, которую последней отправили на сервер
        IReplica replicaFromQue = que.get(noQueSendDone);

        // Сравниваем CRC реплик
        if (UtJdx.equalReplicaCrc(replicaFromQue, crcSrvLast)) {
            // Реплика совпадает с последним письмом - не считаем ситуацию аварийной.
            // Это бывает, если прервался цикл: отправка на сервер - отметка об отправке в БД,
            // успели отправить на сервер, но не успели отметить в базе.
            log.warn("Last replica already sent, que: " + que.getQueName() + ", no: " + noQueSendDone + ", box: " + box);
            //
            return true;
        } else {
            log.error("Last send replica crc <> ws.crc, ws.crc: " + replicaFromQue.getInfo().getCrc() + ", mail.replica.crc: " + crcSrvLast + ", que: " + que.getQueName() + ", no: " + noQueSendDone + ", box: " + box);
            //
            return false;
        }
    }

    /**
     * Читаем реплику номер no из ящика box.
     * Если она есть (осталась при штатной передаче, либо её запросили повторно на предыдущем цикле), то возвращаем ее,.
     * если реплики не оказалосьь в ящике - запрашиваем у executor.
     */
    public static IReplica receiveOrRequestReplica(IMailer mailer, String box, long no, String executor) throws Exception {
        try {
            // Читаем реплику из ящика
            IReplica replica = mailer.receive(box, no);

            // Читаем заголовок
            JdxReplicaReaderXml.readReplicaInfo(replica);

            //
            return replica;
        } catch (Exception exceptionMail) {
            // Какая-то ошибка
            if (UtJdxErrors.errorIs_replicaMailNotFound(exceptionMail)) {
                // Ошибка: реплики в ящике нет - запросм сами повторную передачу
                handleReplicaMailNotFound(mailer, box, no, executor, exceptionMail);
            }
            throw exceptionMail;
        }
    }

    /**
     * Запрашивает повторную передачу реплики
     */
    public static void handleReplicaMailNotFound(IMailer mailer, String box, long no, String executor, Exception exceptionMail) throws Exception {
        log.error("Replica not found: " + no + ", box: " + box + ", error: " + exceptionMail.getMessage());

        // Реплику номер 1 - не просить.
        // Это сделано, чтобы станция, добавленная в систему позже других,
        // НЕ запрашивала с сервера реплику, номер которой, скорее всего, будет НЕ 1, а больше.
        // При добавлении новой станции сервер сам все пришлет что нужно, отслеживать автоматически - не нужно,
        // в крайнем случае - проконтролировать вручную.
        if (no == 1) {
            log.info("Replica no == 1, no set required, just wait");
            return;
        }

        // Реплику еще не просили повторно?
        RequiredInfo requiredInfoNow = mailer.getSendRequired(box);
        if (requiredInfoNow.requiredFrom == -1 || requiredInfoNow.requiredFrom > no || (requiredInfoNow.requiredTo != -1 && requiredInfoNow.requiredTo < no)) {
            // Оказывается ещё не просили у executor-а прислать, или просили диапазон, который не включает в себя no -
            // попросим сейчас недостающий диапазон.
            log.info("Try set required, box: " + box + ", no: " + no + ", executor: " + executor);

            long requiredTo;
            if (requiredInfoNow.requiredTo != -1)
                requiredTo = Math.max(requiredInfoNow.requiredTo, no);
            else {
                requiredTo = no;
            }

            //
            RequiredInfo requiredInfo = new RequiredInfo();
            requiredInfo.executor = executor;
            requiredInfo.requiredFrom = no;
            requiredInfo.requiredTo = requiredTo;

            // Просим
            mailer.setSendRequired(box, requiredInfo);

            // Заказали и ждем пока executor пришлет, а пока - ошибка
            throw new XError("Set required done, wait for receive, box: " + box + ", send required: " + requiredInfo);
        } else {
            // Уже просили прислать - ждем пока executor пришлет, а пока - ошибка
            throw new XError("Wait for receive, box: " + box + ", wait required: " + requiredInfoNow);
        }
    }

    public static void checkMailServer(String mailUrl, String guid) throws Exception {
        // Короткая проверка
        if (guid == null || guid.length() == 0) {
            // Конфиг для мейлера
            JSONObject cfgMailer = new JSONObject();
            cfgMailer.put("url", mailUrl);
            cfgMailer.put("guid", "-");
            cfgMailer.put("localDirTmp", "temp/mailer");

            // Мейлер
            MailerHttp mailer = new MailerHttp();
            mailer.init(cfgMailer);

            //
            System.out.println("------------------------------");
            System.out.println("Check mail server...");
            try {
                JSONObject res = mailer.ping();
                if (!String.valueOf(res.get("result")).equalsIgnoreCase("ok")) {
                    System.out.println("ERROR: " + res.get("error"));
                } else {
                    System.out.println("OK");
                    //System.out.println(res.get("dt"));
                }
            } catch (Exception e) {
                System.out.println("ERROR: " + UtJdxErrors.collectExceptionText(e));
            }

            //
            return;
        }

        // Подробная проверка
        Random rnd = new Random();
        rnd.setSeed(new DateTime().getMillis());
        long wsIdRandom = 100 + rnd.nextInt(1000);

        // Конфиг для мейлера
        JSONObject cfgMailer = new JSONObject();
        String guidWs = guid + "/test_ws_" + wsIdRandom;
        cfgMailer.put("url", mailUrl);
        cfgMailer.put("guid", guidWs);
        cfgMailer.put("localDirTmp", "temp/mailer");

        // Мейлер
        MailerHttp mailer = new MailerHttp();
        mailer.init(cfgMailer);

        //
        System.out.println("------------------------------");
        System.out.println("Check mail server");
        JSONObject res = mailer.ping();
        System.out.println("OK");
        System.out.println(res.get("dt"));

        //
        String box = "test";
        mailer.createMailBox(box);
        System.out.println("createMailBox '" + box + "' - ok");

        //
        UtRepl utRepl = new UtRepl(null, new JdxDbStruct());
        IReplica replicaSend = utRepl.createReplicaMute(wsIdRandom);
        System.out.println("replicaSend.getInfo: " + replicaSend.getInfo().toJSONObject_withFileInfo());

        //
        mailer.send(replicaSend, box, 1);
        System.out.println("send - ok");

        //
        IReplica replicaReceive = mailer.receive(box, 1);
        System.out.println("receive - ok");

        //
        System.out.println("replicaReceive.getInfo: " + replicaReceive.getInfo().toJSONObject_withFileInfo());

        // Успешное обращение к getData ящика доказывает его нормальную работу
        System.out.println("files: " + mailer.getData("files", box));
        System.out.println("ping.read: " + mailer.getData("ping.read", box));
        System.out.println("ping.write: " + mailer.getData("ping.write", box));
        System.out.println("last.dat.info: " + mailer.getData("last.dat.info", box));
        System.out.println("last.read: " + mailer.getData("last.read", box));
        System.out.println("last.write: " + mailer.getData("last.write", box));
        System.out.println("required.info: " + mailer.getData("required.info", box));


        //
        mailer.delete(box, 1);
        System.out.println("delete - ok");

        //
        System.out.println("files: " + mailer.getData("files", box));
    }
}
