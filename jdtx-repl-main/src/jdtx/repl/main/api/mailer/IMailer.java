package jdtx.repl.main.api.mailer;

import jdtx.repl.main.api.replica.*;
import org.json.simple.*;

import java.util.*;

/**
 * Отправляет реплики из исходящей очереди на сервер.
 * Забирает реплики с сервера во входящую очередь.
 */
public interface IMailer {

    void init(JSONObject cfg);

    /**
     * Информация о состоянии почтового ящика
     *
     * @return Сколько писем есть на сервере (no) в папке box
     */
    long getBoxState(String box) throws Exception;

    /**
     * Информация о фактическом состоянии почтового ящика (используется для анализа при восстановлении из бэкапа)
     *
     * @return Какой номер уже есть (ранее отправлен) на сервере в папке box
     */
    long getSendDone(String box) throws Exception;

    /**
     * Информация о фактическом состоянии почтового ящика (используется для анализа при восстановлении из бэкапа)
     *
     * @return Какой возраст (no) нами прочитан с сервера в папке box
     */
    long getReceiveDone(String box) throws Exception;

    /**
     * Информация о желаемом состоянии почтового ящика (используется для запроса повторной отправки реплик)
     *
     * @return Начиная с какого письма (no) требуется отправить письма в папку box
     */
    RequiredInfo getSendRequired(String box) throws Exception;

    /**
     * Сбросить информацию о желаемом состоянии почтового ящика
     */
    void setSendRequired(String box, RequiredInfo requiredInfo) throws Exception;

    /**
     * Отправка реплики
     */
    void send(IReplica repl, String box, long no) throws Exception;

    /**
     * Информация о реплике (письме) с указанным номером
     *
     * @return заголовок с возрастом реплики, её типом, размером, crc и т.п.
     */
    IReplicaInfo getReplicaInfo(String box, long no) throws Exception;

    /**
     * Получение реплики (письма)
     */
    IReplica receive(String box, long no) throws Exception;

    /**
     * Удалить реплику (письмо) из ящика
     */
    long delete(String box, long no) throws Exception;


    /**
     * Удалить реплики (письма) из ящика до номера no включительно
     */
    long deleteAll(String box, long no) throws Exception;


    /**
     * Записать на сервер произвольные данные (для отслеживания состояния, ошибок и т.п.)
     */
    void setData(Map<String, Object> data, String name, String box) throws Exception;

    /**
     * Прочитать произвольные данные
     */
    JSONObject getData(String name, String box) throws Exception;

}
