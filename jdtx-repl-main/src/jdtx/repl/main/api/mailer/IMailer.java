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
     * @return Сколько писем есть на сервере (age или no) в папке box
     */
    long getBoxState(String box) throws Exception;

    /**
     * Информация о фактическом состоянии почтового ящика (используется для анализа при восстановлении из бэкапа)
     *
     * @return Какой возраст (age или no) уже есть на сервере в папке box
     */
    long getSendDone(String box) throws Exception;

    /**
     * Информация о желаемом состоянии почтового ящика (используется для запроса повторной отправки реплик)
     *
     * @return Начиная с какого письма (age или no) требуется отправить письма в папку box
     */
    SendRequiredInfo getSendRequired(String box) throws Exception;

    /**
     * Сбросить информацию о желаемом состоянии почтового ящика
     */
    void setSendRequired(String box, SendRequiredInfo requiredInfo) throws Exception;

    /**
     * Отправка реплики
     */
    void send(IReplica repl, String box, long no) throws Exception;

    /**
     * Информация о реплике (письме)
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
    void delete(String box, long no) throws Exception;


    /**
     * Отметить (сообщить) произвольные данные (для отслеживания состояния, ошибок и т.п.)
     */
    void setData(Map data, String name, String box) throws Exception;

    /**
     * Прочитать произвольные данные
     */
    JSONObject getData(String name, String box) throws Exception;

}
