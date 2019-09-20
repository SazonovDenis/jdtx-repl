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
    long getSrvState(String box) throws Exception;

    /**
     * Информация о желаемом состоянии почтового ящика (используется для запроса повторной отправки реплик)
     *
     * @return Начиная с какого письма (age или no) требуется отправить письма в папку box
     */
    long getSendRequired(String box) throws Exception;

    /**
     * Сбросить информацию о желаемом состоянии почтового ящика
     */
    void setSendRequired(String box, long required) throws Exception;

    /**
     * Отправка реплики
     */
    void send(IReplica repl, String box, long no) throws Exception;

    /**
     * Информация о письме (реплике)
     *
     * @return заголовок с возрастом реплики, её типом, размером, crc и т.п.
     */
    ReplicaInfo getReplicaInfo(String box, long no) throws Exception;

    /**
     * Получение реплики
     */
    IReplica receive(String box, long no) throws Exception;

    /**
     * Удалить письмо из ящика
     */
    void delete(String box, long no) throws Exception;

    /**
     * Отметить попытку чтения (для отслеживания активности станции, когда нет данных для реальной передачи)
     */
    void pingRead(String box) throws Exception;

    /**
     * Отметить попытку записи (для отслеживания активности станции, когда нет данных для реальной передачи)
     */
    void pingWrite(String box) throws Exception;

    /**
     * Отметить (сообщить) данные сервера (для отслеживания состояния сервера)
     */
    void setSrvInfo(Map info) throws Exception;

    /**
     * Отметить (сообщить) данные о рабочей станции (станция отчитывается о себе - для отслеживания состояния станции)
     */
    void setWsInfo(Map info) throws Exception;

}
