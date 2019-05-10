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
     * Сколько есть на сервере (age или no) в папке box
     */
    long getSrvState(String box) throws Exception;

    /**
     * Отправка реплики
     */
    void send(IReplica repl, String box, long no) throws Exception;

    /**
     * Информация о письме (заголовок с возрастом реплики, её типом и т.п.)
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
     * Отметить попытку чтения
     */
    void pingRead(String box) throws Exception;

    /**
     * Отметить попытку записи
     */
    void pingWrite(String box) throws Exception;

    /**
     * Отметить данные сервера
     */
    void setSrvInfo(Map info) throws Exception;

    /**
     * Отметить данные рабочей станции
     */
    void setWsInfo(Map info) throws Exception;

}
