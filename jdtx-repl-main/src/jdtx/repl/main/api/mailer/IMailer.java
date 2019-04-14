package jdtx.repl.main.api.mailer;

import jdtx.repl.main.api.replica.*;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;

/**
 * Отправляет реплики из исходящей очереди на сервер.
 * Забирает реплики с сервера во входящую очередь.
 */
public interface IMailer {

    void init(JSONObject cfg);

    /**
     * @return Сколько есть на сервере (age или no) в папке box
     */
    long getSrvSate(String box) throws Exception;

    void send(IReplica repl, long no, String box) throws Exception;

    IReplica receive(long no, String box) throws Exception;

    ReplicaInfo getInfo(long no, String box) throws Exception;

    void delete(long no, String box) throws Exception;

    void ping(String box) throws Exception;

    DateTime getPingDt(String box) throws Exception;

}
