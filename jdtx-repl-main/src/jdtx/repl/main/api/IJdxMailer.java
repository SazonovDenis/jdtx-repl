package jdtx.repl.main.api;

import org.joda.time.DateTime;
import org.json.simple.JSONObject;

import java.io.IOException;

/**
 * Отправляет реплики из исходящей очереди на сервер.
 * Забирает реплики с сервера во входящую очередь.
 */
public interface IJdxMailer {

    void init(JSONObject cfg);

    /**
     * @return Сколько есть на сервере (age или no) в папке box
     */
    long getSrvSate(String box) throws Exception;

    void send(IReplica repl, long n, String box) throws Exception;

    IReplica receive(long n, String box) throws Exception;

    void delete(long n, String box) throws Exception;

    void ping(String box) throws Exception;

    DateTime getPingDt(String box) throws Exception;
}
