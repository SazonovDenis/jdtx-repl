package jdtx.repl.main.api;

import org.json.simple.*;

import java.io.IOException;

/**
 * Отправляет реплики из исходящей очереди на сервер.
 * Забирает реплики с сервера во входящую очередь.
 */
public interface IJdxMailer {

    void init(JSONObject cfg);

    /**
     * @return Сколько уже отправлено на сервер (age или no)
     */
    long getSrvSend() throws Exception;

    void send(IReplica repl, long n) throws Exception;

    /**
     * @return Сколько есть на сервере (age или no)
     */
    long getSrvReceive() throws Exception;

    IReplica receive(long n) throws Exception;

}
