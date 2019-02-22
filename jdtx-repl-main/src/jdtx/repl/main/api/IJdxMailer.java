package jdtx.repl.main.api;

import org.json.simple.*;

/**
 * Отправляет реплики из исходящей очереди на сервер.
 * Забирает реплики с сервера во входящую очередь.
 */
public interface IJdxMailer {

    void send() throws Exception;

    void receive() throws Exception;

    void init(JSONObject cfg);

}
