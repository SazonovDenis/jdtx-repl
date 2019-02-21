package jdtx.repl.main.api;

import org.json.simple.*;

import java.io.*;

/**
 * Публикация - набор правил подготовки реплик.
 * Публикация позволяет готовить данные как наверх, так и вниз.
 * В общем виде правило - это просто запрос к аудиту
 * и left join к соответствующей таблице с данными, с наложением фильтров.
 */
public interface IPublication {

    void loadRules(Reader r) throws Exception;

    JSONArray getData();

    void setData(JSONArray data);

}
