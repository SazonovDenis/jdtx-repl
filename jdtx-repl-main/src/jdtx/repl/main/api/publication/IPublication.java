package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

/**
 * Публикация - набор правил подготовки реплик.
 * Публикация управляет подготовкой данных как наверх, так и вниз.
 * В общем виде правило - это просто запрос к аудиту
 * и left join к соответствующей таблице с данными, с наложением фильтров.
 */
public interface IPublication {

    void loadRules(JSONObject cfg, IJdxDbStruct baseStruct) throws Exception;

    IJdxDbStruct getData();

}
