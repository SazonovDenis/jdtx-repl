package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

public interface IPublicationStorage {

    /**
     * Заполняет правила публикации с учетом наличия таблиц в структуре structActual
     */
    void loadRules(JSONObject cfg, IJdxDbStruct structActual) throws Exception;

    Collection <IPublicationRule> getPublicationRules();

    IPublicationRule getPublicationRule(String tableName);

}
