package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

public interface IPublicationRuleStorage {

    /**
     * Заполняет правила публикации с учетом наличия таблиц в структуре struct
     */
    void loadRules(JSONObject cfg, IJdxDbStruct struct) throws Exception;

    Collection <IPublicationRule> getPublicationRules();

    IPublicationRule getPublicationRule(String tableName);

}
