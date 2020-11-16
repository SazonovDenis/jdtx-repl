package jdtx.repl.main.api.publication;

import jdtx.repl.main.api.struct.*;
import org.json.simple.*;

import java.io.*;
import java.util.*;

public interface IPublicationStorage {

    void loadRules(JSONObject cfg, IJdxDbStruct baseStruct) throws Exception;

    Collection <IPublicationRule> getPublicationRules();

    IPublicationRule getPublicationRule(String tableName);

}
