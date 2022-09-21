package jdtx.repl.main.api.settings;

import org.json.simple.*;

public interface IWsSettings {

    /**
     * Читаем код нашей станции
     */
    long getWsId() throws Exception;

    /**
     * Читаем guid нашей станции
     */
    String getWsGuid() throws Exception;

    /**
     * Записываем код и guid нашей станции
     */
    void setWsIdGuid(long wsId, String wsGuid) throws Exception;

    JSONObject getCfgDecode() throws Exception;

    void setCfgDecode(JSONObject cfg) throws Exception;

}
