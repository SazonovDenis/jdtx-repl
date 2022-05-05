package jdtx.repl.main.api.replica;

import org.joda.time.*;
import org.json.simple.*;

/**
 *
 */
public interface IReplicaInfo {

    /**
     * Станция-автор реплики
     */
    long getWsId();

    void setWsId(long wsId);

    /**
     * Возраст базы у автора реплики
     */
    long getAge();

    void setAge(long age);

    /**
     * Номер реплики у автора, монотонно растет для автора
     */
    long getNo();

    void setNo(long no);

    String getCrc();

    void setCrc(String crc);

    DateTime getDtFrom();

    void setDtFrom(DateTime dtFrom);

    DateTime getDtTo();

    void setDtTo(DateTime dtTo);

    int getReplicaType();

    void setReplicaType(int replicaType);

    String getDbStructCrc();

    void setDbStructCrc(String crc);

    /**/

    void assign(IReplicaInfo info);

    void fromJSONObject(JSONObject infoJson);

    JSONObject toJSONObject();

    String toJSONString();

}
