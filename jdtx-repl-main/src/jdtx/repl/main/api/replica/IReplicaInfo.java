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
     * Возраст аудита у автора реплики, для которого сформирована реплика
     */
    long getAge();

    void setAge(long age);

    /**
     * Номер реплики в исходящей очереди у автора, монотонно растет для автора
     */
    long getNo();

    void setNo(long no);

    /**
     * Контрольная сумма файла реплики
     */
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

    void assign(IReplicaInfo info);

    void fromJSONObject(JSONObject infoJson);

    JSONObject toJSONObject_withFileInfo();

    String toJSONString_noFileInfo();

}
