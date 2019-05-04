package jdtx.repl.main.api.replica;

import org.joda.time.*;

/**
 */
public interface IReplicaInfo {

    long getWsId();

    void setWsId(long wsId);

    long getAge();

    void setAge(long age);

    DateTime getDtFrom();

    void setDtFrom(DateTime dtFrom);

    DateTime getDtTo();

    void setDtTo(DateTime dtTo);

    int getReplicaType();

    void setReplicaType(int replicaType);

    String getCrc();

    void setCrc(String crc);

}