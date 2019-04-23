package jdtx.repl.main.api.replica;

import org.joda.time.*;

import java.io.*;

/**
 * Блок данных. todo: возможно нет необходитости хранить возраст (номер) ВНУТРИ реплики
 */
public interface IReplica {

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

    File getFile();

    void setFile(File file);

}
