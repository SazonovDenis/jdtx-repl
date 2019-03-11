package jdtx.repl.main.api;

import java.io.*;

/**
 * Блок данных. todo: возможно нет необходитости хранить возраст и номер ВНУТРИ реплики
 */
public interface IReplica {

    void setWsId(long wsId);

    long getWsId();

    void setAge(long age);

    long getAge();

    void setReplicaType(int replicaType);

    int getReplicaType();

    void setFile(File file);

    File getFile();

}
