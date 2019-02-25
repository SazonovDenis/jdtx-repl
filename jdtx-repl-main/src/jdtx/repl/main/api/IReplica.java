package jdtx.repl.main.api;

import java.io.*;

/**
 * Блок данных
 */
public interface IReplica {

    void setWsId(long wsId);

    long getWsId();

    void setAge(long age);

    long getAge();

    void setNo(long age);

    long getNo();

    void setFile(File file);

    File getFile();

}
