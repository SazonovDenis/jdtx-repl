package jdtx.repl.main.ut;

import java.util.*;

/**
 */
public interface IListener {

    void onEventInfo(Map event);

    Map onEventHandle(Map event);

}
