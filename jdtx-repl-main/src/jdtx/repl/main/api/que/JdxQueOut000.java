package jdtx.repl.main.api.que;

import jandcode.dbm.db.*;
import org.apache.commons.logging.*;

/**
 * Исходящая очередь queOut000 на сервере для КАЖДОЙ рабочей станции,
 * В эти очереди распределяем queCommon.
 * <p>
 * Важно - JdxQueOut000 особенная - одна физическая таблица содержит реплики на несколько станций, каждая станция - независимо
 * <p>
 * todo Рефакторинг JdxCommon vs JdxQueOut000 vs JdxQueOut001
 */
public class JdxQueOut000 extends JdxQueOut001 {

    //
    protected static Log log = LogFactory.getLog("jdtx.JdxQueOut000");

    //
    public JdxQueOut000(Db db, long wsId) {
        super(db, UtQue.QUE_OUT000, UtQue.STATE_AT_SRV);
        this.wsId = wsId;
    }


    /*
     * IJdxReplicaQue
     */


}
