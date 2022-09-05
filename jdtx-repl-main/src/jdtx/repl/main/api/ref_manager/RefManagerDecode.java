package jdtx.repl.main.api.ref_manager;


import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;

import java.util.*;

/**
 * Реализация IRefManager, если разведение pk основано на перекодировке, как в PS
 * <p>
 * Работает на допущении, что до определенного id - записи принадлежат этой рабочей станции,
 * а выше некоторого id - это места для перекодированных записей с других станций.
 * <p>
 * Чужие id подвергаются перекодировке и занимают диапазоны по слотам, по мере поступления записей.
 * <p>
 * todo: Есть вариант обезопасить изменение политики диапазанов: в таблице decode сохранить также и реальные значения id_min id_max для диапазонов, а не только номера диапазонов
 * Кроме того, тогда можно будет НЕ ОГРАНИЧИВАТЬ максимальную id от станции - просто для крупных ID
 * будет больше номер диапазона, а его истинный id?????.
 */
public class RefManagerDecode extends RefManagerService implements IRefManager {

    // Своими записи считаются значения id в диапазоне от 0 до 100 000 000
    public static long SLOT_SIZE = 1000000;
    static long SLOT_START_NUMBER = 100;  //todo: политика назначения диапазонов


    // Код нашей рабочей станции
    long self_ws_id = -1;

    // Слоты
    protected Map<String, Map<RefDecoderSlot, Long>> wsToSlotList;
    protected Map<String, Map<Long, RefDecoderSlot>> slotToWsList;

    // Стратегии перекодировки
    RefDecodeStrategy decodeStrategy = null;


    // ------------------------------------------
    // RefManagerService
    // ------------------------------------------

    @Override
    public void init(Db db, JdxReplWs ws) throws Exception {
        super.init(db, ws);

        // Код станции wsId
        initWs(ws.getWsId());

        // Слоты из БД
        initSlots();

        // Стратегии перекодировки
        initStrategy(ws.getCfgDecode());
    }

    @Override
    public IJdxDataSerializer getJdxDataSerializer() throws Exception {
        checkInit();
        return new JdxDataSerializerDecode(db, ws.getWsId());
    }


    // ------------------------------------------
    // IRefManager
    // ------------------------------------------

    public JdxRef get_ref(String tableName, long id_local) throws Exception {
        checkInit();

        //
        tableName = tableName.toUpperCase();

        //
        JdxRef ref = new JdxRef();

        // По правилам - надо перекодировать эту локальную id?
        if (!needDecodeStrategy(tableName, id_local)) {
            ref.value = id_local;
            return ref;
        }

        // Это наша собственная id?
        if (id_local <= get_max_own_id()) {
            ref.ws_id = this.self_ws_id;
            ref.value = id_local;
            return ref;
        }

        // Берем слоты для таблицы
        Map<Long, RefDecoderSlot> slotToWs = findOrAdd1(slotToWsList, tableName);

        // Номер нашего слота для id_local
        long own_slot_no = id_local / SLOT_SIZE;

        // Ищем наш слот
        RefDecoderSlot sl = slotToWs.get(own_slot_no);
        if (sl == null) {
            throw new XError("IRefManager.get_ref: trying to decode id, than was not inserted, table: " + tableName + ", ws_id: " + this.self_ws_id + ", id: " + id_local);
        }

        // По нашему номеру слота определяем ws_id и ws_slot_no
        long ws_slot_no = sl.ws_slot_no;
        long id = ws_slot_no * SLOT_SIZE + id_local % SLOT_SIZE;

        //
        ref.ws_id = sl.ws_id;
        ref.value = id;
        return ref;
    }

    public long get_id_local(String tableName, JdxRef ref) throws Exception {
        checkInit();

        //
        tableName = tableName.toUpperCase();

        // Не надо перекодировать?

        // Собственные id - не перекодируем
        if (ref.ws_id == this.self_ws_id) {
            return ref.value;
        }

        // Стратегии перекодировки запрещают - не пререкодируем
        if (!needDecodeStrategy(tableName, ref.value)) {
            return ref.value;
        }

        // Нужна перекодировка, а рабочая станция не указана
        if (ref.ws_id <= 0) {
            throw new XError("IRefManager.get_id_own: id is need to decode, but ws_id is invalid, table: " + tableName + ", ws_id: " + ref.ws_id + ", id: " + ref.value);
        }

        // Пробуем перекодировку чужой id имеющемися слотами
        long own_id = find_id_by_slot(tableName, ref);
        if (own_id != -1) {
            return own_id;
        }

        // ---
        // Добавляем слоты

        // Берем слоты для таблицы
        Map<Long, RefDecoderSlot> slotToWs = findOrAdd1(slotToWsList, tableName);
        Map<RefDecoderSlot, Long> wsToSlot = findOrAdd2(wsToSlotList, tableName);

        // Создаем новый слот
        RefDecoderSlot sl = new RefDecoderSlot();
        sl.ws_id = ref.ws_id;
        sl.ws_slot_no = ref.value / SLOT_SIZE;

        // Создаем новый слот
        long own_slot_no = calcNextOwnSlotNo(tableName);

        // Записываем новый слот в свои кэши
        wsToSlot.put(sl, own_slot_no);
        slotToWs.put(own_slot_no, sl);

        // Записываем новый слот в БД
        writeSlotToDb(tableName, sl.ws_id, sl.ws_slot_no, own_slot_no);


        // ---
        // Перекодировка чужой id через слот
        own_id = own_slot_no * SLOT_SIZE + ref.value % SLOT_SIZE;


        // ---
        return own_id;
    }


    // ------------------------------------------
    //
    // ------------------------------------------

    protected void initWs(long wsId) {
        if (wsId <= 0) {
            throw new XError("invalid wsId <= 0, wsId: " + wsId);
        }
        this.self_ws_id = wsId;
    }

    protected void initSlots() throws Exception {
        // Слоты
        slotToWsList = new HashMap<>();
        wsToSlotList = new HashMap<>();

        // Загрузим все слоты
        DataStore st = db.loadSql("select * from " + UtJdx.SYS_TABLE_PREFIX + "decode");
        for (DataRecord rec : st) {
            // Берем слоты для таблицы
            String tableName = rec.getValueString("table_name");
            tableName = tableName.toUpperCase();

            // Создем слот
            RefDecoderSlot sl = new RefDecoderSlot();
            sl.ws_id = rec.getValueLong("ws_id");
            sl.ws_slot_no = rec.getValueLong("ws_slot");

            //
            long own_slot_no = rec.getValueLong("own_slot");

            // Добавляем слот в наборы
            Map<Long, RefDecoderSlot> slotToWs = findOrAdd1(slotToWsList, tableName);
            slotToWs.put(own_slot_no, sl);
            //
            Map<RefDecoderSlot, Long> wsToSlot = findOrAdd2(wsToSlotList, tableName);
            wsToSlot.put(sl, own_slot_no);
        }
    }

    protected void initStrategy(JSONObject cfgDecode) throws Exception {
        decodeStrategy = new RefDecodeStrategy();
        decodeStrategy.init(cfgDecode);
    }


    //////////////////////
    //////////////////////
    //////////////////////
    // todo: необходимость public метода говорит об архитектурной проблеме
    //////////////////////
    //////////////////////
    //////////////////////
    public static long get_max_own_id() {
        return SLOT_SIZE * SLOT_START_NUMBER - 1;
    }

    /**
     * @return Нужно ли перекодировать по стратегии перекодировки
     */
    private boolean needDecodeStrategy(String tableName, long db_id) {
        checkInit();
        return decodeStrategy.needDecodeOwn(tableName, db_id);
    }

    private void checkInit() {
        if (decodeStrategy == null) {
            throw new XError("RefManagerDecode: service RefManagerService not initialized");
        }
    }

    private Map<Long, RefDecoderSlot> findOrAdd1(Map<String, Map<Long, RefDecoderSlot>> slotToWsList, String tableName) {
        Map<Long, RefDecoderSlot> slotToWs = slotToWsList.get(tableName);
        if (slotToWs == null) {
            slotToWs = new HashMap<>();
            slotToWsList.put(tableName, slotToWs);
        }
        return slotToWs;
    }

    private Map<RefDecoderSlot, Long> findOrAdd2(Map<String, Map<RefDecoderSlot, Long>> wsToSlotList, String tableName) {
        Map<RefDecoderSlot, Long> wsToSlot = wsToSlotList.get(tableName);
        if (wsToSlot == null) {
            wsToSlot = new HashMap<>();
            wsToSlotList.put(tableName, wsToSlot);
        }
        return wsToSlot;
    }


    /**
     * Перекодировка id через слоты, загруженные в кэше
     */
    private long find_id_by_slot(String tableName, JdxRef ref) throws Exception {
        // Берем слоты для таблицы
        Map<RefDecoderSlot, Long> wsToSlot = findOrAdd2(wsToSlotList, tableName);

        //
        RefDecoderSlot sl = new RefDecoderSlot();
        sl.ws_id = ref.ws_id;
        sl.ws_slot_no = ref.value / SLOT_SIZE;

        // Ищем наш слот
        Long own_slot_no = wsToSlot.get(sl);
        if (own_slot_no == null) {
            // Слот в кэше не найден?
            return -1;
        }

        // Перекодировка через слот
        return own_slot_no * SLOT_SIZE + ref.value % SLOT_SIZE;
    }

    // Записываем новый слот в БД
    private void writeSlotToDb(String tableName, long ws_id, long ws_slot_no, long own_slot_no) throws Exception {
        Map params = UtCnv.toMap(
                "table_name", tableName,
                "ws_id", ws_id,
                "ws_slot", ws_slot_no,
                "own_slot", own_slot_no
        );
        String sql = "insert into " + UtJdx.SYS_TABLE_PREFIX + "decode (table_name, ws_id, ws_slot, own_slot) values (:table_name, :ws_id, :ws_slot, :own_slot)";
        db.execSql(sql, params);
    }


    private long calcNextOwnSlotNo(String tableName) throws Exception {
        DataRecord rec = db.loadSql("select max(own_slot) as own_slot_max, count(*) as cnt from " + UtJdx.SYS_TABLE_PREFIX + "decode where table_name = '" + tableName + "'").getCurRec();
        long own_slot_max;
        if (rec.getValueLong("cnt") == 0) {
            own_slot_max = SLOT_START_NUMBER;
        } else {
            own_slot_max = rec.getValueLong("own_slot_max");
        }

        //
        return own_slot_max + 1;
    }


}
