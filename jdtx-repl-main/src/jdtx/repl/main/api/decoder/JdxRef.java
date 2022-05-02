package jdtx.repl.main.api.decoder;

/**
 * Значение типа "ссылка".
 * Реализовано в виде пары: код рабочей станции + значение ссылки
 */
public class JdxRef {

    private static long EMPTY_VALUE = Long.MAX_VALUE;

    public long ws_id = EMPTY_VALUE;
    public long value = -1;

    public JdxRef() {
        super();
    }

    public JdxRef(long ws_id, long value) {
        super();
        this.ws_id = ws_id;
        this.value = value;
    }

    public boolean isEmptyWs() {
        return this.ws_id == EMPTY_VALUE;
    }

    public static JdxRef parse(String val) {
        if (val == null || val.length() == 0 || val.compareToIgnoreCase("null") == 0) {
            return null;
        }

        JdxRef ref = new JdxRef();

        String[] ref_arr = val.split(":");
        if (ref_arr.length == 1) {
            ref.value = Long.parseLong(ref_arr[0]);
        } else {
            ref.ws_id = Long.parseLong(ref_arr[0]);
            ref.value = Long.parseLong(ref_arr[1]);
        }

        return ref;
    }

    @Override
    public String toString() {
        if (isEmptyWs()) {
            return String.valueOf(value);
        } else {
            return ws_id + ":" + value;
        }
    }

    @Override
    public boolean equals(Object val) {
        if (!(val instanceof JdxRef)) {
            return false;
        }

        JdxRef ref = (JdxRef) val;
        return ref.ws_id == this.ws_id && ref.value == this.value;
    }

}
