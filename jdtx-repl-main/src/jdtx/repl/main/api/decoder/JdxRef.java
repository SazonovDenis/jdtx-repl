package jdtx.repl.main.api.decoder;

/**
 * Значение типа "ссылка" в виде пары: код рабочей станции + значение ссылки
 */
public class JdxRef {

    public long ws_id = -1;
    public long value = -1;

    public static JdxRef parse(String val) {
        if (val == null || val.length() == 0) {
            return null;
        }

        JdxRef ref = new JdxRef();

        String[] ref_arr = val.split(":");
        if (ref_arr.length == 1) {
            ref.value = Long.valueOf(ref_arr[0]);
        } else {
            ref.ws_id = Long.valueOf(ref_arr[0]);
            ref.value = Long.valueOf(ref_arr[1]);
        }

        return ref;
    }

    public String toString() {
        if (ws_id == -1) {
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
