package jdtx.repl.main.api.decoder;

/**
 * Расширенный id - Пара: код рабочей станции + id
 */
public class JdxRef {

    public long ws_id = -1;
    public long id = -1;

    public static JdxRef parse(String val) {
        JdxRef ref = new JdxRef();

        String[] ref_arr = val.split(":");
        if (ref_arr.length == 1) {
            ref.id = Long.valueOf(ref_arr[0]);
        } else {
            ref.ws_id = Long.valueOf(ref_arr[0]);
            ref.id = Long.valueOf(ref_arr[1]);
        }

        return ref;
    }

    public String toString() {
        if (ws_id == -1) {
            return String.valueOf(id);
        } else {
            return ws_id + ":" + id;
        }
    }

    @Override
    public boolean equals(Object val) {
        if (!(val instanceof JdxRef)) {
            return false;
        }

        JdxRef ref = (JdxRef) val;
        return ref.ws_id == this.ws_id && ref.id == this.id;
    }

}
