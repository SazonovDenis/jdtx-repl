package jdtx.repl.main.api;

/**
 */
public class JdxDecoderSlot {

    long ws_id;
    long ws_slot_no;

    public int hashCode() {
        //
        return toString().hashCode();
    }

    public String toString() {
        //
        return ws_id + ":" + ws_slot_no;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof JdxDecoderSlot)) {
            return super.equals(obj);
        }
        //
        JdxDecoderSlot rd = (JdxDecoderSlot) obj;
        return this.ws_slot_no == rd.ws_slot_no && this.ws_id == rd.ws_id;
    }

}
