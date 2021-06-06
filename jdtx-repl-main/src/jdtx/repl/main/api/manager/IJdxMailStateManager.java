package jdtx.repl.main.api.manager;

public interface IJdxMailStateManager {

    long getMailSendDone() throws Exception;

    void setMailSendDone(long sendDone) throws Exception;

}
