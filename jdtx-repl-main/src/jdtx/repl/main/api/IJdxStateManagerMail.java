package jdtx.repl.main.api;

public interface IJdxStateManagerMail {

    long getMailSendDone() throws Exception;

    void setMailSendDone(long sendDone) throws Exception;

}
