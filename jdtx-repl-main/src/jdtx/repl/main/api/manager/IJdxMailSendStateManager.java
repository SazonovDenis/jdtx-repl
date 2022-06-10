package jdtx.repl.main.api.manager;

/**
 * Состояние почты: отметка, насколько отправлена почта.
 * Реализовано в виде интерфейса (а не в виде конкретной реализации) из за унивесального метода
 * jdtx.repl.main.api.util.UtMail#sendQueToMail, там удобно отметки делать через абстрактный отметчик.
 */
public interface IJdxMailSendStateManager {

    long getMailSendDone() throws Exception;

    void setMailSendDone(long sendDone) throws Exception;

}
