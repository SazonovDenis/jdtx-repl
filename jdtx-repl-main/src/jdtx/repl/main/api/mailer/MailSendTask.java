package jdtx.repl.main.api.mailer;

public class MailSendTask {

    public long sendFrom = -1;
    public long sendTo = -1;
    public boolean required = false;
    public boolean recreate = false;
    public String executor = null;

    @Override
    public String toString() {
        if (sendFrom == -1) {
            return "no task";
        } else if (sendFrom > sendTo) {
            return "empty";
        } else if (sendTo > 0) {
            return "send: " + sendFrom + " .. " + sendTo + ", recreate: " + recreate + ", executor: " + executor;
        } else {
            return "send: " + sendFrom + " .. " + "all" + ", recreate: " + recreate + ", executor: " + executor;
        }
    }

}
