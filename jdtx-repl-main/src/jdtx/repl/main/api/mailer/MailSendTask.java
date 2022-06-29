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
        }

        String s;
        if (sendTo > 0) {
            s = sendFrom + " .. " + sendTo;
        } else {
            s = sendFrom + " .. " + "all";
        }

        if (executor != null) {
            s = s + ", recreate: " + recreate + ", executor: " + executor;
        }

        return s;
    }

}
