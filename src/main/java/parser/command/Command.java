package parser.command;

/**
 * @author HP
 * @date 2022/5/20
 * 传过来的命令有分号，这里去掉了
 */
public abstract class Command {
    String cmd;
    String[] cmdWords;
    public Command(String cmd) {
        this.cmd = cmd;
        cmdWords = cmd.substring(0,cmd.indexOf(";")).trim().split("\\s+");
    }

    public abstract void analysis();
}
