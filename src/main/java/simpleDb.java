import java.util.Scanner;
import parser.Parser;


/**
 * @author HP
 * @date 2022/5/19
 */
public class simpleDb {

    public static void main(String[] args) {
        new simpleDb().start();
    }

    public void start() {
        System.out.println("-----------------------------");
        System.out.println("|                            |");
        System.out.println("-----------------------------");
        System.out.println("|     Welcome to SimpleDB    |");
        System.out.println("-----------------------------");
        System.out.println("|                            |");
        System.out.println("-----------------------------");

        Scanner reader = new Scanner(System.in);

        StringBuilder buffer = new StringBuilder();
        String line;
        boolean quit = false;
        while (!quit) {
            System.out.print("SimpleDB> ");
            line = reader.nextLine();
            while (line.indexOf(';') >= 0) {
                int split = line.indexOf(';');
                buffer.append(line, 0, split + 1);
                String cmd = buffer.toString().trim();
                cmd = cmd.substring(0, cmd.length() - 1).trim() + ";";
                if (cmd.equalsIgnoreCase("quit;")
                        || cmd.equalsIgnoreCase("exit;")) {
                    shutdown();
                    quit = true;
                    break;
                }

                long startTime = System.currentTimeMillis();
                processNextStatement(cmd);
                long time = System.currentTimeMillis() - startTime;
                System.out.printf("----------------\n%.2f seconds\n\n",
                        ((double) time / 1000.0));


                line = line.substring(split + 1);
                buffer = new StringBuilder();
            }
            if (line.length() > 0) {
                buffer.append(line);
                buffer.append("\n");
            }
        }


    }

    protected void shutdown() {
        System.out.println("Bye");
    }

    private void processNextStatement(String cmd) {
        new Parser().allocate(cmd);
    }

}
