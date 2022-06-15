import org.junit.Test;
import parser.command.Use;

/**
 * @author HP
 * @date 2022/5/21
 */
public class UseTest {

    @Test
    public void useTest(){
        new Use("use Student1;").analysis();
    }
}
