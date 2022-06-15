import org.junit.Test;
import parser.command.Show;

/**
 * @author HP
 * @date 2022/5/21
 */
public class ShowTest {

    @Test
    public void DatabaseTest(){
        new Show("show database;").analysis();
    }
}
