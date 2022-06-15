import org.junit.Test;
import parser.command.Create;
/**
 * @author HP
 * @date 2022/5/21
 */
public class CreateTest {

    @Test
    public void DatabaseTest(){
        new Create("create database Student1;").createDatabase();
        new Create("create database Student2;").createDatabase();
        new Create("create database Student3;").createDatabase();
    }
}
