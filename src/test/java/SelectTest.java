import org.junit.Test;
import parser.command.Select;

/**
 * @author HP
 * @date 2022/5/23
 */
public class SelectTest {

    @Test
    public void testWinSelect(){
        new Select("select * from nuaa where x=[0.1,0.3],y=[0.2,0.4],z=[0.3,0.6],w=[0.2,0.3];").analysis();
    }

}
