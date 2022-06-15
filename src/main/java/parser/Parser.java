package parser;

import parser.command.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author HP
 * @date 2022/5/20
 */
public class Parser {

    private final String create = "create";
    private final String drop = "drop";
    private final String show = "show";
    private final String use = "use";
    private final String insert = "insert";
    private final String select = "select";
    private final String delete = "delete";


    public void allocate(String cmd){
        String start = "";
        //正则表达式的匹配规则
        String regex = "^[a-z]+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(cmd);
        //获取匹配值
        while(matcher.find()){
            start = matcher.group();
        }

        switch (start){
            case show:
                new Show(cmd).analysis();
                break;
            case create:
                new Create(cmd).analysis();
                break;
            case drop:
                new Drop(cmd).analysis();
                break;
            case use:
                new Use(cmd).analysis();
                break;
            case select:
                new Select(cmd).analysis();
                break;
            case delete:
                new ComDelete(cmd).analysis();
                break;
            case insert:
                new ComInsert(cmd).analysis();
                break;
            default:
                System.out.println("语法错误或命令不支持！");
                break;
        }
    }
}
