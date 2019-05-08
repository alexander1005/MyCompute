import java.util.HashMap;
import java.util.Map;

public class test3 {


    public static Map<String,String> map = new HashMap<>();

    public static void main(String[] args) {

        test3.map.putIfAbsent("123","123");
        test3 test3 = new test3();

        //System.out.println(test3.map);
    }
}
