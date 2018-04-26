import java.util.HashMap;
import java.util.Map;

public class dadsa {
    public static String filterNumber(String number)
    {
        number = number.replaceAll("[^(0-9)]", "");
        return number;
    }
    public static String filterNumbers(Map map)
    {
        map.put("a","b");
        map.put("c","b");
       return  "aaaa";
    }
    public static void main(String[] args) {
// String you = "^&^&^you123$%$%你好";
    String you = "^&^&^you123$%$%你好";
    Map map = new HashMap<>();
        System.out.println(map.size());
//    you = filterNumber(you);
//        System.out.println(you);
        filterNumbers(map);

        System.out.println(map.size());

    }
}
