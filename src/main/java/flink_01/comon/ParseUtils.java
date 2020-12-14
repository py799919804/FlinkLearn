package flink_01.comon;

import java.util.*;

public class ParseUtils {


    public static List<Map<String,Integer>> parseHttpList(String[] httpArrays){
        ArrayList<Map<String, Integer>> httpList = new ArrayList<>();
        for(String httpPost :httpArrays ){
            String[] hsotAndPost = httpPost.split(":");
            HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();
            stringIntegerHashMap.put(hsotAndPost[0], Integer.parseInt(hsotAndPost[1]));
            httpList.add(stringIntegerHashMap);
        }
        return httpList;
    }

    public static void main(String[] args) throws Exception {
        ResourceBundle configResource = ResourceBundle.getBundle("config");
        System.out.println(configResource.getString("es.url"));
    }

}
