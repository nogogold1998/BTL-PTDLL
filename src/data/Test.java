package data;

import java.util.*;

public class Test {
    public static void main(String[] args){
        Set<List<String>> a = new HashSet<List<String>>();

        a.add(new ArrayList<String>());
        List<String> l1= new ArrayList<>();
        l1.add("Hello");
        List<String> l2= new ArrayList<>();
        l2.add("Hello");
        l2.add("World");
        List<String> l3= new ArrayList<>();
        l3.add("Hello");
        l3.add("World");
        a.add(l1);
        a.add(l2);
        a.add(l3);
        for (List<String> b : a){
            System.out.println(b.toString());
        }
    }
}
