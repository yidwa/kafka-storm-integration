package calculating;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import clojure.main;

public class HMapSortByvalues {

	HashMap<String, Integer> hmap;
	
	public HMapSortByvalues() {
		// TODO Auto-generated constructor stub
		this.hmap = new HashMap<>();
	}
	
	public static HashMap sortByValues(HashMap map){
	    List list = new LinkedList(map.entrySet());
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	               return ((Comparable) ((Map.Entry) (o1)).getValue())
	                  .compareTo(((Map.Entry) (o2)).getValue());
	            }
	       });
	       HashMap sortedHashMap = new LinkedHashMap();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	              Map.Entry entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       return sortedHashMap;
	  }
	
	
	public static void main(String[] args) {
		 HashMap<String,Integer> hmap = new  HashMap<String,Integer>();
	      hmap.put("D", 321);
	      hmap.put("E", 5208);
	      hmap.put("F", 5641);
	      hmap.put("G", 3067);
	      hmap.put("A", 5403);
	      hmap.put("B", 859);
	      System.out.println("Before Sorting:");
	      Set set = hmap.entrySet();
	      Iterator iterator = set.iterator();
	      while(iterator.hasNext()) {
	           Map.Entry me = (Map.Entry)iterator.next();
	           System.out.print(me.getKey() + ": ");
	           System.out.println(me.getValue());
	      }
	      Map<Integer, String> map = sortByValues(hmap); 
	      System.out.println("After Sorting:");
	      Set set2 = map.entrySet();
	      Iterator iterator2 = set2.iterator();
	      while(iterator2.hasNext()) {
	           Map.Entry me2 = (Map.Entry)iterator2.next();
	           System.out.print(me2.getKey() + ": ");
	           System.out.println(me2.getValue());
	      }
	}
}
