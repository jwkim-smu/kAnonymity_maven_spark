package kts;
import java.util.ArrayList;

public class test1{
	public static void main(String args[]) {
		ArrayList<ArrayList<String>> kts = new ArrayList<ArrayList<String>>();
		ArrayList<String> kts2 = new ArrayList<String>();
		kts2.add("kim");
		kts.add(kts2);
		System.out.println(kts.get(0).get(0));
	}
}