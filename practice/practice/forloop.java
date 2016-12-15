package practice;
//import java.util.*;

public class forloop {

	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		int numbers[] = {1, 3, 2, 4, 5};
		String str =  "Hello There";//new String("Hello there");
		
		str += " this worked";
		
		System.out.println(str);
		
		for(Object key : numbers)
		{
			System.out.println(key);
		}
		
		return;
	}

}
