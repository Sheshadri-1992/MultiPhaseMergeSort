package com.POD.iiit;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class AscendingSort implements Comparator<String> {

	public static HashMap<String,String> attr_type;
	public static HashMap<String,Integer> attr_pos;
	public static List<String> attr_sort_cols;
	
	AscendingSort(HashMap<String,String> attr_type_args,HashMap<String,Integer> attr_pos_args,List<String> attr_sort_cols_args)
	{
		attr_type= attr_type_args;
		attr_pos= attr_pos_args;
		attr_sort_cols=attr_sort_cols_args;
		System.out.println("IN comparator class "+attr_type.toString());
	}
	
	
	@Override
	public int compare(String string1, String string2) {
		// TODO Auto-generated method stub
		for(int i=0;i<attr_sort_cols.size();i++)
		{
			String colname = attr_sort_cols.get(i);
			String colDataType = attr_type.get(colname);
			int position = attr_pos.get(colname);
			
			
//			System.out.println("Column data type is " + colDataType);
			//need to receive a negative, positive or 0
			//if 1st is lesser than 2nd negative
			//if 1st is greater than 2nd positive
			//if first is equal to second then 0
			int result = compareAs(colDataType,position,string1,string2);
			
			if(result==0)
			{
				//sort on the next column
			}
			else
			{
				return result;
			}
		}

		return 0;
	}
	
	public static int compareAs(String colDataType,int position,String string1,String string2){

		String[] array1 = string1.split(",");
		String[] array2 = string2.split(",");
		
		String op1 = array1[position];
		String op2 = array2[position];
		
		if(colDataType.contains("char")||colDataType.contains("CHAR"))
		{
			//do this (compare as two characters)
			
			op1=op1.toLowerCase();
			op2=op2.toLowerCase();
			
//			System.out.println("Comparing "+op1+" and "+op2);
			return op1.compareTo(op2);
			
		}
		else if(colDataType.contains("int") || colDataType.contains("INT"))
		{
			//do this (compare as two integers)
			
			long int1 = Long.parseLong(op1);
			long int2 = Long.parseLong(op2);
			if(int1<int2)
				return -1;
			else if(int1>int2)
				return 1;
		}
		else
		{
			//compare two dates
			try
			{
				DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
				Date date1 = df.parse(op1);
				Date date2 = df.parse(op2);
				
				return date1.compareTo(date2);
			}
			catch(ParseException e)
			{
				System.out.println("Date format exception compareAs Function class:FirstPhaseSort");
			}
			
		}
		return 0;
	}


}
