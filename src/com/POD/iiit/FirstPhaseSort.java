package com.POD.iiit;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;



public class FirstPhaseSort {

	public static HashMap<String,String> attr_type;
	public static HashMap<String,Integer> attr_pos;
	public static List<String> attr_sort_cols;
	public static String filename;
	public static long no_of_records;
	public static List<String> lines;
	public static FileReaderClass myReader;
	public static double no_of_sorted_lists;
	public static String temp_file;
	public static long no_of_records_in_main_memory;	
	private static int length;
	public static int sort_order;
	
	FirstPhaseSort(HashMap<String,String> attr_type_args,HashMap<String,Integer> attr_pos_args,List<String> attr_sort_cols_args,String fileName_args,long no_of_records_args,double no_of_sorted_lists_args,long no_of_records_in_main_memory_args,int sort_order_args)
	{
		attr_type = attr_type_args;
		attr_pos = attr_pos_args;
		attr_sort_cols = attr_sort_cols_args;
		filename = fileName_args;
		no_of_records = no_of_records_args;
		lines = new ArrayList();
		no_of_sorted_lists= no_of_sorted_lists_args;
		temp_file = "inter_part";
		no_of_records_in_main_memory = no_of_records_in_main_memory_args;
		sort_order=sort_order_args;
	}
	
	public void collectRecords()
	{
		
		myReader = new FileReaderClass(filename);
		myReader.openFile();
		long total_sorted_sublists = (long)no_of_sorted_lists;
		if(total_sorted_sublists==0)
			total_sorted_sublists=1;
		
		for(int i=0;i<total_sorted_sublists;i++)
		{
			readNLines();
			sortAndWrite(temp_file+i);
			lines.clear();
		}
		myReader.closeFile();
		System.out.println("Things successfully done");
				
	}
	
	public static void sortAndWrite(String interFileName)
	{
		if(sort_order==0)
		{
			//ascending order sort
			Collections.sort(lines,new AscendingSort(attr_type,attr_pos,attr_sort_cols));
		}
		else
		{
			Collections.sort(lines,new DescendingSort(attr_type,attr_pos,attr_sort_cols));
		}
		
		FileWriterClass obj = new FileWriterClass(interFileName);
		obj.createFile();
		
		for(int i=0;i<lines.size();i++)
		{
			obj.writeline(lines.get(i));
		}
		obj.closeFile();
	}
	
	public void readNLines()
	{
		try
		{
			String line=myReader.buff_reader.readLine();
			int counter = 0;
			
			while(line!=null)
			{
				lines.add(counter,line);
				counter++;
				if(counter<no_of_records_in_main_memory)
					line = myReader.buff_reader.readLine();
				else
					break;
			}
		}
		catch(IOException e)
		{
			System.out.println("IOException in In Read N lines function class:FirstPhaseSort");
		}
		
		System.out.println("Number of lines read are "+lines.size());
		
	}
	

 }
	
	

