package com.POD.iiit;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class SecondPhaseSort {

	public static HashMap<String,String> attr_type;
	public static HashMap<String,Integer> attr_pos;
	public static List<String> attr_sort_cols;
	public static double blockSize;
	public static double no_of_sorted_lists;
	public static String temp_file;
	public static FileReaderClass[] fr;
	public static FileWriterClass fw;
	public static boolean outputBlockFilled;
	public static List<ArrayList<String>> stringBlocks;
	public static List<String> outputBlock;
	public static long[] block_indices;
	public static long output_index;
	public static File outputFile;
	public static FileWriter outputWriter;
	public static List<String> attr_output_cols;
	public static int[] output_positions;
	
	SecondPhaseSort(double blockSize_args,HashMap<String,String> attr_type_args,HashMap<String,Integer> attr_pos_args,List<String> attr_sort_cols_args,List<String> attr_output_cols_args,String fileName_args,double no_of_sorted_lists_args)
	{
		blockSize=Math.ceil(blockSize_args);
		attr_type = attr_type_args;
		attr_pos = attr_pos_args;
		attr_sort_cols = attr_sort_cols_args;
		no_of_sorted_lists=no_of_sorted_lists_args;
		attr_output_cols= attr_output_cols_args;
		temp_file="inter_part";		
		fr = new FileReaderClass[(int)no_of_sorted_lists];
		block_indices = new long[(int)no_of_sorted_lists];
		stringBlocks = new ArrayList<ArrayList<String>>();
		outputBlock = new ArrayList<String>();
		output_positions= new int[(int)attr_output_cols.size()];
		
	}
	
	public void createOutputFile()
	{
		for(int i=0;i<(int)attr_output_cols.size();i++)
		{
			output_positions[i] =attr_pos.get(attr_output_cols.get(i)).intValue();
		}
		
		
		try
		{
			outputFile = new File("output.txt");
			outputFile.createNewFile();
			outputWriter = new FileWriter(outputFile);
		}
		catch(IOException e)
		{
			System.out.println("IOException in createOutputFile Class:SecondPhaseSort");
		}		
	}
	
	public static void closeOutputFile()
	{
		try
		{
			outputWriter.close();
		}
		catch(IOException e)
		{
			System.out.println("IOException in closeOutputFile Class:SecondPhaseSort");
		}
	}
	
	public void openNFiles()//this can possibly fail
	{
		
		for(int i=0;i<no_of_sorted_lists;i++)
		{
			fr[i] = new FileReaderClass(temp_file+i);
			fr[i].openFile();
		}
	}
	
	public void fillInitialBlocks()
	{
		System.out.println("Block size is "+(int)blockSize);
		System.out.println("No of sorted sublists are "+no_of_sorted_lists);
		
		for(int i=0;i<no_of_sorted_lists;i++)
		{
			int j=0;
			ArrayList<String> myString = new ArrayList<String>();
			try
			{
				String line = fr[i].buff_reader.readLine();
				while(line!=null)
				{
					
					myString.add(j, line);
					j++;
					if(j<(int)blockSize)
					{
						line= fr[i].buff_reader.readLine();
					}
					else
					{
						break;
					}
					
					
				}
				stringBlocks.add(i, myString);
				System.out.println("The index j is " +j+" total number of records "+stringBlocks.get(i).size());
			
				
			}
			catch(IOException e)
			{
				System.out.println("IO Exception in fillInitialBlocks of SecondPhaseSort");
			}
		}
	}
	
	public void mergeLists(int order_args)
	{
		System.out.println("No of sorted sublists are "+no_of_sorted_lists+" Sorted order is "+order_args); 
		String minRecord=null;
		int minIndex=-1;
		String compRecord=null;
		int compIndex=-1;
		int order=order_args; //0 means ascending
		int result=0;
		
		for(int i=0;i<no_of_sorted_lists;i++)
		{
			block_indices[i]=0;
		}
		
		while(filesActive())
		{
			minRecord=null;
			minIndex=-1;
			
			for(int i=0;i<no_of_sorted_lists;i++)
			{
				/** Fetch one from each sublist to compare
				 * whichever is the smallest add that to the output block
				 * once everything is added, flush  it to the output file
				 */	 			
//				System.out.println("Index that is going out of range block "+i+" its index manager "+block_indices[i]+" but its actual size is "+stringBlocks.get(i).size());
				if(fr[i].active==true)
				{
					compRecord = stringBlocks.get(i).get((int)block_indices[i]);
					compIndex = i;
				}
				else
				{
//					System.out.println("File"+i+" is exhausted");
					compRecord=null;
					compIndex=-1;
				}	
				
				
				if(minRecord==null && compRecord!=null)
				{
					minRecord=compRecord;
					minIndex = compIndex;
				}
				else if(minRecord==null && compRecord==null)
				{
					//skip
					System.out.println("SKIP ");

				}
				else if(minRecord!=null && compRecord!=null)
				{
//					System.out.println("Does controlcome here? ");

					result = compareTwoRecords(minRecord,compRecord);
//					System.out.println("Does controlcome here? ");

					if(order==0)//for ascending order
					{
//						System.out.println("Does controlcome here? ");

						if(result>0)
						{
							minRecord=compRecord;
							minIndex=compIndex;
							
						}
					}
					else if(order==1)//for descending orde
					{
//						System.out.println("Does controlcome here? ");
//						System.out.println("String1 is "+minRecord +" String2 is "+compRecord);
						if(result<0)
						{
							minRecord=compRecord;
							minIndex=compIndex;
						}
					}
				}
			}//end of for loop
			
			if(minIndex==-1 || minRecord==null)
			{
				//do nothing
			}
			else
			{
				block_indices[minIndex]=block_indices[minIndex]+1;
				outputBlock.add((int)output_index, minRecord);
				output_index++;
				if(block_indices[minIndex]==(int)blockSize || block_indices[minIndex]==stringBlocks.get(minIndex).size())
				{
//					System.out.println("Should come here block, the block is "+minIndex);
					fillRecordsIntoBuffer(minIndex);
				}
				
				if(output_index==(long)blockSize)
				{
//					System.out.println("Should come here output");
					//write to output file
					writeToOutputFile();
				}
			}
		}
		
		writeToOutputFile();
		closeOutputFile();
		
	}
	
	
	public static void writeToOutputFile()
	{
		try
		{	
			String line;
			
			for(int i=0;i<outputBlock.size();i++)
			{
				line=outputBlock.get(i);
				String[] array = line.split(",");
				
				String newline="";
				int k;
				for(k=0;k<output_positions.length-1;k++)
				{
					newline = newline.concat(array[output_positions[k]]);
					newline = newline.concat(",");
				}
				newline = newline.concat(array[output_positions[k]]);
				newline = newline.concat("\n");
				outputWriter.write(newline);
				
//				outputWriter.write("\n");
			}
			output_index=0;
			outputBlock.clear();
		}
		catch(IOException e)
		{
			System.out.println("IOException in writeToOutputFile class:SecondPhaseSort");
		}
		
	}
	
	
	/**Fill records into the buffer **/
	public static void fillRecordsIntoBuffer(long minIndex)
	{
		
		try
		{
//			System.out.println("Fill more records the block to be filled is "+minIndex);
			ArrayList<String> myString = new ArrayList<String>();
			String line = fr[(int)minIndex].buff_reader.readLine();
			
			if(line==null)
			{
				fr[(int)minIndex].active=false;
				System.out.println("File" + minIndex +" is complete");
			}
			else
			{
				stringBlocks.get((int)minIndex).clear();//since its full
				
				long counter=0;
				while(line!=null)
				{
					myString.add((int)counter, line);
					counter++;

					if(counter<(int)blockSize)
					{
						line = fr[(int)minIndex].buff_reader.readLine();
					}
					else
					{
						break;
					}
				}
				
				
				stringBlocks.remove((int)minIndex);
				stringBlocks.add((int)minIndex, myString);
				block_indices[(int)minIndex]=0;
				
				
//				System.out.println("Block "+minIndex+" was filled with "+stringBlocks.get((int)minIndex).size()+" records");
//				System.out.println("Size of block1 is "+stringBlocks.get(1).size()+" records");
			}
		}
		
		catch(IOException e)
		{
			System.out.println("IOException in fillRecordsIntoBuffer class:SecondPhaseSort");
		}
//		System.out.println("Exiting this fillblocks function");
				
	}
	
	
	/**compare two records to say which of them is minimum **/
	public static int compareTwoRecords(String minRecord_args,String compRecord_args)
	{
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
			int result = compareAs(colDataType,position,minRecord_args,compRecord_args);
			
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
			
			int int1 = Integer.parseInt(op1);
			int int2 = Integer.parseInt(op2);
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
	
	
	public static boolean filesActive()
	{
		boolean result=fr[0].active;
		for(int i=0;i<no_of_sorted_lists;i++)
		{
			result = result || fr[i].active;
		}
		return result;
	}
	
	
	public void closeNFiles()
	{
		for(int i=0;i<no_of_sorted_lists;i++)
		{
			fr[i].closeFile();
		}	
	}
	
}