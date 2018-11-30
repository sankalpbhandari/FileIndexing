package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class Utility 
{
	private static Utility utility = new Utility();
	private final String PROMPT = "DavisBaseLite>";
	private final String VERSION = "v1.0.0";
	private final String COPY_RIGHT = "©2018 Divyesh Meet Sankalp Shraddha";
	private final String DEFAULT_DATABASE = null;
	private final String DEFAULT_TABLE_COLUMN = null;
	private final long DEFAULT_PAGE_SIZE = 512L;
	
	public String selectedDatabase;

	private Utility() 
	{
		
	}

	public static Utility getInstance()
	{
		return utility;
	}

	public static String displayLine(String displayChar, int num)
	{
		String displayString = "";
		for (int i = 0; i < num; i++)
		{
			displayString += displayChar ;
		}
		return displayString;
	}

	public static void splashScreen()
	{
		System.out.println(displayLine("-", 80));
		System.out.println("Welcome to DavisBaseLite");
		System.out.println("DavisBaseLite Version " + getInstance().getVersion());
		System.out.println(getInstance().getCopyRight());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(displayLine("-", 80));
	}

	public List<Column> getColumns(String tableName)
	{
		List<Column> columns = new java.util.ArrayList<Column>();
		try
		{
			if(utility.isTablePresent(tableName, true))
			{
				RandomAccessFile table = new RandomAccessFile(utility.getSeletedDatabase() + File.separator + "columns.tbl", "rw");
				while (table.getFilePointer() < table.length()) 
				{
					int isDeleted = table.readByte();
					byte length = table.readByte();
					byte[] bytes = new byte[length];
					table.read(bytes, 0, bytes.length);
					String[] column = new String(bytes).replaceAll("#", " ").split(" ");
					if((column[0].equals(tableName)) && (isDeleted == 0))
					{
						Column c = new Column();
						c.setColumnName(column[1]);
						c.setDataType(column[2]);
						c.setPrimary(false);
						c.setNotNullable(false);
						if (column.length == 4)
						{
							if(column[3].equals("primarykey"))
							{
								c.setPrimary(true);
							} 
							else if(column[3].equals("notnullable"))
							{
								c.setNotNullable(true);
							}
						}
						columns.add(c);
					}
				}
				table.close();
			}
		} 
		catch (Exception e)
		{
			System.out.println("Error");
		}

		return columns;
	}

	public void markAllColumnsDeleted(String tableName) {
		try {
			if (utility.isTablePresent(tableName, true)) {
				RandomAccessFile table = new RandomAccessFile(utility.getSeletedDatabase() + ".columns.tbl", "rw");
				while (table.getFilePointer() < table.length()) {
					int isDeleted = table.readByte();
					byte length = table.readByte();
					byte[] bytes = new byte[length];
					table.read(bytes, 0, bytes.length);
					String[] column = new String(bytes).replaceAll("#", " ").split(" ");
					if ((column[0].equals(tableName)) && (isDeleted == 0)) {
						long tablePointer = table.getFilePointer();
						table.seek(tablePointer - length - 1L);
						table.writeByte(1);
						table.seek(tablePointer);
					}
				}
				table.close();
			}
		} catch (Exception e) {
			System.out.println("Error");
		}
	}

	public String getPrompt() {
		return PROMPT;
	}

	public String getVersion() {
		return VERSION;
	}

	public String getCopyRight() {
		return COPY_RIGHT;
	}
	
	public long getPageSize()
	{
		return this.DEFAULT_PAGE_SIZE;
	}

	public String getDefaultDatabase() {
		return DEFAULT_DATABASE;
	}

	public String getDefaultTableColumn() {
		return DEFAULT_TABLE_COLUMN;
	}

	public String getSeletedDatabase() {
		return IUtitlityConstants.DATABASE_PATH + selectedDatabase;
	}

	
	public void setSeletedDatabase(String seletedDatabase) {
		this.selectedDatabase = seletedDatabase;
	}

	public boolean isTablePresent(String tableName, boolean showMessage) {
		try {
			File file = new File(utility.getSeletedDatabase() + "." + tableName + ".tbl");
			if ((file.exists()) && (!file.isDirectory()))
				return true;
			if (showMessage) {
				System.out.println("Table " + tableName + " is not present");
			}
		} catch (Exception e) {
			return false;
		}

		return false;
	}

	public long convertStringToDate(String dateString) {
		String pattern = "MM:dd:yyyy";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		try {
			Date date = format.parse(dateString);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Date().getTime();
	}

	public String convertDateToString(long date) {
		String pattern = "MM:dd:yyyy";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date d = new Date(date);
		return format.format(d);
	}

	public String convertDateTimeToString(long date) {
		String pattern = "YYYY-MM-DD_hh:mm:ss";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date d = new Date(date);
		return format.format(d);
	}
	
	public boolean isDatabaseExist(String dbName)
	{
		File dBFile = new File("data" + File.separator + dbName);
		if(dBFile.exists())
			return true;
		else
			return false;
	}
	public String getDATABASE_PATH() {
		return IUtitlityConstants.DATABASE_PATH;
	}
}
