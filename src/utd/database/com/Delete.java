package utd.database.com;

import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * 
 * @author nikitakothari
 *
 *         Delete entry from table
 */

public class Delete {

	Utility utility = Utility.getInstance();

	public void delete(String userCommand) {
		try {
			String[] tokens = userCommand.split(" ");
			String tableName = tokens[3].trim();
			if (utility.isTablePresent(tableName, true)) {
				String filter = userCommand.substring(userCommand.indexOf("where") + 5, userCommand.length()).trim();
				String[] filterArray = filter.split("=");
				RandomAccessFile table = new RandomAccessFile(utility.getSeletedDatabase() + "." + tableName + ".tbl",
						"rw");

				if (table.length() > 0L) {
					java.util.List<Column> columns = utility.getColumns(tableName);
					long startPointer = table.getFilePointer();
					table.readByte();
					int cells = table.readByte();
					table.readShort();
					long rightPointer = table.readInt();
					ArrayList<Short> cellPointers = new ArrayList<Short>();
					for (int i = 0; i < cells; i++) {
						cellPointers.add(Short.valueOf(table.readShort()));
					}
					boolean nextPage = true;
					while (nextPage) {
						ArrayList<Short> keep = new ArrayList<Short>();
						for (int i = 0; i < cellPointers.size(); i++) {
							table.seek(((Short) cellPointers.get(i)).shortValue());
							boolean isRemove = false;
							for (Column column : columns) {
								if (column.getDataType().equals("int")) {
									String value = "" + table.readInt();
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("tinyint")) {
									String value = "" + table.readByte();
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("smallint")) {
									String value = "" + table.readShort();
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("bigint")) {
									String value = "" + table.readLong();
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("real")) {
									String value = "" + table.readFloat();
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("double")) {
									String value = "" + table.readDouble();
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("date")) {
									String value = utility.convertDateToString(table.readLong());
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else if (column.getDataType().equals("datetime")) {
									String value = utility.convertDateTimeToString(table.readLong());
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								} else {
									int length = table.readByte();
									byte[] bytes = new byte[length];
									table.read(bytes, 0, bytes.length);
									String value = " " + new String(bytes);
									if ((column.getColumnName().equals(filterArray[0]))
											&& (value.equals(filterArray[1]))) {
										isRemove = true;
									}
								}
							}

							if (!isRemove)
								keep.add((Short) cellPointers.get(i));
						}
						int noOfRemovedElement = cellPointers.size() - keep.size();
						if (noOfRemovedElement > 0) {
							table.seek(startPointer);
							table.readByte();
							table.writeByte(keep.size());
							table.readShort();
							table.readInt();
							for (int i = 0; i < keep.size(); i++) {
								table.writeShort(((Short) keep.get(i)).shortValue());
							}
							for (int i = 0; i < noOfRemovedElement; i++) {
								table.writeShort(0);
							}
						}
						if (rightPointer != 0L) {
							table.seek(rightPointer);
							startPointer = rightPointer;
							table.readByte();
							cells = table.readByte();
							table.readShort();
							rightPointer = table.readInt();
							cellPointers = new ArrayList<Short>();
							for (int i = 0; i < cells; i++) {
								cellPointers.add(Short.valueOf(table.readShort()));
							}
						} else {
							nextPage = false;
						}
					}
					System.out.println("Record is deleted Successfully");
				} else {
					System.out.println("No record present");
				}
				table.close();
			}
		} catch (Exception e) {
			System.out.println("Error, whiel deleting a record");
		}
	}
}
