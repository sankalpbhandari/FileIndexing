package utd.database.com;

import java.io.RandomAccessFile;



public class BPlusTree {
	Utility utility = Utility.getInstance();
	static Long currentPointer = null;
	static String tableName = "";

	public boolean writeLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			table.seek(pageStart);
			table.writeByte(1);
			table.writeByte(1);
			table.writeShort((int) (pageEnd - recordSize));
			table.writeInt(rightPointer);
			table.writeShort((int) (pageEnd - recordSize));
			currentPointer = Long.valueOf(pageEnd - recordSize);
			table.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public boolean updateLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			table.seek(pageStart);
			table.readByte();

			int cells = table.readByte() + 1;
			table.seek(pageStart + 1L);
			table.writeByte(cells);

			int oldCellAddress = table.readShort();
			int newCellAddress = oldCellAddress - recordSize;
			table.seek(pageStart + 2L);
			table.writeShort(newCellAddress);
			table.writeInt(rightPointer);

			short[] cellAddress = new short[cells];
			for (int i = 0; i < cells - 1; i++) {
				cellAddress[i] = table.readShort();
			}
			table.writeShort(newCellAddress);
			currentPointer = Long.valueOf(newCellAddress);
			table.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public boolean updateRightPointerOfLeafHeader(long pageStart, long pageEnd, int recordSize, int rightPointer) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			table.seek(pageStart);
			table.readByte();
			table.readByte();
			table.readShort();
			table.writeInt(rightPointer);
			table.close();
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public int getLastId(long pageStart) {
		try {
			int lastId = 0;
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			table.seek(pageStart + utility.getPageSize());
			table.readByte();
			table.readByte();
			int lastAddress = table.readShort();
			table.writeInt(0);
			table.seek(lastAddress);
			lastId = table.readInt();
			table.close();
			return lastId;
		} catch (Exception e) {
		}
		return 0;
	}

	public void checkInteriorPageOverflow(long pagePointer) {
		if (pagePointer != 0L) {
			try {
				RandomAccessFile table = new RandomAccessFile(tableName, "rw");
				int noOfPages = 0;
				table.readByte();
				table.readInt();
				table.readInt();
				table.readInt();
				long rightMostPointer = table.readInt();
				long parentPointer = table.readInt();
				long OverflowBucket = 0L;
				int id = 0;
				while (rightMostPointer != 0L) {
					noOfPages++;
					if (3 == noOfPages) {
						OverflowBucket = rightMostPointer;
						table.readByte();
						table.readInt();
						id = table.readInt();
						break;
					}
					table.seek(rightMostPointer);
					table.readByte();
					table.readInt();
					table.readInt();
					table.readInt();
					rightMostPointer = table.readInt();
				}
				if (OverflowBucket != 0L) {
					long newPageStart = utility.getPageSize() * (table.length() / utility.getPageSize() + 2L);
					long topParentPointer = 0L;
					if (parentPointer == 0L) {
						parentPointer = newPageStart;
					} else {
						table.seek(parentPointer);
						table.readByte();
						table.readInt();
						table.readInt();
						table.readInt();
						table.writeInt((int) newPageStart);

						topParentPointer = table.readInt();
					}

					table.seek(newPageStart);
					table.readByte();
					table.writeInt((int) pagePointer);
					table.writeInt(id);
					table.writeInt((int) OverflowBucket);
					table.writeInt((int) topParentPointer);

					table.seek(pagePointer);
					table.readByte();
					table.readInt();
					table.readInt();
					table.readInt();
					rightMostPointer = table.readInt();
					table.writeInt((int) parentPointer);

					table.seek(pagePointer + utility.getPageSize());
					table.readByte();
					table.readInt();
					table.readInt();
					table.writeInt(0);

					while (rightMostPointer != 0L) {
						table.seek(rightMostPointer);
						table.readByte();
						table.readInt();
						table.readInt();
						table.readInt();
						rightMostPointer = table.readInt();
						table.writeInt((int) parentPointer);
					}
					checkInteriorPageOverflow(parentPointer);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void checkOverflow() {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			long newPageStart = utility.getPageSize() * (table.length() / utility.getPageSize() + 2L);
			if (table.length() > utility.getPageSize() * 3L) {
				long pagePointer = -1L;
				while (table.getFilePointer() < table.length()) {
					int pageType = table.readByte();
					if (pageType == 0) {
						pagePointer = table.getFilePointer() - 1L;
						break;
					}
					table.seek(table.getFilePointer() + utility.getPageSize() - 1L);
				}

				if (pagePointer == -1L) {
					long leftPointer = 0L;
					int lastId = getLastId(0L);
					table.seek(newPageStart);
					table.writeByte(0);
					table.writeInt(0);
					table.writeInt(lastId + 1);
					table.writeInt((int) (leftPointer + utility.getPageSize() * 3L));
					table.writeInt(0);
					table.writeInt(0);
					table.close();
				} else {
					table.seek(pagePointer);
					int pageType = table.readByte();
					table.readInt();
					table.readInt();
					long rightPointer = table.readInt();
					long rightMostPointer = table.readInt();

					table.seek(rightPointer);
					pageType = table.readByte();
					while (pageType != 1) {
						table.readInt();
						table.readInt();
						rightPointer = table.readInt();
						rightMostPointer = table.readInt();
						table.seek(rightPointer);
						pageType = table.readByte();
					}

					while (rightMostPointer != 0L) {
						pagePointer = rightMostPointer;
						table.seek(rightMostPointer);
						pageType = table.readByte();
						table.readInt();
						table.readInt();
						rightPointer = table.readInt();
						rightMostPointer = table.readInt();
					}

					table.seek(rightPointer);
					int noOfPages = 1;
					long overflowBucket = 0L;
					long lastLeafPagePointer = rightPointer;
					table.readByte();
					table.readByte();
					table.readShort();
					rightMostPointer = table.readInt();
					while (rightMostPointer != 0L) {
						table.seek(rightMostPointer);
						noOfPages++;
						if (noOfPages == 4) {
							overflowBucket = rightMostPointer;
							break;
						}
						table.readByte();
						table.readByte();
						table.readShort();
						rightMostPointer = table.readInt();
					}

					if (overflowBucket != -1L) {
						table.seek(lastLeafPagePointer);
						table.readByte();
						table.readInt();
						table.readInt();
						rightPointer = table.readInt();
						table.writeInt((int) newPageStart);
						long parentPointer = table.readInt();

						table.seek(lastLeafPagePointer + utility.getPageSize());
						table.readByte();
						table.readInt();
						table.readInt();
						table.writeInt(0);

						int lastId = getLastId(lastLeafPagePointer);
						table.seek(newPageStart);
						table.writeByte(0);
						table.writeInt((int) rightPointer);
						table.writeInt(lastId + 1);
						table.writeInt((int) overflowBucket);
						table.writeInt(0);
						table.writeInt((int) parentPointer);

						checkInteriorPageOverflow(rightPointer);
					}
				}
				table.close();
			}
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long insert(int recordSize) {
		try {
			RandomAccessFile table = new RandomAccessFile(tableName, "rw");
			long filelength = table.length();
			long fileIndex = table.getFilePointer();
			long pageStart = 0L;
			long pageEnd = pageStart + utility.getPageSize() - 1L;
			if (filelength == 0L) {
				table.close();
				writeLeafHeader(pageStart, pageEnd, recordSize, 0);
				return currentPointer.longValue();
			}

			int pageType = table.readByte();
			int cells = table.readByte();
			int startPointer = table.readShort();
			int rightPointer = table.readInt();
			while (rightPointer != 0) {
				table.seek(rightPointer);
				pageType = table.readByte();
				cells = table.readByte();
				startPointer = table.readShort();
				rightPointer = table.readInt();
			}

			if ((pageType == 1) && (rightPointer == 0)) {
				fileIndex = table.getFilePointer();
				table.close();
				pageStart = fileIndex - 8L;
				pageEnd = pageStart + utility.getPageSize();
				cells++;
				if (startPointer - recordSize > pageStart + 8L + 2 * cells) {
					updateLeafHeader(pageStart, pageEnd, recordSize, rightPointer);
				} else {
					rightPointer = (int) ((filelength + 1L) / utility.getPageSize() * utility.getPageSize());
					updateRightPointerOfLeafHeader(pageStart, pageEnd, recordSize, rightPointer);
					writeLeafHeader(rightPointer, rightPointer + utility.getPageSize() - 1L, recordSize, 0);
					checkOverflow();
				}
			}
			table.close();
			return currentPointer.longValue();
		} catch (Exception e) {
		}
		return 0L;
	}
}
