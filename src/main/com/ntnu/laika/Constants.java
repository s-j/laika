package com.ntnu.laika;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public final class Constants {
    public static final boolean DEBUG = true;

    public static int BUFFER_BLOCK_SIZE = 16384;
    public static final int TOTAL_BUFFER_BLOCK_SIZE = 16384 * 1000;

    public static final int BYTE_SIZE = 1;
    public static final int CHAR_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int FLOAT_SIZE = 4;
    public static final int DOUBLE_SIZE = 8;
    
    public static int STRING_BYTE_LENGTH = 20;
    public static int DOCNO_BYTE_LENGTH = 20;
    public static int MAX_NUMBER_OF_DOCUMENTS = 25500000;

	public static int MAX_QUERY_LENGTH = 25;
	public static int MAX_NUMBER_OF_RESULTS = 100;
	public static boolean USE_SKIPS = true;
	
	public static int WORKERS_CNT = 8;
}