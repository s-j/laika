package com.ntnu.laika.distributed.dp.queryprocessing;

import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public interface DPQueryState extends Closeable{
	 public DPResultState processQuery();
}
