package com.ntnu.laika.distributed.tp.queryprocessing;

import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public interface TPQueryState extends Closeable{
	 public TPResultState processQuery();
}
