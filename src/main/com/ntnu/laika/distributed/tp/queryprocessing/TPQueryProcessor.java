package com.ntnu.laika.distributed.tp.queryprocessing;

import com.ntnu.laika.distributed.tp.TPQueryBundle;
import com.ntnu.laika.utils.Closeable;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public interface TPQueryProcessor extends Closeable{
	public void processQuery(TPQueryBundle bundle);
}
