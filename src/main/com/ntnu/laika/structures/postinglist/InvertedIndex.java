package com.ntnu.laika.structures.postinglist;

import com.ntnu.laika.structures.lexicon.LexiconEntry;
import com.ntnu.laika.structures.lexicon.ShortLexiconEntry;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public interface InvertedIndex {
	public PostingListIterator getPostingListIterator(LexiconEntry lEntry);
	public PostingListIterator getPostingListIterator(ShortLexiconEntry lEntry);
}
