package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.ntnu.laika.structures.lexicon.global.GlobalLexiconEntry;
import com.ntnu.laika.utils.BitVector;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class LexMapping {
	
	private HashMap<String, BitVector> map = new HashMap<String, BitVector>();
	
	public LexMapping(String path){
		final int NODES = GlobalLexiconEntry.MAX_NODES_SUPPORTED;
		BitVector bv0   = new BitVector(NODES, 0);
		BitVector bv1   = new BitVector(NODES, 1);
		BitVector bv2   = new BitVector(NODES, 2);
		BitVector bv3   = new BitVector(NODES, 3);
		BitVector bv4   = new BitVector(NODES, 4);
		BitVector bv5   = new BitVector(NODES, 5);
		BitVector bv6   = new BitVector(NODES, 6);
		BitVector bv7   = new BitVector(NODES, 7);
		BitVector bvall = new BitVector(NODES, 0, 1, 2, 3, 4, 5, 6, 7);
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line, tmp[];
			while ((line = br.readLine())!=null){
				tmp = line.split(" ");
				switch(Integer.parseInt(tmp[1])){
					case   1: map.put(tmp[0], bv0  ); break;
					case   2: map.put(tmp[0], bv1  ); break;
					case   3: map.put(tmp[0], bv2  ); break;
					case   4: map.put(tmp[0], bv3  ); break;
					case   5: map.put(tmp[0], bv4  ); break;
					case   6: map.put(tmp[0], bv5  ); break;
					case   7: map.put(tmp[0], bv6  ); break;
					case   8: map.put(tmp[0], bv7  ); break;
					default : map.put(tmp[0], bvall);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public BitVector getBitVector(String term){
		return map.get(term);
	}
	
}
