package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.ntnu.laika.Constants;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class DPDocMapping {
	
	private byte[] map;
	
	public DPDocMapping(String path, int numnodes){
		map = new byte[Constants.MAX_NUMBER_OF_DOCUMENTS];
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line; int i=0;
			
			while ((line = br.readLine())!=null){
				map[i++] = (byte)(Integer.parseInt(line)-1);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public int getNode(int docID){
		return map[docID];
	}
	
	public static void main(String args[]){
		new DPDocMapping("/home/simonj/workstuff/2012/Enver/partvec_doc/partvec_doc.8.HP", 8);
	}
}
