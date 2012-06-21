package com.ntnu.laika.utils;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class BitVector{
	public byte[] data;
	
	public BitVector(byte ... data){
		this.data = data;
	}
	
	public BitVector(int length){
		data = new byte[(length>>3) + ((length&7)!=0?1:0)];
	}
	
	public BitVector(int length, int ... IDs){
		data =  new byte[(length>>3) + ((length&7)!=0?1:0)];
		for (int i : IDs) data[i>>3] |= 1<<(i&7);	
	}
	
	public void setBit(int pos){
		data[pos>>3] |= 1<<(pos&7); 
	}
	
	public boolean checkBit(int pos){
		return (data[pos>>3] & 1<<(pos&7)) != 0;
	}
	
	public static int getBitCount(byte[] data){
		int cnt=0;
		byte tmp;
		for (int i=0; i<data.length; i++){
			tmp=data[i];
			
			if ((tmp&1) != 0) cnt++;
			if ((tmp&2) != 0) cnt++;
			if ((tmp&4) != 0) cnt++;
			if ((tmp&8) != 0) cnt++;
			
			if ((tmp&16) != 0) cnt++;
			if ((tmp&32) != 0) cnt++;
			if ((tmp&64) != 0) cnt++;
			if ((tmp&128) != 0) cnt++;
		}
		return cnt;				
	}
	
	
	public int getBitCount(){
		int cnt=0;
		byte tmp;
		for (int i=0; i<data.length; i++){
			tmp=data[i];
			
			if ((tmp&1) != 0) cnt++;
			if ((tmp&2) != 0) cnt++;
			if ((tmp&4) != 0) cnt++;
			if ((tmp&8) != 0) cnt++;
			
			if ((tmp&16) != 0) cnt++;
			if ((tmp&32) != 0) cnt++;
			if ((tmp&64) != 0) cnt++;
			if ((tmp&128) != 0) cnt++;
		}
		return cnt;				
	}
	public byte[] getBytes(){
		return data;
	}
	
	public int[] getIDs(){
		int cnt = getBitCount(data);
		int values[] = new int[cnt];
		int _cnt = 0;
		for (int i=0; i<data.length; i++){
			for (int j=0; j<8; j++) {
				if ((data[i] & 1<<j) != 0) values[_cnt++] = (i<<3)+j;
			}
		}
		return values;
	}
	
	public static void main(String[] args){
        int NODES = 8;
		BitVector bv = new BitVector(NODES, 0);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 1);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 2);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 3);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 4);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 5);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 6);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 7);
		System.out.println(bv.getIDs().length + " " + bv.getIDs()[0]);
		bv = new BitVector(NODES, 0, 1, 2, 3, 4, 5, 6, 7);
		System.out.println(bv.getIDs().length + " ");
		for (int i=0; i<8; i++) System.out.println("->"+bv.getIDs()[i]);
	}
}