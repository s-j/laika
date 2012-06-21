package com.ntnu.lpsolver;

import java.util.Comparator;
/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class Edge{
	protected int x, y;
	protected double val;
	
	public Edge(int x, int y, double val) {
		this.x = x;
		this.y = y;
		this.val = val;
	}
	
	protected static Comparator<Edge> ORDER = new Comparator<Edge>() {
		public int compare(Edge b, Edge c) {
			double bval= (b.x == b.y)? b.val: b.val/2;
			double cval= (c.x == c.y)? c.val: c.val/2;
			return (bval > cval)? 1: -1;
		}
	};
}