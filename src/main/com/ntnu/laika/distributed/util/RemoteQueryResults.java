package com.ntnu.laika.distributed.util;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.ntnu.laika.Constants;
import com.ntnu.laika.query.QueryResults;
import com.ntnu.laika.query.processing.ResultHeap;
import com.ntnu.laika.utils.Pair;
import com.ntnu.laika.utils.Triple;
import com.ntnu.network.ApplicationHandler;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 * @version $Id $.
 */
public class RemoteQueryResults {

	public static Pair<Integer, QueryResults> fromChannelBufferAdvanced(ChannelBuffer buffer){
		int qid = buffer.readInt();
		int cnt = buffer.readInt();
		int realCnt = buffer.readInt();
		byte compression = buffer.readByte();
		ByteBuffer bbuffer = buffer.toByteBuffer();
		return new Pair<Integer, QueryResults>(qid, ResultSetCompression.fromByteBuffer(bbuffer, cnt, realCnt, compression));
	}
	
	public static Triple<Integer, QueryResults, Long> fromChannelBuffer(ChannelBuffer buffer){
		int qid = buffer.readInt();
		int cnt = buffer.readInt();
		int realCnt = buffer.readInt();
		int docids[] = new int[cnt];
		double scores[] = new double[cnt];
		for (int i = 0; i<cnt; i++){
			docids[i] = buffer.readInt();
			scores[i] = buffer.readFloat();
		}
		long processingTime = buffer.readLong();
		return new Triple<Integer, QueryResults, Long>(qid, new QueryResults(docids, scores, cnt, realCnt), processingTime);
	}
	
	public static ChannelBuffer toChannelBufferAdvanced(int qid, ResultHeap rheap, int realCnt){
		int cnt = rheap.size();
		
		byte compression = ResultSetCompression.MAXCOMPRESSION;
		LinkedByteBuffer tmpbuffer = ResultSetCompression.toByteBuffer(rheap, compression);
		
		ChannelBuffer buffer = ChannelBuffers.buffer(3 * Constants.INT_SIZE + 2 + tmpbuffer.getSize());
		
		buffer.writeByte(ApplicationHandler.RESULTS);
		buffer.writeInt(qid);
		buffer.writeInt(cnt);
		buffer.writeInt(realCnt);
		buffer.writeByte(compression);
		
		tmpbuffer.flip();
		//buffer.writeBytes(tmpbuffer);
		tmpbuffer.flushToChannelBufferAndFree(buffer);
		return buffer;
	}

	public static ChannelBuffer toChannelBuffer(int qid, ResultHeap rheap, int realCnt, long processingTime){
		int cnt = rheap.size();
		int docids[] = new int[cnt];
		double scores[] = new double[cnt];
		rheap.decrSortResults(docids, scores);
		/* StringBuilder sb = new StringBuilder();
		for (int i=0; i<cnt; i++) sb.append(i + " " + docids[i] + " " + scores[i] + "\n");
		System.out.println(sb); */
		
		ChannelBuffer buffer = ChannelBuffers.buffer(3 * Constants.INT_SIZE + 1 + cnt * (Constants.INT_SIZE + Constants.FLOAT_SIZE) + Constants.LONG_SIZE);
		
		buffer.writeByte(ApplicationHandler.RESULTS);
		buffer.writeInt(qid);
		buffer.writeInt(cnt);
		buffer.writeInt(realCnt);

		for (int i=0;i<cnt;i++){
			buffer.writeInt(docids[i]);
			buffer.writeFloat((float)scores[i]);
		}
		
		buffer.writeLong(processingTime);
		return buffer;
	}

}
