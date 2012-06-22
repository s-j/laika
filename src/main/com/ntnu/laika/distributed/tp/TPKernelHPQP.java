package com.ntnu.laika.distributed.tp;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import com.ntnu.laika.Constants;
import com.ntnu.laika.structures.Index;
import com.ntnu.laika.structures.LocalIndex;
import com.ntnu.laika.structures.MasterIndex;
import com.ntnu.network.ApplicationHandler;
import com.ntnu.network.CntBarrier;
import com.ntnu.network.MessageHandler;
import com.ntnu.network.NetworkServer;

/**
 * @author <a href="mailto:simonj@idi.ntnu.no">Simon Jonassen</a>
 */
public class TPKernelHPQP {
protected NetworkServer server;
protected ExecutorService workpool;
protected Index index;
protected boolean isRootNode;
protected int nodeId;
protected int portNr;
protected int numworkers;
private String rootInfo;
private String[] args;

public static final int LESTER = 0;
public static final int SKIPLESTER = 1;
public static final int MSDOR = 2;
public static final int AND = 3;
public static int processortype = MSDOR;
public static int lester_L;

public static boolean testing = true;

//args: portnr;<roothost:port> indexpath
public TPKernelHPQP(String[] args){
//	while (true){ System.out.println(1); if (false) break;}
	this.args = args;
	workpool = Executors.newCachedThreadPool();
	
	String tmp[] = args[0].split("/");
	portNr = Integer.parseInt(tmp[0]);
	rootInfo = tmp[1];
	//NOTE: here it requires that the node with no root info is the root node itself.
	isRootNode = (rootInfo.length()==1);
	
	//load properties
	index = isRootNode?new MasterIndex(args[1]):new LocalIndex(args[1]);
	Properties properties = index.getIndexProperties();
	nodeId = Integer.parseInt(properties.getProperty("id"));
	numworkers = Integer.parseInt(properties.getProperty("workerscnt"));
	server = new NetworkServer(nodeId, portNr, workpool);
}

//
// Scenario:
// Each node loads data, connects and says hello to the broker, which forwards hello_fwd to the nodes it knows and says hello_ack back
// A node receiving a hello_fwd connects and says hello_ack to the specified node.
// 
// When a node receives workerscnt+1 hello*'s - it sends ready to the root node. 
// When the rootnode receives workerscnt ready's it proceeds to the benchmark.
// 
// When the benchmark is done, broker sends SHUTDOWN to every other node and halts.
// Each node receiving the shutdown request halts.
//

public void boot(){
	//SimpleStats.init("blocks","chunks","docs","scors","accin","accout");
	if (isRootNode){
		isRootNode = true;
		CntBarrier barrier = new CntBarrier(server, ApplicationHandler.READY, numworkers);
		barrier.waitFor();
		
		//done connectiing, now do processing, then disconnect!!!
		System.out.println("rootready!");
		//server.printChannels();
		//server.printKnownHosts();
		
		System.out.println("run:" +args[2]);
		String subargs[] = args[2].split("/");
		
		TPMasterQueryPreprocessingHPQP.isGLB = subargs[3].equals("glb");
		processortype = subargs[4].equals("and") ? AND : MSDOR;
		System.out.println((processortype==AND)?"(and)":"(or)" + subargs[3]);
		if (subargs.length > 5 && subargs[5].equals("testing")){
			System.out.println("testing");
			testing = true;
		}
		
		TPQueryDispatcherHPQP dispatcher = new TPQueryDispatcherHPQP(this, Integer.parseInt(subargs[0]), Integer.parseInt(subargs[1]), 
				Integer.parseInt(subargs[2]));
		server.setMessageHandler(ApplicationHandler.RESULTS, dispatcher);
		workpool.submit(dispatcher);
	} else {
		String subargs[] = args[2].split("/");
		//Constants.MAX_NUMBER_OF_RESULTS = Integer.parseInt(subargs[3]);
		//System.out.println(" NumRes: " + Constants.MAX_NUMBER_OF_RESULTS);
		processortype = subargs[4].equals("and") ? AND : MSDOR;
			
		//Statistics localstats = ((LocalIndex)index).getLocalStatistics();
		TPWorkerQueryProcessingHPQP worker = new TPWorkerQueryProcessingHPQP(this);
		server.setMessageHandler(ApplicationHandler.QUERY, worker);
		server.setMessageHandler(ApplicationHandler.QUERY_BUNDLE, worker);
		
		CntBarrier barrier = new CntBarrier(server, ApplicationHandler.HELLO, numworkers);	//numworkers + rootnode - self
		server.setMessageHandler(ApplicationHandler.SHUTDOWN, new ShutdownRequestHandler(worker));
		
		String tmp[] = rootInfo.split(":");
		String rootHost = tmp[0];
		int rootPortNr = Integer.parseInt(tmp[1]);
	
		Channel ch = server.connect(rootHost, rootPortNr);
		server.sayHello(ch);
		
		//now, wait for the remaining nodes to connect...			
		barrier.waitFor();
		System.out.println(nodeId+"ready!");
		//server.printChannels();
		//server.printKnownHosts();
		
		//READY! Acknowledge the rootnode.
		ChannelBuffer buffer = ChannelBuffers.buffer(1);
		buffer.writeByte(ApplicationHandler.READY);
		ch.write(buffer);
	}
}

private class ShutdownRequestHandler implements MessageHandler{
	private TPWorkerQueryProcessingHPQP worker;
	
	public ShutdownRequestHandler(TPWorkerQueryProcessingHPQP worker){
		this.worker = worker;
	}
	
	@Override
	public void messageReceived(Channel channel, ChannelBuffer buffer){
		//System.out.println(nodeId + " " + SimpleStats.getString());
		server.shutdown();
		worker.shutdown();
	}
}

public static void main(String[] args){
	TPKernelHPQP kernel = new TPKernelHPQP(args);
	kernel.boot();
}

public void printInfo() {
	// TODO Auto-generated method stub
}
}