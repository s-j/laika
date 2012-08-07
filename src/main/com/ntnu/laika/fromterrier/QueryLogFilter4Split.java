package com.ntnu.laika.fromterrier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.ntnu.laika.distributed.dp.DPMasterQueryPreprocessing.MasterQuery;
import com.ntnu.laika.distributed.dp.DPMasterQueryPreprocessing;
//import com.ntnu.laika.distributed.tp.TPMasterQueryPreprocessingHPQP.MasterQuery;
//mport com.ntnu.laika.distributed.tp.TPMasterQueryPreprocessingHPQP;
import com.ntnu.laika.structures.MasterIndex;

public class QueryLogFilter4Split {
	public static void main(String args[]) throws IOException{
		//MasterIndex index = new MasterIndex("/mnt/data/data/ENVERIDX/8.HP.CUT.RYES/0");
		MasterIndex index = new MasterIndex("/mnt/data/data/ENVERIDX/8.DPHP.RNO/0");
		index.loadFastMaxScores(index.getStatistics().getNumberOfUniqueTerms());
		DPMasterQueryPreprocessing preproc = new DPMasterQueryPreprocessing(index.getGlobalLexicon(), true);
		//TPMasterQueryPreprocessingHPQP preproc = new TPMasterQueryPreprocessingHPQP(index.getGlobalLexicon());

		BufferedReader br= new BufferedReader(new FileReader("/mnt/data/data/ENVERIDX/querylog.test"));
		BufferedWriter bw= new BufferedWriter(new FileWriter("/mnt/data/data/ENVERIDX/querylog.test_ext_dp"));
		
		String q;
		for (int i=0; i<30000;){
			q=br.readLine();
			MasterQuery mq = preproc.processQuery(q);
			if (mq!=null){
				bw.write(q+"\t"+mq.numTerms+"\n");
				i++;
				if (i%1000==0) System.out.println(i);
			}
		}
		preproc.close();
		index.close();
		br.close();
		bw.close();
	}
}
