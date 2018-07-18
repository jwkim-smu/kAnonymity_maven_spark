package com.kAnonymity_maven;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class kAnonymity_spark2 implements Serializable {

	int KValue = 5;
	int dataSize = 100;
	int totalAttrSize = 7;
	String joinAttrListStr = "0"; //

	int check_num = 0;

	HashMap<Integer, String> need_gTree = new HashMap<Integer, String>();
	ArrayList<Integer> need_att = new ArrayList<Integer>();

	// -------------------------cluster_option----------------------------------
	String inputFileName = new String("/home/hp/data/inputFile.txt");
	String resizingInputData_T1 = "/home/hp/data/t1_resizingBy_" + dataSize + ".txt";
	String genTreeFileName = new String("/home/hp/data/gTree.txt");
	String resultFileName = "result/" + joinAttrListStr + "_" + dataSize;
	String original_T1 = new String("/home/hp/data/t1_sample.txt");
	String k_anonymous_tree_T1 = "hdfs:///home/hp/data/gTree_T1_";
	String k_member_T1 = "/home/hp/data/k_member_T1_";
	String transformed_kmember_T1 = new String("/home/hp/data/k_member_T1_" + KValue + ".txt");
	String transformed_kanonimity_T1 = new String("/home/hp/data/gTree_T1_" + KValue + ".txt");
	String ILmatrix_T1 = new String("/home/hp/data/ILmatrix_T1_" + KValue + ".txt");

	boolean stopFlag;

	int count = 0;
	String tranformedStr2 = new String();

	String inputFile_T1 = new String(resizingInputData_T1);

	HashMap<String, Integer> maxMap = new HashMap<String, Integer>();
	HashMap<String, ArrayList<Integer>> rangeMap = new HashMap<String, ArrayList<Integer>>();
	ArrayList<String> projectionList = new ArrayList<String>();
	ArrayList<Double> projectionSizeList = new ArrayList<Double>();
	ArrayList<ArrayList> tupleList_T1 = new ArrayList<ArrayList>();
	ArrayList<String> transfromed_tupleList_T1 = new ArrayList<String>();
	int IRcnt = 0;
	double curIR = 0.0;
	JavaRDD<Integer> line;
	JavaPairRDD<Integer, String> line2;
	JavaPairRDD<String, Integer> line3;

	public void loadGenTree() {
		System.out.println("loadGenTree Start!!");
		try {
			FileInputStream stream = new FileInputStream(genTreeFileName);
			InputStreamReader reader = new InputStreamReader(stream);
			BufferedReader buffer = new BufferedReader(reader);

			while (true) {
				String label = buffer.readLine();
				if (label == null)
					break;

				StringTokenizer st = new StringTokenizer(label, "|");
				String attrName = st.nextElement().toString();

				for (int i = 0; i < need_att.size(); i++) {
					if (attrName.equals(need_gTree.get(need_att.get(i)))) {
						Integer treeLevel = new Integer(st.nextElement().toString());
						String valueStr = st.nextElement().toString();

						// update min and max
						Integer curMax = maxMap.get(attrName);
						if (curMax == null)
							maxMap.put(attrName, treeLevel);
						else if (curMax.intValue() < treeLevel.intValue())
							maxMap.put(attrName, treeLevel);

						// insert range list
						ArrayList<Integer> tempArr = new ArrayList<Integer>();
						StringTokenizer valueStr_st = new StringTokenizer(valueStr, "_");
						while (valueStr_st.hasMoreTokens()) {
							tempArr.add(new Integer(valueStr_st.nextToken()));
						}

						rangeMap.put(attrName + "-" + treeLevel, tempArr);
						break;
					}
				}

			}

			System.out.println("maxMap : " + maxMap);
			System.out.println("rangeMap : " + rangeMap);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("loadGenTree Finish!!");
		}

	}

	private ArrayList<Integer> fitNode;

	public kAnonymity_spark2() {

		setting();
		if (need_att.size() != 0) {
			for (int i = 0; i < need_att.size(); i++) {
				switch (need_att.get(i)) {
				case 1:
					projectionList.add("age");
					break;
				case 2:
					projectionList.add("sex");
					break;
				case 3:
					projectionList.add("surgery");
					break;
				case 4:
					projectionList.add("length");
					break;
				case 5:
					projectionList.add("location");
					break;

				}
			}
			projectionList.add("disease");
		}

		else {
			projectionList.add("age");
			projectionList.add("sex");
			projectionList.add("surgery");
			projectionList.add("length");
			projectionList.add("location");
			// this.projectionList.add("a2");
			projectionList.add("disease");
		}
	}

	public void loadData(String inputFileName, ArrayList<ArrayList> curTupleList) {
		System.out.println("loadData Start!!");
		try {
			FileInputStream stream = new FileInputStream(inputFileName);
			InputStreamReader reader = new InputStreamReader(stream);
			BufferedReader buffer = new BufferedReader(reader);

			int curCount = 0;
			while (true) {
				String label = buffer.readLine();
				if (label == null)
					break;

				ArrayList curTuple = new ArrayList();
				StringTokenizer st = new StringTokenizer(label, "|");

				int need_att_index = 0;
				int flag = 1;
				int i = 1;
				String temp;
				while (true) {
					if (flag == 0)
						break;
					temp = st.nextToken();

					if (i == need_att.get(need_att_index)) {
						curTuple.add(new Integer(temp));
						need_att_index++;
						i++;
					}

					else {
						i++;
					}

					if (need_att_index == need_att.size())
						flag = 0;
				}

				String disease = null;
				while (st.hasMoreTokens()) {
					disease = st.nextToken();
				}

				curTuple.add(new String(disease)); // disease
				curTupleList.add(curTuple);

			}

			System.out.println("curTupleList : " + curTupleList + "\nlogenData()_FINSH\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("loadData Finish!!");
		}
	}

	public boolean performGeneralization(final ArrayList<Integer> curNode, ArrayList<ArrayList> curTupleList,
			final ArrayList<String> transfromed_curTupleList, JavaRDD<ArrayList> info) {
		// System.out.println(info.collect());
		stopFlag = true;
		System.out.println("\nperformGeneralization start!");
		final int attrNumber = projectionList.size();
		double nodeIR = 0.0;
		check_num = 0;

		HashMap<String, ArrayList<String>> anonymizedResult = new HashMap<String, ArrayList<String>>();

		line = info.map(new Function<ArrayList, String>() {
			public String call(ArrayList x) {

				ArrayList<String> temp = new ArrayList<String>();
				String tranformedStr = new String();

				for (int k = 0; k < attrNumber - 1; k++) {
					String attrName = projectionList.get(k);
					int treeLevel = curNode.get(k).intValue();
					int curAttrValue = ((Integer) x.get(k)).intValue();

					if (treeLevel > 0) {
						ArrayList<Integer> curRangeList = rangeMap.get(attrName + "-" + treeLevel);
						if (curRangeList.size() == 2) {
							tranformedStr = tranformedStr + "|" + curRangeList.get(0) + "_" + curRangeList.get(1);
							// nodeIR += 1.0;
						} else {
							for (int m = 0; m < curRangeList.size() - 1; ++m) {
								int curMin = curRangeList.get(m);
								int curMax = curRangeList.get(m + 1);

								if ((curMin <= curAttrValue) && (curAttrValue <= curMax)) {
									tranformedStr = tranformedStr + "|" + curMin + "_" + curMax;
									break;
								}
							}
						}
					}

					else {
						tranformedStr = tranformedStr + "|" + curAttrValue;
					}
				}
				transfromed_curTupleList.add(tranformedStr + "|" + ((String) x.get(attrNumber - 1)));
				temp.add(tranformedStr);

				String ill = "";
				String line = "|";
				StringTokenizer kts = new StringTokenizer(temp.get(0).toString(), ",");
				StringTokenizer kts2 = new StringTokenizer(kts.nextToken(), "|");
				while (true) {
					if (kts2.hasMoreTokens()) {
						ill = kts2.nextToken();
						line = line + ill + "|";
						continue;
					}

					else
						break;
				}

				return line;

			}
		}).mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) throws Exception {
				return x + y;
			}
		}).filter(new Function<Tuple2<String, Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				
				return v1._2 >= KValue;
			}
		}).values();
		
		if(!line.isEmpty()) {
			check_num = line.reduce(new Function2<Integer, Integer, Integer>() {
				
				public Integer call(Integer v1, Integer v2) throws Exception {
					
					return v1+v2;
				}
			});
		}
		
		
		System.out.println("check_num : " + check_num);
		
		return check_num == dataSize;

	}

	public void performAnonymity() {
		System.out.println("performAnonymity start!!");
		SparkConf conf = new SparkConf().setMaster("yarn-cluster").setAppName("kAnonymity_spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<ArrayList> info = sc.parallelize(tupleList_T1).persist(StorageLevel.MEMORY_ONLY_SER());
		ArrayList<ArrayList<Integer>> nodeQueue = new ArrayList<ArrayList<Integer>>();
		HashMap<String, Integer> duplicateList = new HashMap<String, Integer>();

		ArrayList<Integer> initNode = new ArrayList<Integer>();

		for (int i = 0; i < projectionList.size() - 1; ++i)
			initNode.add(new Integer(0));

		// initialize
		nodeQueue.add(initNode);

		int curCount = 0;
		while (nodeQueue.size() > 0) {
			ArrayList<Integer> curNode = nodeQueue.remove(0);
			System.out.println("curnode : " + curNode);
			// Perform anonymization
			if (curCount > 0) {
				transfromed_tupleList_T1.clear();

				if ((performGeneralization(curNode, tupleList_T1, transfromed_tupleList_T1, info))) {
					info.unpersist();
					fitNode = (ArrayList<Integer>) (curNode.clone());
					System.out.println("curNode : " + curNode);

					System.out.println("all jobs finished");

					return;
				} // if end

			} // if end

			// add next nodes
			for (int i = 0; i < projectionList.size() - 1; ++i) {
				ArrayList<Integer> tempNode = (ArrayList<Integer>) (curNode.clone());
				Integer attrMaxValue = maxMap.get(projectionList.get(i));
				// System.out.println("maxMap : " + maxMap);
				// System.out.println("curNode : " + curNode);
				// System.out.println("tempNode : " + tempNode);
				// System.out.println("attrMaxValue : " + attrMaxValue);
				// System.out.println("projectionList : " +projectionList + "\n");
				if (attrMaxValue >= (tempNode.get(i).intValue() + 1)) {

					tempNode.set(i, new Integer(tempNode.get(i).intValue() + 1));

					String tempStr = new String();

					for (int j = 0; j < projectionList.size() - 1; ++j)
						tempStr = tempStr + "_" + tempNode.get(j);
					Object tempObj = duplicateList.get(tempStr);
					if (tempObj == null) {
						nodeQueue.add(tempNode);
						duplicateList.put(tempStr, new Integer(0));
					}
				}
			}
			++curCount;
		}
		System.out.println("performAnonymity Finish!!");
	}

	public String run() {
		System.out.println("============================================================");
		System.out.println("k    : " + KValue);
		System.out.println("============================================================");

		loadGenTree();
		loadData(inputFile_T1, tupleList_T1);

		performAnonymity();

		System.out.println(line.collect());
		return KValue + "\t" + fitNode + "\t";
		// return null;
	}

	public void setting() {
		need_gTree.put(1, "age");
		need_gTree.put(2, "sex");
		need_gTree.put(3, "surgery");
		need_gTree.put(4, "length");
		need_gTree.put(5, "location");

		int num = 5;
		// for(int i=0;i<num;i++) {
		// //need_att.add(scan.nextInt());
		//
//		// // // }
//		need_att.add(1);
//		need_att.add(5);

		if (num == 0 || num == 5) {
			for (int i = 0; i < 5; i++) {
				need_att.add(i + 1);
			}
		}
	}

	// main part
	public static void main(String[] args) {
		// System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-2.8.4");

		long start = System.currentTimeMillis();

		kAnonymity_spark2 mykAnonymity = new kAnonymity_spark2();

		mykAnonymity.run();
		System.out.println("***** Done ***** ");
		long end = System.currentTimeMillis();

		System.out.println("running time : " + (end - start) / 1000.0);
	}

}