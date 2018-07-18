package com.kAnonymity_maven;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class kAnonymity_spark_local implements Serializable {

	int KValue = 5;
	int dataSize = 20;
	int totalAttrSize = 7;
	String joinAttrListStr = "0"; //

	HashMap<Integer, String> need_gTree = new HashMap<Integer, String>();
	ArrayList<Integer> need_att = new ArrayList<Integer>();

	// -------------------------cluster_option----------------------------------
	String inputFileName = new String("data/inputFile.txt");
	String resizingInputData_T1 = "data/t1_resizingBy_" + dataSize + ".txt";
	String genTreeFileName = new String("data/gTree.txt");
	String resultFileName = "result/" + joinAttrListStr + "_" + dataSize;
	String original_T1 = new String("data/t1_sample.txt");
	String k_anonymous_tree_T1 = "data/gTree_T1_";
	String k_member_T1 = "data/k_member_T1_";
	String transformed_kmember_T1 = new String("data/k_member_T1_" + KValue + ".txt");
	String transformed_kanonimity_T1 = new String("data/gTree_T1_" + KValue + ".txt");
	String ILmatrix_T1 = new String("data/ILmatrix_T1_" + KValue + ".txt");

	int count = 0;
	String tranformedStr2 = new String();
	boolean stopFlag = true;

	String inputFile_T1 = new String(resizingInputData_T1);

	HashMap<String, Integer> maxMap = new HashMap<String, Integer>();
	HashMap<String, ArrayList<Integer>> rangeMap = new HashMap<String, ArrayList<Integer>>();
	ArrayList<String> projectionList = new ArrayList<String>();
	ArrayList<Double> projectionSizeList = new ArrayList<Double>();
	ArrayList<ArrayList> tupleList_T1 = new ArrayList<ArrayList>();
	ArrayList<String> transfromed_tupleList_T1 = new ArrayList<String>();
	int IRcnt = 0;
	double curIR = 0.0;
	JavaRDD<String> line;
	JavaPairRDD<Integer, String> line2;

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

	public kAnonymity_spark_local() {

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


	public boolean performGeneralization(final ArrayList<Integer> curNode, ArrayList<ArrayList> curTupleList,
			final ArrayList<String> transfromed_curTupleList, JavaRDD<String> info2) {
		// System.out.println(info.collect());
		System.out.println("performGeneralization start!");
		final int attrNumber = projectionList.size();
		double nodeIR = 0.0;
		stopFlag = true;

		System.out.println("curNode : " + curNode);

		Map<String, Long> kts1 = info2.map(new Function<String, String>() {
			public String call(String v1) throws Exception {
				int hash_value_count;
				ArrayList<String> temp = new ArrayList<String>();
				String tranformedStr = new String();

				StringTokenizer token = new StringTokenizer(v1, "|");

				for (int k = 0; k < attrNumber - 1; k++) {
					String attrName = projectionList.get(k);
					int treeLevel = curNode.get(k).intValue();
					int curAttrValue = Integer.parseInt(token.nextToken());

					if (treeLevel > 0) {
						ArrayList<Integer> curRangeList = rangeMap.get(attrName + "-" + treeLevel);
						if (curRangeList.size() == 2) {
							tranformedStr = tranformedStr + "|"+ curRangeList.get(0) + "_" + curRangeList.get(1);
							// nodeIR += 1.0;
						} else {
							for (int m = 0; m < curRangeList.size() - 1; ++m) {
								int curMin = curRangeList.get(m);
								int curMax = curRangeList.get(m + 1);

								if ((curMin <= curAttrValue) && (curAttrValue <= curMax)) {
									tranformedStr = tranformedStr + "|"+ curMin + "_" + curMax ;
									break;
								}
							}
						}
					}

					else {
						tranformedStr = tranformedStr + "|" + curAttrValue;
					}
				}
//
//				while (token.hasMoreTokens()) {
//					String remain_value = token.nextToken();
//					if (!token.hasMoreElements()) {
//						tranformedStr = tranformedStr + "|" + remain_value;
//						break;
//					}
//				}

				return tranformedStr;
			}
		}).countByValue();
		System.out.println("kts1 : " + kts1);
		System.out.println("kts1_value : " + kts1.values());
		Iterator<Long> kts2 = kts1.values().iterator();

		while (kts2.hasNext()) {
			Long check_num = kts2.next();
			System.out.println("check_num : " + check_num.intValue() + ", K : " + KValue);
			if (check_num.intValue() < KValue)
				return false;
		}

		return stopFlag;

	}

	public void performAnonymity() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("kAnonymity_spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		System.out.println("performAnonymity start!!");

		JavaRDD<String> info2 = sc.textFile("data/t1_resizingBy_100.txt");

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

				if ((performGeneralization(curNode, tupleList_T1, transfromed_tupleList_T1, info2))) {

					// fitNode = (ArrayList<Integer>) (curNode.clone());
					System.out.println("all jobs finished");

					return;
				} // if end

			} // if end

			// add next nodes
			for (int i = 0; i < projectionList.size() - 1; ++i) {
				ArrayList<Integer> tempNode = (ArrayList<Integer>) (curNode.clone());
				Integer attrMaxValue = maxMap.get(projectionList.get(i));
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

		performAnonymity();

		return KValue + "\t";
		// return null;
	}

	public void setting() {
		need_gTree.put(1, "age");
		need_gTree.put(2, "sex");
		need_gTree.put(3, "surgery");
		need_gTree.put(4, "length");
		need_gTree.put(5, "location");

		int num = 5;

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

		kAnonymity_spark_local mykAnonymity = new kAnonymity_spark_local();

		mykAnonymity.run();
		System.out.println("***** Done ***** ");
		long end = System.currentTimeMillis();

		System.out.println("running time : " + (end - start) / 1000.0);
	}

}