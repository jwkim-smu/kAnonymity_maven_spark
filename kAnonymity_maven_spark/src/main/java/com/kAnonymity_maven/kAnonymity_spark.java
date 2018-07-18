package com.kAnonymity_maven;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class kAnonymity_spark implements Serializable {

	int KValue;
	int totalAttrSize = 7;
	String joinAttrListStr = "0"; //

	HashMap<Integer, String> need_gTree = new HashMap<Integer, String>();
	ArrayList<Integer> need_att = new ArrayList<Integer>();

	// -------------------------cluster_option----------------------------------
	String genTreeFileName = new String("/home/hp/data/gTree.txt");
	int count = 0;
	String tranformedStr2 = new String();
	boolean stopFlag = true;

	HashMap<String, Integer> maxMap = new HashMap<String, Integer>();
	HashMap<String, ArrayList<Integer>> rangeMap = new HashMap<String, ArrayList<Integer>>();
	ArrayList<String> projectionList = new ArrayList<String>();
	ArrayList<Double> projectionSizeList = new ArrayList<Double>();
	ArrayList<ArrayList> tupleList_T1 = new ArrayList<ArrayList>();
	int IRcnt = 0;
	double curIR = 0.0;
	JavaRDD<String> line;
	JavaPairRDD<Integer, String> line2;

	Map<String, Long> output;

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

	public kAnonymity_spark(int k) {

		setting(k);

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

	public boolean performGeneralization(final ArrayList<Integer> curNode, JavaRDD<String> info2) {
		System.out.println("performGeneralization start!");
		final int attrNumber = projectionList.size();
		stopFlag = true;

		Iterator<Long> kts2;
		Map<String, Long> kts1;

		kts1 = info2.map(new Function<String, String>() {
			public String call(String v1) throws Exception {
				String tranformedStr = new String();

				StringTokenizer token = new StringTokenizer(v1, "|");

				for (int k = 0; k < attrNumber - 1; k++) {
					String attrName = projectionList.get(k);
					int treeLevel = curNode.get(k).intValue();
					int curAttrValue = Integer.parseInt(token.nextToken());

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
				return tranformedStr;
			}
		}).countByValue();

		kts2 = kts1.values().iterator();

		while (kts2.hasNext()) {
			Long check_num = kts2.next();
			if (check_num.intValue() < KValue) {
				System.out.println(curNode + " is fail!! \n");
				return false;
			}
		}
		System.out.println(kts1);

		return stopFlag;

	}

	public void performAnonymity(String input) {
		System.out.println("performAnonymity start!!");
		SparkConf conf = new SparkConf().setMaster("yarn-cluster").setAppName("kAnonymity_spark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// input_data "hdfs:///home/hp/data/inputFile_10G.txt"
		JavaRDD<String> info2 = sc.textFile(input).coalesce(144).cache();

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

			// Perform anonymization
			if (curCount > 0) {
				if ((performGeneralization(curNode, info2))) {
					System.out.println("Success node : " + curNode);

					sc.close();
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

	public String run(String input) {
		loadGenTree();
		performAnonymity(input);
		System.out.println("all jobs finished");

		System.out.println("============================================================");
		System.out.println("k    : " + KValue);
		System.out.println("============================================================");

		return KValue + "\t";
	}

	public void setting(int k) {
		KValue = k;
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

	public static void main(String[] args) {

		// args[0] is kvalue
		// args[1] is input file name
		int kvalue = Integer.parseInt(args[0]);
		String file_name = args[1];
		long start = System.currentTimeMillis();
		kAnonymity_spark mykAnonymity = new kAnonymity_spark(kvalue);
		mykAnonymity.run(file_name);
		System.out.println("***** Done ***** ");
		System.out.println("KValue is : " + kvalue);
		System.out.println("Input File is : " + file_name);
		long end = System.currentTimeMillis();

		System.out.println("running time : " + (end - start) / 1000.0);
	}

}