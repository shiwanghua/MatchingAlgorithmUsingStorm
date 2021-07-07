package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
//import sun.text.normalizer.UBiDiProps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MyUtils {
    public Integer numVisualSubSet1;
    public Integer numVisualSubSet2;
    public HashMap<Integer, ArrayList<Integer>> completeNumberToVSS2Num; // All Equivalent vss2 number
    public Integer numVisualSubSet3;
    public HashMap<Integer, ArrayList<Integer>> zeroNumberToVSS3Num;     // All Equivalent vss3 number
    public Integer numVisualSubSet4;
    public HashMap<Integer, ArrayList<Integer>> zeroNumber2ToVSS4Num;    // All Equivalent vss4 number

    private Integer redundancy;
    private Integer numExecutor;
    private Integer numState;
    private Integer maxNumVisualSubSet;
    private Integer numCompleteNumbers;
    private Integer numZerosOfCompleteNumbers;
    public Integer numZeros2OfCompleteNumbers;

    static public HashMap<String, AtomicInteger> boltNameToIDAllocator;

    static {
        boltNameToIDAllocator = new HashMap<>();
    }

    private HashMap<Pair<Integer, Integer>, ArrayList<String>> mpv;
    private ArrayList<String> VSSIDtoExecutorID;
    private ArrayList<String> ExecutorIDtoVSSID;

    private Boolean[] executorCombination;// 最终结果：下标是否是完备数

    public MyUtils(Integer num_executor, Integer redundancy_degree) {
        numExecutor = num_executor;
        redundancy = redundancy_degree;
        numState = (int) Math.pow(2, numExecutor);
        numCompleteNumbers = 1; // executorCombination中值为true的个数
        mpv = new HashMap<>();
        executorCombination = new Boolean[numState]; // 单线程，一直重复覆盖利用就好了，不需要深拷贝
        completeNumberToVSS2Num = new HashMap<>();
        zeroNumberToVSS3Num = new HashMap<>();
        zeroNumber2ToVSS4Num = new HashMap<>();

        // calculate the number of visual subset
        maxNumVisualSubSet = calculateCmn();
//        numVisualSubSet1 = maxNumVisualSubSet;                              // 第一种：取最大编码数作为行数
//        numVisualSubSet2 = findVSSNumWithTheMostCompleteNumber();           // 第二种：取完备数最多的行数
//        numVisualSubSet3 = findVSSNumWithTheMostWeightedCompleteNumber();   // 第三种：取完备数中０的个数最多的行数
        calculateNumVSS(); // 三合一

//        VSSIDtoExecutorID = SubsetCodeGeneration1(numExecutor,redundancy);  // recursion
        mpv = null;  //  Now is not needed.
        VSSIDtoExecutorID = SubsetCodeGeneration2(numVisualSubSet3);
        ExecutorIDtoVSSID = generateExecutorIDtoVSSID(VSSIDtoExecutorID);  // 不需要深拷贝
        generateCombinationResult(ExecutorIDtoVSSID);                      // 只需要 new 一次，所以无需返回，直接在函数里修改executorCombination
    }

    static public synchronized Integer allocateID(String boltname) {
        if (!boltNameToIDAllocator.containsKey(boltname))
            boltNameToIDAllocator.put(boltname, new AtomicInteger(0));
        return (Integer) boltNameToIDAllocator.get(boltname).getAndIncrement();
    }

    // equal to VSSIDtoExecutorID.size()
    private Integer calculateCmn() {
//        System.out.println("numExecutor: " +numExecutor);
//        System.out.println("redundancy: " +redundancy);
        if (redundancy >= numExecutor) // 无需划分子订阅集，每个线程都存整个订阅集
            return 1;

//        Long n = 1L, m = 1L, nm = 1L;
        // 容易越界
//        for (int i = 2; i <= numExecutor; i++) {
//            n *= i;
//            if (i == redundancy)
//                m = n;
//            if (i == (numExecutor - redundancy))
//                nm = n;
//        }
//        n=n / m / nm;

        Long n = 1L, r = 1L;
        for (int i = 2; i <= redundancy; i++)
            r = r * i;
        for (int i = numExecutor - redundancy + 1; i <= numExecutor; i++)
            n = n * i;
        n = n / r;
        return n.intValue();
    }

    // Test 1 ~ C_n^r as number of visual subscription set to find the one with the most number of complete numbers.
    private Integer findVSSNumWithTheMostCompleteNumber() {
        if (redundancy >= numExecutor) // 无需划分子订阅集，每个线程都存整个订阅集
            return 1;
        int maxCompleteNumber = 0;
        int bestVSSNum = 1;
        for (int lineNum = 1; lineNum <= maxNumVisualSubSet; lineNum++) {
            if (lineNum * redundancy < numExecutor)
                continue;
            VSSIDtoExecutorID = SubsetCodeGeneration2(lineNum);
            ExecutorIDtoVSSID = generateExecutorIDtoVSSID(VSSIDtoExecutorID);  // 转置
            generateCombinationResult(ExecutorIDtoVSSID); // 结果保存在numCompleteNumber中
            if (maxCompleteNumber < numCompleteNumbers) {
                maxCompleteNumber = numCompleteNumbers;
                bestVSSNum = lineNum;
            }
        }
        return bestVSSNum;
    }

    // Test 1 ~ C_n^r as number of visual subscription set to find the one with the most number of zeros in all complete numbers.
    private Integer findVSSNumWithTheMostWeightedCompleteNumber() {
        if (redundancy >= numExecutor) // 无需划分子订阅集，每个线程都存整个订阅集
            return 1;
        int maxZeroNumber = 0;
        int bestVSSNum = 1;
        for (int lineNum = 1; lineNum <= maxNumVisualSubSet; lineNum++) {
            if (lineNum * redundancy < numExecutor)
                continue;
            VSSIDtoExecutorID = SubsetCodeGeneration2(lineNum);
            ExecutorIDtoVSSID = generateExecutorIDtoVSSID(VSSIDtoExecutorID);  // 转置
            generateCombinationResult(ExecutorIDtoVSSID); // 结果保存在numZerosOfCompleteNumbers中
            if (maxZeroNumber < numZerosOfCompleteNumbers) {
                maxZeroNumber = numZerosOfCompleteNumbers;
                bestVSSNum = lineNum;
            }
        }
        return bestVSSNum;
    }

    private void calculateNumVSS() {
        numVisualSubSet1 = maxNumVisualSubSet;
        if (redundancy >= numExecutor) // 无需划分子订阅集，每个线程都存整个订阅集
        {
            numVisualSubSet2 = 1;
            numVisualSubSet3 = 1;
            return;
        }

        int maxCompleteNumber = 0;
        int maxZeroNumber = 0;
        int maxZeroNumber2 = 0; // 平方

        for (int lineNum = 1; lineNum <= maxNumVisualSubSet; lineNum++) {
            if (lineNum * redundancy % numExecutor != 0) // lineNum * redundancy < numExecutor ||
                continue;
            VSSIDtoExecutorID = SubsetCodeGeneration2(lineNum);
            ExecutorIDtoVSSID = generateExecutorIDtoVSSID(VSSIDtoExecutorID);  // 转置
            generateCombinationResult(ExecutorIDtoVSSID);

            if (maxCompleteNumber < numCompleteNumbers) {
                maxCompleteNumber = numCompleteNumbers;
                numVisualSubSet2 = lineNum;
            }
//            ArrayList<Integer> a=completeNumberToVSS2Num.getOrDefault(numCompleteNumbers, new ArrayList<>());
//            a.add(lineNum);
//            completeNumberToVSS2Num.put(numCompleteNumbers,a);
            completeNumberToVSS2Num.computeIfAbsent(numCompleteNumbers, key -> new ArrayList<>()).add(lineNum);

            if (maxZeroNumber < numZerosOfCompleteNumbers) {
                maxZeroNumber = numZerosOfCompleteNumbers;
                numVisualSubSet3 = lineNum;
            }
            zeroNumberToVSS3Num.computeIfAbsent(numZerosOfCompleteNumbers, key -> new ArrayList<>()).add(lineNum);

            if (maxZeroNumber2 < numZeros2OfCompleteNumbers) {
                maxZeroNumber2 = numZeros2OfCompleteNumbers;
                numVisualSubSet4 = lineNum;
            }
            zeroNumber2ToVSS4Num.computeIfAbsent(numZeros2OfCompleteNumbers, key -> new ArrayList<>()).add(lineNum);
        }
    }

    //  从K位里生成含k个1的字符串的集合　递归编码
    //  K: numExecutor; k: redundancy
    private ArrayList<String> SubsetCodeGeneration1(int K, int k) {
        if (k == 0) return new ArrayList<String>() {{
            add(StringUtils.repeat("0", K));
        }};
        if (mpv.containsKey(Pair.of(k, K))) return mpv.get(Pair.of(k, K));
        ArrayList<String> strSet = new ArrayList<>();
        for (int i = k; i <= K; i++)  //  只有前 i 位有1且第 i 位必须是1
        {
            String highStr = StringUtils.repeat("0", K - i) + "1";
            //  从前i-1位里生成含k-1个1的字符串的集合
            ArrayList<String> lowPart = SubsetCodeGeneration1(i - 1, k - 1);
            mpv.put(Pair.of(k - 1, i - 1), lowPart);
            int size = lowPart.size();
            for (int j = 0; j < size; j++)
                strSet.add(highStr + lowPart.get(j));
        }
        return strSet;
    }

    // 第二种编码　迭代编码: 需考虑numExecutor远小于numVisualSubSet和numExecutor远大于numVisualSubSet的情形
    private ArrayList<String> SubsetCodeGeneration2(int numVSS) {
        StringBuilder[] stringBuilder = new StringBuilder[numVSS];
        for (int i = 0; i < numVSS; i++)
            stringBuilder[i] = new StringBuilder(StringUtils.repeat("0", numExecutor));
        int p = 0; // 遍历每个执行者位置
        for (int i = 0; i < Math.min(redundancy, numExecutor); i++) {
            for (int j = 0; j < numVSS; j++) {
                while (stringBuilder[j].charAt(p) == '1')
                    p = (p + 1) % numExecutor; // 这里应该只需要执行一次（当行数是列数整数倍或者列数是行数的整数倍时）
                stringBuilder[j].setCharAt(p, '1');
                p = (p + 1) % numExecutor;
            }
        }
        ArrayList<String> strSet = new ArrayList<>();
        for (int i = 0; i < numVSS; i++)
            strSet.add(stringBuilder[i].toString());
        return strSet;
    }

    // 转置二维数组
    private ArrayList<String> generateExecutorIDtoVSSID(ArrayList<String> VSSIDtoExecutorID) {
        int numExecutor = VSSIDtoExecutorID.get(0).length();
        StringBuilder[] stringBuilder = new StringBuilder[numExecutor];
        for (int i = 0; i < numExecutor; i++)
            stringBuilder[i] = new StringBuilder();

        for (int i = 0; i < VSSIDtoExecutorID.size(); i++) {
            for (int j = 0; j < numExecutor; j++) {
                stringBuilder[j].append(VSSIDtoExecutorID.get(i).charAt(j));
            }
        }

        ArrayList<String> ectIDtoVSSID = new ArrayList<>();
        for (int i = 0; i < numExecutor; i++)
            ectIDtoVSSID.add(stringBuilder[i].toString());

//        for (int i = 0; i < numExecutor; i++)
//            System.out.println(i+": "+ExecutorIDtoVSSID.get(i));
        return ectIDtoVSSID;
    }

    private void generateCombinationResult(ArrayList<String> ExecutorIDtoVSSID) {

        int countOne, id;
        String orResult;
        numCompleteNumbers = 0;
        numZerosOfCompleteNumbers = 0;
        for (int i = 0; i < numState; i++) {
            countOne = 0;
            int j = i;
            while (j != 0) {
                countOne++;
                j = j & (j - 1);
            }
            if (countOne < 1)  // 当只有１个执行者时只有一个１，但也是满的，是完备数
                executorCombination[i] = false;
            else if (countOne > numExecutor - redundancy) // cannot equal ! not redundancy !
            {
                executorCombination[i] = true;
                numCompleteNumbers++;
                numZerosOfCompleteNumbers += numExecutor - countOne;
                numZeros2OfCompleteNumbers += (int) Math.pow(numExecutor - countOne, 2);
            } else {
                executorCombination[i] = true;
                j = i;
                id = 0;
                orResult = "";
                while (j > 0) {
                    if (j % 2 == 1) {
                        if (orResult == "")  // Init orResult
                            orResult = ExecutorIDtoVSSID.get(id);
                        else
                            orResult = orOperation(orResult, ExecutorIDtoVSSID.get(id));
                    }
                    j = j >> 1;
                    id++;
                }

                for (j = 0; j < orResult.length(); j++)
                    if (orResult.charAt(j) == '0') {
                        executorCombination[i] = false; // It's i not j !
                        break;
                    }
                if (executorCombination[i] == true) { // 第一种编码不会为true, 第二种编码才可能为true
                    numCompleteNumbers++;
                    numZerosOfCompleteNumbers += numExecutor - countOne;
                    numZeros2OfCompleteNumbers += (int) Math.pow(numExecutor - countOne, 2);
//                    System.out.println("i = "+i+", orResult="+orResult+", countOne="+countOne);
                }
            }
        }
    }

    private String orOperation(String a, String b) {
        int n = a.length();
        if (b.length() != n)  //  exception
            return "false";
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < n; i++)
            if (a.charAt(i) == '1' || b.charAt(i) == '1')
                res.append('1');
            else res.append('0');
        return res.toString();
    }

    public Integer getIDNum(String boltname) {
        return boltNameToIDAllocator.get(boltname).get();
    }

    public ArrayList<String> getVSSIDtoExecutorID() {
        return VSSIDtoExecutorID;
    }

    public Boolean[] getExecutorCombination() {
        return executorCombination;
    }

    public Integer getNumVisualSubSet() {
        return numVisualSubSet3;
    }

    public Integer getNumCompleteNumbers() {
        return numCompleteNumbers;
    }

    public Integer getNumZerosOfCompleteNumbers() {
        return numZerosOfCompleteNumbers;
    }

    public Integer getNumZeros2OfCompleteNumbers() {
        return numZeros2OfCompleteNumbers;
    }
}
