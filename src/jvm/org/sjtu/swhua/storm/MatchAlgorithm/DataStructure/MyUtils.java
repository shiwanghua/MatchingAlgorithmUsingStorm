package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MyUtils {
    private Integer numVisualSubSet;
    private Integer numExecutor;
    private Integer redundancy;

    static public HashMap<String, AtomicInteger> boltNameToIDAllocator;
    static {
        boltNameToIDAllocator = new HashMap<>();
    }
    private HashMap<Pair<Integer, Integer>, ArrayList<String>> mpv;
    private ArrayList<String> VSSIDtoExecutorID;
    private ArrayList<String> ExecutorIDtoVSSID;

    private Boolean[] executorCombination;

    public MyUtils(Integer num_executor, Integer redundancy_degree) {
        numExecutor = num_executor;
        redundancy = redundancy_degree;

        mpv = new HashMap<>();
        VSSIDtoExecutorID = new ArrayList<>();

        // calculate the number of visual subset
        numVisualSubSet = calculateCmn(numExecutor, redundancy);

        VSSIDtoExecutorID = SubsetCodeGeneration(redundancy, numExecutor);  // recursion
        mpv = null;  //  Now is not needed.

        generateExecutorIDtoVSSID(VSSIDtoExecutorID);
        generateCombinationResult(ExecutorIDtoVSSID);
    }

    static public synchronized Integer allocateID(String boltname) {
        if (!boltNameToIDAllocator.containsKey(boltname))
            boltNameToIDAllocator.put(boltname, new AtomicInteger(0));
        return (Integer) boltNameToIDAllocator.get(boltname).getAndIncrement();
    }

    public Integer getIDNum(String boltname) {
        return boltNameToIDAllocator.get(boltname).get();
    }

    // equal to VSSIDtoExecutorID.size()
    private Integer calculateCmn(int numExecutor, int redundancy) {
        int n = 1, m = 1, nm = 1;
        for (int i = 2; i <= numExecutor; i++) {
            n *= i;
            if (i == redundancy)
                m = n;
            if (i == (numExecutor - redundancy))
                nm = n;
        }
        return n / m / nm;
    }

    //  从K位里生成含k个1的字符串的集合
    private ArrayList<String> SubsetCodeGeneration(int k, int K) {
        if (k == 0) return new ArrayList<String>() {{
            add(StringUtils.repeat("0", K));
        }};
        if (mpv.containsKey(Pair.of(k, K))) return mpv.get(Pair.of(k, K));
        ArrayList<String> strSet = new ArrayList<>();
        for (int i = k; i <= K; i++)  //  只有前 i 位有1且第 i 位必须是1
        {
            String highStr = StringUtils.repeat("0", K - i) + "1";
            //  从前i-1位里生成含k-1个1的字符串的集合
            ArrayList<String> lowPart = SubsetCodeGeneration(k - 1, i - 1);
            mpv.put(Pair.of(k - 1, i - 1), lowPart);
            int size = lowPart.size();
            for (int j = 0; j < size; j++)
                strSet.add(highStr + lowPart.get(j));
        }
        return strSet;
    }

    private void generateExecutorIDtoVSSID(ArrayList<String> VSSIDtoExecutorID) {
        int numExecutor = VSSIDtoExecutorID.get(0).length();
        StringBuilder[] stringBuilder = new StringBuilder[numExecutor];
        for (int i = 0; i < numExecutor; i++)
            stringBuilder[i] = new StringBuilder();

        for (int i = 0; i < VSSIDtoExecutorID.size(); i++) {
            for (int j = 0; j < numExecutor; j++) {
                stringBuilder[j].append(VSSIDtoExecutorID.get(i).charAt(j));
            }
        }

        ExecutorIDtoVSSID = new ArrayList<>();
        for (int i = 0; i < numExecutor; i++)
            ExecutorIDtoVSSID.add(stringBuilder[i].toString());

//        for (int i = 0; i < numExecutor; i++)
//            System.out.println(i+": "+ExecutorIDtoVSSID.get(i));
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

    private void generateCombinationResult(ArrayList<String> ExecutorIDtoVSSID) {

        int numExecutor = ExecutorIDtoVSSID.size();
        int n = (int) Math.pow(2, numExecutor);
        int countOne, id;
        String orResult;
        executorCombination = new Boolean[n];
        for (int i = 0; i < n; i++) {
            countOne = 0;
            int j = i;
            while (j != 0) {
                countOne++;
                j = j & (j - 1);
            }
            if (countOne <= 1)
                executorCombination[i] = false;
            else if (countOne > numExecutor-redundancy) // cannot equal ! not redundancy !
                executorCombination[i] = true;
            else {
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
                if(executorCombination[i]==true){ // 当前这种子集构造法不存在这样的情况，这个分支可以删了
                    System.out.println("i = "+i+", orResult="+orResult+", countOne="+countOne);
                }
            }
        }
    }

    public ArrayList<String> getVSSIDtoExecutorID() {
        return VSSIDtoExecutorID;
    }

    public Boolean[] getExecutorCombination() {
        return executorCombination;
    }

    public Integer getNumVisualSubSet() {
        return numVisualSubSet;
    }
}
