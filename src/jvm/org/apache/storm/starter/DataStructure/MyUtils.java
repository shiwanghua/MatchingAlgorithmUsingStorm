package org.apache.storm.starter.DataStructure;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.units.qual.A;
import org.json.simple.JSONObject;

import java.lang.management.BufferPoolMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MyUtils {
    private AtomicInteger allocator;
    private Integer numVisualSubSet;
    private Integer numExecutor;
    private Integer redundancy;

    static public HashMap<String, AtomicInteger> boltNameToIDAllocator;

    private HashMap<Pair<Integer, Integer>, ArrayList<String>> mpv;
    private ArrayList<String> VSSIDtoExecutorID;
    private ArrayList<String> ExecutorIDtoVSSID;
    private Boolean[] executorCombination;

    public MyUtils(Integer num_executor, Integer redundancy_degree) {
        numExecutor = num_executor;
        redundancy = redundancy_degree;

        boltNameToIDAllocator = new HashMap<>();
        mpv = new HashMap<>();
        VSSIDtoExecutorID = new ArrayList<>();
        allocator = new AtomicInteger(0);

        // calculate the number of visual subset
        numVisualSubSet = calculateCmn(numExecutor, redundancy);

        VSSIDtoExecutorID = SubsetCodeGeneration(redundancy, numExecutor);  // recursion
        mpv = null;  //  Now is not needed.

        generateExecutorIDtoVSSID(VSSIDtoExecutorID);
        generateCombinationResult(ExecutorIDtoVSSID);
    }

    public Integer allocateID() {
        return (Integer) allocator.getAndIncrement();
    }

    public Integer getIDNum() {
        return allocator.get();
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

        for(int i=0;i<numExecutor;i++)
            stringBuilder[i]=new StringBuilder();
        for(int i=0;i<VSSIDtoExecutorID.size();i++){
            System.out.println(String.format("%02d: ", i)+VSSIDtoExecutorID.get(i));
        }
        System.out.println(numExecutor);

        for (int i = 0; i < VSSIDtoExecutorID.size(); i++) {
            for (int j = 0; j < numExecutor; j++) {
                stringBuilder[j].append(VSSIDtoExecutorID.get(i).charAt(j));
            }
        }

        ExecutorIDtoVSSID = new ArrayList<>();
        for (int i = 0; i < numExecutor; i++)
            ExecutorIDtoVSSID.add(stringBuilder[i].toString());
    }

    private String xor(String a, String b) {
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
        executorCombination = new Boolean[n];
        for (int i = 0; i < n; i++) {
            int countOne = 0;
            int j = i;
            while (j != 0) {
                countOne++;
                j = j & (j - 1);
            }
            if (countOne <= 1)
                executorCombination[i] = false;
            else if (countOne >= redundancy)
                executorCombination[i] = true;
            else {
                executorCombination[i] = true;
                j = i;
                int id = 0;
                String xorResult = "";
                while (j > 0) {
                    if (j % 2 == 1) {
                        if (xorResult == "")  // Init xorResult
                            xorResult = ExecutorIDtoVSSID.get(id);
                        else
                            xorResult = xor(xorResult, ExecutorIDtoVSSID.get(id));
                    }
                    j = j >> 1;
                    id++;
                }

                for (j = 0; j < xorResult.length(); j++)
                    if (xorResult.charAt(j) == '0') {
                        executorCombination[j] = false;
                        break;
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

    public Integer getNumVisualSubSet(){
        return numVisualSubSet;
    }
}
