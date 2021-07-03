package org.sjtu.swhua.storm.MatchAlgorithm.DataStructure;

public class TypeConstant {
    // 操作类型
    public static final int Insert_Subscription = 1;
    public static final int Insert_Attribute_Subscription = 2;
    public static final int Update_Attribute_Subscription = 3;
    public static final int Delete_Attribute_Subscription = 4;
    public static final int Delete_Subscription = 5;
    public static final int Event_Match_Subscription = 6;
    public static final int Null_Operation = 7;

    // 算法类型
    public static final int SIMPLE = 1;
    public static final int REIN = 2;
    public static final int TAMA = 3;

    // 平凡参数
    public static final int subSetSize = 1000000;
    public static final int numAttributeType = 30;
    public static final int maxNumSubscriptionPerPacket = 1;
    public static final int maxNumEventPerPacket = 1;
    public static final int maxNumAttributePerSubscription = 10; // 对所有匹配算法都是越小越好！
//    public static final String baseLogFilePath = "/home/swhua/Desktop/log/log1/";
    public static final String baseLogFilePath = "/root/log/log_10_100b_3v/";
    public static final long intervalTime = 1000000000L; // The interval between two calculations of speed

    // 实验模型参数

    public static final int TYPE = 2; // 本次实验所用的匹配算法类型，假定都用同一种匹配算法

    // Simple
    public static final double maxAttributeProportion_Simple = 0.5;
    public static final int maxNumAttributePerEvent_Simple = (int) (numAttributeType * maxAttributeProportion_Simple);
    public static final double maxIntervalWidth_Simple = 0.2;

    //REIN
    public static final int numBucket = 100;
    public static final double minAttributeProportion_Rein = 1.0; // 事件有取值的属性个数至少占属性种数的比例
    public static final int minNumAttributePerEvent_Rein = (int) (numAttributeType * minAttributeProportion_Rein); // 每个事件里最少要有这么多个属性有值, 属性越少匹配越慢
    public static final double minIntervalWidth_Rein = 0.5;

    // TAMA
    public static final int numLevel = 5;
    public static final double maxAttributeProportion_Tama = 0.5; // 事件有取值的属性个数至多占属性种数的比例
    public static final int maxNumAttributePerEvent_Tama = (int) (numAttributeType * maxAttributeProportion_Tama);
    public static final double minIntervalWidth_Tama = 0.05;

    //MPM
    public static final int numExecutorPerSpout = 1;
    public static final int numExecutorPerMatchBolt =10;
    public static final int redundancy = 3;
    public static final int numMatchBolt = 1;
}
