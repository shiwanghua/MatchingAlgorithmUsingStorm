package org.apache.storm.starter.DataStructure;

public class TypeConstant {
    public static final int Insert_Subscription = 1;
    public static final int Insert_Attribute_Subscription = 2;
    public static final int Update_Attribute_Subscription = 3;
    public static final int Delete_Attribute_Subscription = 4;
    public static final int Delete_Subscription = 5;
    public static final int Event_Match_Subscription = 6;
    public static final int Null_Operation = 7;

    public static final int numAttributeType=30;
    public static final int maxNumSubscriptionPerPacket = 100;
    public static final int maxNumAttributePerSubscription = 10;
    public static final int subSetSize = 200000;
    public static final int maxNumEventPerPacket=20;
    public static final int maxNumAttributePerEvent=10;
}
