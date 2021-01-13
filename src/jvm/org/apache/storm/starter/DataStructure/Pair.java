package org.apache.storm.starter.DataStructure;

import java.io.Serializable;

public final class Pair<T1, T2> implements Serializable {
    public T1 value1;
    public T2 value2;

    private Pair(){}

    private Pair(T1 first, T2 second) {
        this.value1 = first;
        this.value2 = second;
    }

    public static <T1, T2> Pair<T1, T2> of(T1 first, T2 second) {
        return new Pair<>(first, second);
    }

    public T1 getFirst() {
        return this.value1;
    }

    public T2 getSecond() {
        return this.value2;
    }

    public void setFirst(T1 value1){
        this.value1=value1;
    }

    public void setSecond(T2 value2){
        this.value2=value2;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Pair<?, ?> pair = (Pair)o;
            if (this.value1 != null) {
                if (this.value1.equals(pair.value1)) {
                    return this.value2 != null ? this.value2.equals(pair.value2) : pair.value2 == null;
                }
            } else if (pair.value1 == null) {
                return this.value2 != null ? this.value2.equals(pair.value2) : pair.value2 == null;
            }
            return false;
        } else {
            return false;
        }
    }

//    public int hashCode() {
//        int result = this.value1 != null ? this.value1.hashCode() : 0;
//        result = 31 * result + (this.value2 != null ? this.value2.hashCode() : 0);
//        return result;
//    }

    public String toString() {
        return "(" + this.value1 + ", " + this.value2 + ')';
    }
}
