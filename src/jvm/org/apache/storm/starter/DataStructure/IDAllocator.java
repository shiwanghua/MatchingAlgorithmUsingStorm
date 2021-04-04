package org.apache.storm.starter.DataStructure;

import java.util.concurrent.atomic.AtomicInteger;

public class IDAllocator {
    private AtomicInteger allocator;
    public IDAllocator(){
        allocator=new AtomicInteger(-1);
    }
    public Integer allocateID(){
        return (Integer) allocator.incrementAndGet();
    }

    public Integer getIDNum(){
        return allocator.get();
    }
}
