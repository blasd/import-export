package com.gigaspaces.tools.importexport.remoting;

import com.j_spaces.core.client.SpaceURL;
import org.openspaces.core.GigaSpace;

import java.util.List;

/**
 * Created by skyler on 12/1/2015.
 */
public class RouteTranslator {
    private List<Integer> partitions;
    private GigaSpace proxy;
    private Integer desiredPartitionCount;
    private Integer[] targetPartitions;
    private Integer currentPartitionCount;

    public RouteTranslator(List<Integer> partitions, Integer desiredPartitionCount, GigaSpace proxy) {
        this.partitions = partitions;
        this.desiredPartitionCount = desiredPartitionCount;
        this.proxy = proxy;
    }

    public void initialize() {
        this.currentPartitionCount = parseCurrentPartitionCount();
        this.targetPartitions = determineTargetPartitions();

        if(this.desiredPartitionCount == null || this.desiredPartitionCount <= 0)
            this.desiredPartitionCount = this.currentPartitionCount;
    }

    public Integer getCurrentPartitionCount() {
        return currentPartitionCount;
    }

    public Integer getDesiredPartitionCount() {
        return desiredPartitionCount;
    }

    public Integer[] getTargetPartitions() {
        return targetPartitions.clone();
    }

    private Integer[] determineTargetPartitions() {
        Integer[] output;

        if(partitions == null || partitions.isEmpty()){
            output = new Integer[getCurrentPartitionCount()];

            for(int x = 0; x < getCurrentPartitionCount(); x++){
                output[x] = x;
            }
        } else {
            output = new Integer[this.partitions.size()];
            for(int x = 0; x < this.partitions.size(); x++)
                output[x] = (this.partitions.get(x) - 1);
        }

        return output;
    }

    private Integer parseCurrentPartitionCount() {
        SpaceURL url = proxy.getSpace().getURL();
        String total_members = url.getProperty("total_members");

        String[] split = total_members.split(",");
        return Integer.parseInt(split[0]);
    }
}


