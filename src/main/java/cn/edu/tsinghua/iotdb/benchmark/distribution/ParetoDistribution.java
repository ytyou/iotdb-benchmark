package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

import java.util.Random;

public class ParetoDistribution {

    private static Config config;
    private static Random random;
    private double minValue;
    private double alpha;

    public ParetoDistribution(long random) {
        this.config = ConfigDescriptor.getInstance().getConfig();
        this.random = new Random(random);
        this.minValue = config.PRTMIN;
        this.alpha = config.PRTALPHA;
    }

    public long getNext(){
        return (long)Math.ceil(minValue / Math.pow(random.nextDouble(), 1 / alpha));
    }

    /*
    public static void main(String[] args){
        ParetoDistribution p = new ParetoDistribution(0);
        int a = 0;
        for (int i = 0; i < 20000000; i++) {
            a += p.getNext();
            System.out.println(a);
        }
    }*/
}
