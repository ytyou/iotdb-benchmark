package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.Random;

public class NonUniformDistributionV2 {

    private static Config config;
    private static Random random;
    private double lambda;

    public NonUniformDistributionV2(long random) {
        this.config = ConfigDescriptor.getInstance().getConfig();
        this.random = new Random(random);
        this.lambda = config.LAMBDA;
    }

    //泊松分布    4/100时间单位    大约25一个请求
    public long Poisson(){
        long x = 1;
        double b = 1, c = Math.exp(-(this.lambda - 1)), u;
        do {
            u = random.nextDouble();
            b *= u;
            if (b >= c)
                x++;
        } while (b >= c);
        return x * 1000;
    }

}