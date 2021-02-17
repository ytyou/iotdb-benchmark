package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.Random;

public class LogNormDistribution {

  private final Config config;
  private final Random random;
  private int MAX_DELAY;
  private double MEAN_VALUE;
  private double VARIANCE;
  private double mu;
  private double sigma;
  private double[] range;

  private static class LogNormDistributionHolder {

    private static final LogNormDistribution INSTANCE = new LogNormDistribution();
  }

  public static LogNormDistribution getInstance() {
    return LogNormDistributionHolder.INSTANCE;
  }

  public LogNormDistribution() {
    this.config = ConfigDescriptor.getInstance().getConfig();
    this.random = new Random(config.getDATA_SEED());
    this.MAX_DELAY = config.getMAX_DELAY();
    this.MEAN_VALUE = (double) config.getMEAN_VALUE();
    this.VARIANCE = (double) config.getVARIANCE();
//    10 % overlap with memtable size = 800, interval = 500
//    this.MAX_DELAY = 240000;
//    this.MEAN_VALUE = 120000 ;
//    this.VARIANCE = (double) 120000 * 120000 * 1.44;
//    5 % overlap
//    this.MAX_DELAY = 180000;
//    this.MEAN_VALUE = 60000 ;
//    this.VARIANCE = (double) 60000 * 60000 * 0.54;
    calculateCDFtoPoint();

  }

  private void calculateCDFtoPoint() {
    initParam();
    double[] p = new double[MAX_DELAY + 1];
    double sum = 0;
    for (int i = 1; i < MAX_DELAY; i++) {
      p[i] = getLogNormProbability(i);
      sum += p[i];
    }
    p[MAX_DELAY] = 1 - sum;
    range = new double[MAX_DELAY + 1];
    range[0] = 0;
    for (int i = 1; i <= MAX_DELAY; i++) {
      range[i] = range[i - 1] + p[i];
    }
  }

  private double getLogNormProbability(int x) {
    double a = Math.pow(Math.E, -Math.pow(Math.log(x) - mu, 2) / (2 * sigma * sigma));
    double b = x * sigma * Math.sqrt(2 * Math.PI);
    return a / b;
  }

  private void initParam() {
    mu = Math.log(MEAN_VALUE) - 0.5 * Math.log(1 + VARIANCE / (MEAN_VALUE * MEAN_VALUE));
    sigma = Math.sqrt(Math.log(1 + VARIANCE / (MEAN_VALUE * MEAN_VALUE)));
  }

  private void printOverlapRate() {
    // 初始化时间间隔为 500 ms
    int interval = 500;
    // memtable 大小为 800
    int memtableSize = 800;
    double totalRate = 0;
    int maxOverlap = MAX_DELAY / interval;
    for (int i = 1; i <= maxOverlap; i++) {
      int deltaT = i * interval;
      double probability = 0;
      for (int j = 1; j < MAX_DELAY - deltaT; j++) {
        probability += (range[j] - range[j - 1]) * (1 - range[j + deltaT]);
      }
      totalRate += probability;
      System.out.println("overlap index:" + i + ",  rate:" + probability);
    }
    System.out.println("total overlap rate:" + totalRate/memtableSize);
  }

  public long getArrivalTime(long timestamp) {
    double rand = random.nextDouble();
    for (int i = 1; i <= MAX_DELAY; i++) {
      if (rand <= range[i]) {
        return timestamp + i;
      }
    }
    return 0;
  }

  public static void main(String[] args) {
    LogNormDistribution logNormDistribution = LogNormDistribution.getInstance();
//    for (int i = 1; i < 2000; i++) {
//      System.out.println(logNormDistribution.getArrivalTime(0));
//    }
    logNormDistribution.printOverlapRate();
  }

}
