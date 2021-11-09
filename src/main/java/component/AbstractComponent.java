package component;

import common.metrics.Metric;
import common.metrics.TimeMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import query.LiebreContext;
import stream.Stream;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractComponent<IN, OUT> implements Component {

  private static final Logger LOG = LogManager.getLogger();

  protected final ComponentState<IN, OUT> state;

  // Exponential moving average alpha parameter
  // for cost and selectivity moving averages
  // The ALPHA we want to use for EMAs
  private static final double TARGET_ALPHA = 0.6;
  // The update period that the target alpha would be applied to, in millis
  private static final long TARGET_UPDATE_PERIOD = 1000;
  private static final long MILLIS_TO_NANOS = 1_000_000;
  // The actual alpha that we use, changing depending on the actual update period length
  private volatile double alpha = 0.4;
  private final AtomicLong tuplesWritten = new AtomicLong(0);
  private final AtomicLong tuplesRead = new AtomicLong(0);
  private final AtomicLong processingTimeNanos = new AtomicLong(0);
  private transient volatile long lastRateAndAlphaUpdateTime = System.currentTimeMillis();
  private transient volatile long lastUpdateFromReplica = System.currentTimeMillis();
  private final AtomicReference<Double> selectivity = new AtomicReference<>(1D);
  private final Set<Integer> replicaSet = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean updateInProgress = new AtomicBoolean(false);

  private final AtomicReference<Double> cost = new AtomicReference<>(1D * MILLIS_TO_NANOS);
  private final AtomicReference<Double> rate = new AtomicReference<>(0D);

  private final TimeMetric executionTimeMetric;
  private final Metric rateMetric;
  private transient boolean flushed;

  public AbstractComponent(String id, ComponentType type) {
    this.state = new ComponentState<>(id, type);
    this.executionTimeMetric =
        LiebreContext.operatorMetrics().newAverageTimeMetric(id, "EXEC");
    this.rateMetric = LiebreContext.operatorMetrics().newCountPerSecondMetric(id, "RATE");
  }

  @Override
  public boolean runFor(int rounds) {
    int executions = 0;
    long tuplesWrittenBefore = tuplesWritten.longValue();
    long tuplesReadBefore = tuplesRead.longValue();
    long startTime = System.nanoTime();
    // Process while the component is enabled, the rounds are not finished and there is actually
    // processing happening at every execution
    while (isEnabled() && executions < rounds) {
      run();
      executions += 1;
    }
    long endTime = System.nanoTime();
    // Update processing time
    processingTimeNanos.addAndGet(endTime - startTime);
    return tuplesReadBefore != tuplesRead.longValue() || tuplesWrittenBefore != tuplesWritten.longValue();
  }

  @Override
  public final void run() {
    if (isEnabled()) {
      executionTimeMetric.startInterval();
      process();
      executionTimeMetric.stopInterval();
    }
  }

  protected abstract void process();

  protected final void increaseTuplesRead() {
    tuplesRead.incrementAndGet();
    rateMetric.record(1);
  }

  protected final void increaseTuplesWritten() {
    tuplesWritten.incrementAndGet();
  }

  /**
   * Update the cost and selectivity based on the tuples processed and the time it took. <br>
   * <b>WARNING: The variables for the metrics are available only the execution happens with {@link
   * #runFor(int)} !</b> <br>
   * <b>WARNING: This is not thread safe! It should either be run from the operator thread or from
   * another thread while the operator is stopped. The results are visible to all threads.</b>
   */
  @Override
  public final void updateMetrics() {
    updateRateAndAlpha();
    updateCostAndSelectivity();
  }

  private void updateCostAndSelectivity() {
    if (tuplesRead.longValue() == 0 || processingTimeNanos.longValue() == 0 || replicaSet.size() == 0) {
      return;
    }
    final double currentSelectivity = tuplesWritten.longValue() / (double) tuplesRead.longValue();
    final double currentCost = processingTimeNanos.longValue() / (double) tuplesRead.longValue();
    this.selectivity.set(movingAverage(currentSelectivity, selectivity.get()));
    this.cost.set(movingAverage(currentCost, cost.get()));
    this.tuplesRead.set(0);
    this.tuplesWritten.set(0);
    this.processingTimeNanos.set(0);
  }

  @Override
  public void updateMetricsForReplica(int replicaIndex, long tuplesRead, long tuplesWritten, long processingTimeNanos) {
    if(replicaSet.contains(replicaIndex)) {
      if(updateInProgress.compareAndSet(false, true)) {
        updateMetrics();
        replicaSet.clear();
        updateInProgress.set(false);
      }
      while (updateInProgress.get()) {
        Thread.yield();
      }
    }
    replicaSet.add(replicaIndex);
    this.tuplesRead.addAndGet(tuplesRead);
    this.tuplesWritten.addAndGet(tuplesWritten);
    this.processingTimeNanos.set(Math.max(processingTimeNanos, this.processingTimeNanos.get()));
    this.lastUpdateFromReplica = System.currentTimeMillis();
  }

  private void updateRateAndAlpha() {
    final long updatePeriod = lastUpdateFromReplica - lastRateAndAlphaUpdateTime;
    if (updatePeriod == 0) {
      return;
    }
    // Update alpha value
    this.alpha = Math.min(TARGET_ALPHA, TARGET_ALPHA * updatePeriod / TARGET_UPDATE_PERIOD);
    final double currentRate = (double) tuplesRead.longValue() / (MILLIS_TO_NANOS * updatePeriod);
    this.rate.set(movingAverage(currentRate, rate.get()));
    this.lastRateAndAlphaUpdateTime = lastUpdateFromReplica;
  }

  private double movingAverage(double newValue, double oldValue) {
    return (alpha * newValue) + ((1 - alpha) * oldValue);
  }

  @Override
  public final double getSelectivity() {
    return selectivity.get();
  }

  @Override
  public final double getCost() {
    return cost.get();
  }

  @Override
  public final double getRate() {
    return rate.get();
  }

  public ComponentType getType() {
    return state.getType();
  }

  public final ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  public final ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
  }

  public void enable() {
    executionTimeMetric.enable();
    rateMetric.enable();
    state.enable();
    tuplesRead.set(0);
    tuplesWritten.set(0);
    processingTimeNanos.set(0);
    selectivity.set(1D);
    cost.set(1D);
    rate.set(0D);
    replicaSet.clear();
  }

  public void disable() {
    state.disable();
    executionTimeMetric.disable();
    rateMetric.disable();
  }

  public boolean isEnabled() {
    return state.isEnabled();
  }

  public String getId() {
    return state.getId();
  }

  public int getIndex() {
    return state.getIndex();
  }

  protected <T> boolean isStreamFinished(T tuple, Stream<T> stream) {
    return (tuple == null) && (stream.isFlushed());
  }

  protected void flush() {
    flushAction();
    LOG.info("{} finished processing", getId());
    this.flushed = true;
  }

  protected abstract void flushAction();


  protected boolean isFlushed() {
    return flushed;
  }

  @Override
  public String toString() {
    return getId();
  }

  @Override
  public long getTuplesRead() {
    return tuplesRead.get();
  }

  @Override
  public long getTuplesWritten() {
    return tuplesWritten.get();
  }

  @Override
  public long getProcessingTimeNanos() {
    return processingTimeNanos.get();
  }

  public ComponentState<IN, OUT> getState() {
    return state;
  }
}
