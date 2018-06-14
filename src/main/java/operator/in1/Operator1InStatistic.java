/*
 * Copyright (C) 2017-2018
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package operator.in1;

import common.statistic.AverageStatistic;
import java.util.List;

import common.statistic.CountStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;

public class Operator1InStatistic<IN extends Tuple, OUT extends Tuple> extends
    Operator1InDecorator<IN, OUT> {

  private final AverageStatistic processingTimeStatistic;
  private final AverageStatistic executionTimeStatistic;
  private final CountStatistic timesScheduledStatistic;
  private final CountStatistic timesRunStatistic;

  public Operator1InStatistic(Operator1In<IN, OUT> operator, String outputFolder) {
    this(operator, outputFolder, true);
  }

  public Operator1InStatistic(Operator1In<IN, OUT> operator, String outputFolder,
      boolean autoFlush) {
    super(operator);
    this.processingTimeStatistic = new AverageStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "proc"), autoFlush);
    this.executionTimeStatistic = new AverageStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "exec"), autoFlush);
    this.timesScheduledStatistic = new CountStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "sched"), autoFlush);
    this.timesRunStatistic = new CountStatistic(
        StatisticFilename.INSTANCE.get(outputFolder, operator, "runs"),
        autoFlush);
  }

  @Override
  public void enable() {
    super.enable();
    processingTimeStatistic.enable();
    executionTimeStatistic.enable();
    timesScheduledStatistic.enable();
    timesRunStatistic.enable();
  }

  @Override
  public void disable() {
    processingTimeStatistic.disable();
    executionTimeStatistic.disable();
    timesScheduledStatistic.disable();
    timesRunStatistic.disable();
    super.disable();
  }

  @Override
  public void onScheduled() {
    timesScheduledStatistic.append(1L);
    super.onScheduled();
  }

  @Override
  public void onRun() {
    timesRunStatistic.append(1L);
    super.onRun();
  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {
    long start = System.nanoTime();
    List<OUT> outTuples = super.processTupleIn1(tuple);
    processingTimeStatistic.append(System.nanoTime() - start);
    return outTuples;
  }

  @Override
  public void run() {
    long start = System.nanoTime();
    super.run();
    executionTimeStatistic.append(System.nanoTime() - start);
  }
}
