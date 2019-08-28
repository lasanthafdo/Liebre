/*
 * Copyright (C) 2017-2019
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

package io.palyvos.haren;

import common.statistic.AbstractCummulativeStatistic;
import common.statistic.HistogramStatistic;
import common.statistic.MeterStatistic;
import common.util.StatisticPath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReconfigurationAction implements Runnable {

  static final String STATISTIC_CALLS = "priocalls";
  static final String STATISTIC_TIME = "priotime";
  static final String BARRIER_INFO = "barrier-info";

  private static final Logger LOG = LogManager.getLogger();
  private final List<Task> tasks;
  private final List<AbstractExecutor> executors;
  private final SchedulerState state;
  private final AbstractCummulativeStatistic totalCalls;
  private final AbstractCummulativeStatistic updateTime;
  private final AbstractCummulativeStatistic deploymentTime;
  private final AbstractCummulativeStatistic barrierEnterVariance;
  private final AbstractCummulativeStatistic barrierExitVariance;
  private boolean firstUpdate = true;

  public ReconfigurationAction(List<Task> inputTasks, List<AbstractExecutor> executors,
      SchedulerState state) {
    this.tasks = new ArrayList(inputTasks);
    Collections.sort(tasks, Comparator.comparingInt(Task::getIndex));
    this.executors = executors;
    this.state = state;
    this.state.init(tasks);

    // Statistics Initialization
    this.totalCalls = new MeterStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "Total-Calls"), STATISTIC_CALLS), false);
    totalCalls.enable();
    this.updateTime = new MeterStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "Update-Features"), STATISTIC_TIME), false);
    updateTime.enable();
    this.deploymentTime = new MeterStatistic(
        StatisticPath.get(state.statisticsFolder, statisticName(
            "Deploy-Tasks"), STATISTIC_TIME), false);
    deploymentTime.enable();
    this.barrierEnterVariance = new HistogramStatistic(
        StatisticPath.get(state.statisticsFolder, statisticName(
            "Enter-Variance"), BARRIER_INFO), false);
    barrierEnterVariance.enable();
    this.barrierExitVariance = new HistogramStatistic(
        StatisticPath.get(state.statisticsFolder, statisticName(
            "Exit-Variance"), BARRIER_INFO), false);
    barrierExitVariance.enable();

  }

  static String statisticName(String action) {
    return String.format("%s-Priority-Update", action);
  }

  @Override
  public void run() {
    Validate.isTrue(tasks.size() > 0, "No tasks given!");
    if (firstUpdate) {
      updateAllFeatures();
      firstUpdate = false;
    } else {
      updateFeaturesWithDependencies();
    }
    state.priorityFunction().clearCache();
    List<List<Task>> assignments = deployTasks();
    assignTasks(assignments);
    state.updateRoundEndTime();
    totalCalls.append(1);
    barrierEnterVariance.append(state.barrierEnterVariance());
    barrierExitVariance.append(state.barrierExitVariance());
  }

  private void updateFeaturesWithDependencies() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      if (state.resetUpdated(task)) {
        task.updateFeatures(state.variableFeaturesWithDependencies(),
            state.taskFeatures[task.getIndex()]);
      }
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private void updateAllFeatures() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      task.refreshFeatures();
      task.updateFeatures(state.constantFeatures(), state.taskFeatures[task.getIndex()]);
      task.updateFeatures(state.variableFeaturesNoDependencies(),
          state.taskFeatures[task.getIndex()]);
      task.updateFeatures(state.variableFeaturesWithDependencies(),
          state.taskFeatures[task.getIndex()]);
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private List<List<Task>> deployTasks() {
    long startTime = System.currentTimeMillis();
    List<List<Task>> assignments = state.deploymentFunction().getDeployment(executors.size());
    deploymentTime.append(System.currentTimeMillis() - startTime);
    return assignments;
  }

  private void assignTasks(List<List<Task>> assignments) {
    Validate.isTrue(assignments.size() <= executors.size(), "#assignments > #threads");
    for (int threadId = 0; threadId < executors.size(); threadId++) {
      // Give no work to executors with no assignment
      List<Task> assignment =
          threadId < assignments.size() ? assignments.get(threadId) : Collections.emptyList();
      executors.get(threadId).setTasks(assignment);
    }
  }

  void disable() {
    totalCalls.disable();
    updateTime.disable();
    deploymentTime.disable();
    barrierEnterVariance.disable();
    barrierExitVariance.disable();
  }

}