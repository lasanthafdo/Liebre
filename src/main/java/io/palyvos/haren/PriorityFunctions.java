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

import static io.palyvos.haren.FeatureHelper.CTYPE_SOURCE;

public class PriorityFunctions {

  private static final SinglePriorityFunction TUPLE_PROCESSING_TIME = new CachingPriorityFunction(
      "TUPLE_PROCESSING_TIME", Feature.COST) {
    @Override
    public double applyWithCachingSupport(Task task, double[][] features) {
      double totalProcessingTime = Feature.COST.get(task, features);
      for (Task downstream : task.getDownstream()) {
        totalProcessingTime += apply(downstream, features);
      }
      return totalProcessingTime;
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }
  };

  private static final SinglePriorityFunction GLOBAL_SELECTIVITY = new CachingPriorityFunction(
      "GLOBAL_SELECTIVITY", Feature.SELECTIVITY) {
    @Override
    public double applyWithCachingSupport(Task task, double[][] features) {
      double globalSelectivity = Feature.SELECTIVITY.get(task, features);
      for (Task downstream : task.getDownstream()) {
        globalSelectivity *= apply(downstream, features);
      }
      return globalSelectivity;
    }
  };

  private static final SinglePriorityFunction HEAD_ARRIVAL_TIME = new AbstractPriorityFunction(
      "HEAD_ARRIVAL_TIME", Feature.HEAD_ARRIVAL_TIME) {
    @Override
    public double apply(Task task, double[][] features) {
      return Feature.HEAD_ARRIVAL_TIME.get(task, features);
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }
  };

  private static final SinglePriorityFunction AVERAGE_ARRIVAL_TIME = new AbstractPriorityFunction(
      "AVERAGE_ARRIVAL_TIME", Feature.AVERAGE_ARRIVAL_TIME) {
    @Override
    public double apply(Task task, double[][] features) {
      return Feature.AVERAGE_ARRIVAL_TIME.get(task, features);
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }

  };

  private static final SinglePriorityFunction SOURCE_AVERAGE_ARRIVAL_TIME =
      new CachingPriorityFunction("SOURCE_AVERAGE_ARRIVAL_TIME", Feature.AVERAGE_ARRIVAL_TIME,
          Feature.COMPONENT_TYPE) {
        @Override
        public double applyWithCachingSupport(Task task, double[][] features) {
          if (Feature.COMPONENT_TYPE.get(task, features) == CTYPE_SOURCE) {
            return AVERAGE_ARRIVAL_TIME.apply(task, features);
          }
          double arrivalTime = Double.MAX_VALUE;
          for (Task upstream : task.getUpstream()) {
            arrivalTime = Math.min(arrivalTime, apply(upstream, features));
          }
          return arrivalTime;
        }
      };

  private static final SinglePriorityFunction GLOBAL_AVERAGE_COST =
      new CachingPriorityFunction("GLOBAL_AVERAGE_COST", Feature.COST, Feature.SELECTIVITY,
          Feature.COMPONENT_TYPE) {

        @Override
        public double applyWithCachingSupport(Task task, double[][] features) {
          double globalAverageCost = Feature.COST.get(task, features);
          double selectivity = Feature.SELECTIVITY.get(task, features);
          for (Task downstream : task.getDownstream()) {
            globalAverageCost += selectivity * apply(downstream, features);
          }
          return globalAverageCost;
        }

        @Override
        public boolean reverseOrder() {
          return true;
        }
      };
  private static final SinglePriorityFunction GLOBAL_RATE =
      new AbstractPriorityFunction("GLOBAL_RATE", GLOBAL_SELECTIVITY, GLOBAL_AVERAGE_COST) {
        @Override
        public double apply(Task task, double[][] features) {
          return GLOBAL_SELECTIVITY.apply(task, features) / GLOBAL_AVERAGE_COST
              .apply(task, features);
        }

      };
  private static final SinglePriorityFunction GLOBAL_NORMALIZED_RATE =
      new AbstractPriorityFunction("GLOBAL_NORMALIZED_RATE", GLOBAL_SELECTIVITY,
          GLOBAL_AVERAGE_COST, TUPLE_PROCESSING_TIME) {
        @Override
        public double apply(Task task, double[][] features) {
          return GLOBAL_SELECTIVITY.apply(task, features) / (GLOBAL_AVERAGE_COST.apply(task,
              features)
              * TUPLE_PROCESSING_TIME.apply(task, features));
        }
      };

  private static final SinglePriorityFunction USER_PRIORITY =
      new AbstractPriorityFunction("USER_PRIORITY", Feature.USER_PRIORITY) {
        @Override
        public double apply(Task task, double[][] features) {
          return Feature.USER_PRIORITY.get(task, features);
        }
      };

  private static final SinglePriorityFunction INPUT_QUEUE_SIZE = new AbstractPriorityFunction(
      "INPUT_QUEUE_SIZE", Feature.INPUT_QUEUE_SIZE) {
    @Override
    public double apply(Task task, double[][] features) {
      return Feature.INPUT_QUEUE_SIZE.get(task, features);
    }
  };

  private static final SinglePriorityFunction OUTPUT_QUEUE_SIZE = new AbstractPriorityFunction(
      "OUTPUT_QUEUE_SIZE", Feature.OUTPUT_QUEUE_SIZE) {
    @Override
    public double apply(Task task, double[][] features) {
      return Feature.OUTPUT_QUEUE_SIZE.get(task, features);
    }

    @Override
    public boolean reverseOrder() {
      return true;
    }
  };


  private PriorityFunctions() {

  }

  public static SinglePriorityFunction averageArrivalTime() {
    return AVERAGE_ARRIVAL_TIME;
  }

  public static SinglePriorityFunction headArrivalTime() {
    return HEAD_ARRIVAL_TIME;
  }

  public static SinglePriorityFunction globalRate() {
    return GLOBAL_RATE;
  }

  public static SinglePriorityFunction globalNormalizedRate() {
    return GLOBAL_NORMALIZED_RATE;
  }

  public static SinglePriorityFunction tupleProcessingTime() {
    return TUPLE_PROCESSING_TIME;
  }

  public static SinglePriorityFunction userPriority() {
    return USER_PRIORITY;
  }

  public static SinglePriorityFunction inputQueueSize() {
    return INPUT_QUEUE_SIZE;
  }

  public static SinglePriorityFunction outputQueueSize() {
    return OUTPUT_QUEUE_SIZE;
  }

  public static SinglePriorityFunction chain() {
    return new ChainPriorityFunction();
  }

  public static SinglePriorityFunction sourceAverageArrivalTime() {
    return SOURCE_AVERAGE_ARRIVAL_TIME;
  }

  static SinglePriorityFunction reciprocalFunction(SinglePriorityFunction function) {
    return new ReciprocalPriorityFunction(function);
  }

  private static class ReciprocalPriorityFunction implements SinglePriorityFunction {

    private static final double PREVENT_DIV_ZERO = Math.pow(10, -10);
    private final SinglePriorityFunction original;

    private ReciprocalPriorityFunction(SinglePriorityFunction original) {
      this.original = original;
    }

    @Override
    public double apply(Task task, double[][] features) {
      return 1 / (original.apply(task, features) + PREVENT_DIV_ZERO);
    }

    @Override
    public Feature[] requiredFeatures() {
      return original.requiredFeatures();
    }

    @Override
    public SinglePriorityFunction enableCaching(int nTasks) {
      return original.enableCaching(nTasks);
    }

    @Override
    public void clearCache() {
      original.clearCache();
    }

    @Override
    public boolean cachingEnabled() {
      return original.cachingEnabled();
    }

    @Override
    public String name() {
      return original.name() + "_reciprocal";
    }
  }

}