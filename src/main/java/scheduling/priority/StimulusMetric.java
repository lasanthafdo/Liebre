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

package scheduling.priority;

import java.util.List;

import common.component.Component;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import stream.Stream;

public class StimulusMetric extends PriorityMetric {

  public StimulusMetric(List<Component> tasks, List<Component> ignoredTasks) {
    super(tasks, ignoredTasks);
  }

  @Override
  public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
  }

  @Override
  public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
  }

  @Override
  public List<Double> getPriorities(int scaleFactor) {
    long[] priorities = new long[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      priorities[i] = getPriority(tasks.get(i));
    }
    return scale(priorities, scaleFactor);
  }

  private long getPriority(Component task) {
    if (isIgnored(task)) {
      return 0;
    }
    long latency = 0;
    for (Stream<?> input : getInputs(task)) {
      // FIXME: Streams could save the latest ts in a volatile variable
      // to remove the peek() call
      Tuple t = input.peek();
      if (t instanceof RichTuple) {
        //FIXME: ExtendedRichTuple that contains STIMULUS
        long ts = ((RichTuple) t).getTimestamp();
        latency = Math.max(System.nanoTime() - ts, latency);
      }
    }
    return latency;
  }

}
