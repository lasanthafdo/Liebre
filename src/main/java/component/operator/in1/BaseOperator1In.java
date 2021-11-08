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

package component.operator.in1;

import common.tuple.WatermarkedBaseRichTuple;
import component.ComponentType;
import component.operator.AbstractOperator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import stream.Stream;

/**
 * Default abstract implementation of {@link Operator1In}.
 *
 * @param <IN>  The type of input tuples.
 * @param <OUT> The type of output tuples.
 * @author palivosd
 */
public abstract class BaseOperator1In<IN, OUT> extends AbstractOperator<IN, OUT>
    implements Operator1In<IN, OUT> {

  private static final Map<String, Set<String>> eventHistory = new ConcurrentHashMap<>();
  private final String operatorId;
  /**
   * Construct.
   *
   * @param id The unique id of this component.
   */
  public BaseOperator1In(String id) {
    super(id, ComponentType.OPERATOR);
    if(id.contains("_")) {
      this.operatorId = "#" + id.split("_")[0];
    } else {
      this.operatorId = id;
    }
  }

  @Override
  protected final void process() {
    if (isFlushed()) {
      return;
    }

    Stream<IN> input = getInput();
    Stream<OUT> output = getOutput();

    IN inTuple = input.getNextTuple(getIndex());

    if (isStreamFinished(inTuple, input)) {
      flush();
      return;
    }
    //TODO Add instrumentation
    if (inTuple != null) {
      if (inTuple instanceof WatermarkedBaseRichTuple) {
        Set<String> tupleIdSet = eventHistory.computeIfAbsent(operatorId, operatorId -> ConcurrentHashMap.newKeySet());
        if (!tupleIdSet.add(((WatermarkedBaseRichTuple) inTuple).getTupleId())) {
          throw new IllegalStateException("Same tuple is being processed by different replicas");
        }
      }
      increaseTuplesRead();
      List<OUT> outTuples = processTupleIn1(inTuple);
      if (outTuples != null) {
        for (OUT t : outTuples) {
          increaseTuplesWritten();
          output.addTuple(t, getIndex());
        }
      }
    }

  }

}
