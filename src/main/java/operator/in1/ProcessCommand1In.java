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

package operator.in1;

import java.util.List;

import common.tuple.Tuple;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommand1In<IN extends Tuple, OUT extends Tuple>
		extends AbstractProcessCommand<Operator1In<IN, OUT>> {

	protected ProcessCommand1In(Operator1In<IN, OUT> operator) {
		super(operator);
	}

	@Override
	public final void process() {
		Stream<IN> input = operator.getInput();
		Stream<OUT> output = operator.getOutput();

		IN inTuple = input.getNextTuple();
		if (inTuple != null) {
			metric.recordTupleRead(inTuple, input);
			List<OUT> outTuples = operator.processTupleIn1(inTuple);
			if (outTuples != null) {
				for (OUT t : outTuples) {
					metric.recordTupleWrite(t, output);
					output.addTuple(t);
				}
			}
		}
	}

}
