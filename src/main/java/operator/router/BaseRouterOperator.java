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

package operator.router;

import common.StreamConsumer;
import common.component.ComponentType;
import common.tuple.Tuple;
import java.util.List;
import operator.AbstractOperator;
import org.apache.commons.lang3.Validate;
import scheduling.priority.PriorityMetric;
import stream.Stream;
import stream.StreamFactory;

public class BaseRouterOperator<T extends Tuple> extends AbstractOperator<T, T> implements
    RouterOperator<T> {

  private static final int INPUT_KEY = 0;
  private final ProcessCommandRouter<T> processCommand = new ProcessCommandRouter<>(this);
  protected final RouterFunction<T> router;

  public BaseRouterOperator(String id, StreamFactory streamFactory, RouterFunction<T> router) {
    super(id, ComponentType.ROUTER);
    Validate.notNull(router, "router");
    this.router = router;
  }

  @Override
  public List<String> chooseOperators(T tuple) {
    return router.chooseOperators(tuple);
  }

  @Override
  public void addOutput(StreamConsumer<T> destination, Stream<T> stream) {
    state.addOutput(stream);
  }

  public Stream<T> getOutput() {
    throw new UnsupportedOperationException(
        String.format("'%s': Router has multiple outputs!", state.getId()));
  }

  @Override
  public void run() {
    processCommand.run();
  }

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    processCommand.setMetric(metric);
  }

  @Override
  public void enable() {
    super.enable();
    router.enable();
  }

  @Override
  public void disable() {
    router.disable();
    super.disable();
  }
}
