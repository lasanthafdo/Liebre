package component;

import common.Active;

import java.io.Serializable;

public interface ComponentFunction extends Active, Serializable {

  default boolean canRun() {
    return true;
  }

  @Override
  default void enable() {}

  @Override
  default boolean isEnabled() {
    return true;
  }

  @Override
  default void disable() {}
}
