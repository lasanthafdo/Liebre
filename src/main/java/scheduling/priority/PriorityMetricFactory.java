package scheduling.priority;

import java.util.List;

import common.ActiveRunnable;

public enum PriorityMetricFactory {
	STIMULUS_MATRIX {
		@Override
		public PriorityMetric newInstance(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
			return new StimulusMatrixMetric(tasks, passiveTasks, nThreads);
		}
	},
	QUEUE_SIZE_MATRIX {
		@Override
		public PriorityMetric newInstance(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
			return new QueueSizeMatrixMetric(tasks, passiveTasks, nThreads);
		}
	},
	STIMULUS {
		@Override
		public PriorityMetric newInstance(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
			return new StimulusMetric(tasks, passiveTasks);
		}
	},
	QUEUE_SIZE {
		@Override
		public PriorityMetric newInstance(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
			return new QueueSizeMetric(tasks, passiveTasks);
		}

	},
	CONTROL {
		@Override
		public PriorityMetric newInstance(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks, int nThreads) {
			return new ControlPriorityMetric(tasks, passiveTasks);
		}
	};
	public abstract PriorityMetric newInstance(List<ActiveRunnable> tasks, List<ActiveRunnable> passiveTasks,
			int nThreads);
}