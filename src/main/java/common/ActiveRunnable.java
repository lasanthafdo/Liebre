package common;

public interface ActiveRunnable extends Active, Runnable {
	void onScheduled();

	void onRun();
}
