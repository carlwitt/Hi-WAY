package de.huberlin.wbi.hiway.common;

import de.huberlin.wbi.hiway.common.TaskInstance;

public class HiWayInvocation {
	public final TaskInstance task;
	public final long timestamp;

	public HiWayInvocation(TaskInstance task) {
		this.task = task;
		timestamp = System.currentTimeMillis();
	}
}
