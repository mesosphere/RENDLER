nameFor = {
    6: "TASK_STAGING",  # Initial state. Framework status updates should not use.
    0: "TASK_STARTING",
	1: "TASK_RUNNING",
	2: "TASK_FINISHED", # TERMINAL.
	3: "TASK_FAILED",   # TERMINAL.
	4: "TASK_KILLED",   # TERMINAL.
	5: "TASK_LOST"      # TERMINAL.
}
