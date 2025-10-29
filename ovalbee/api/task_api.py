from ovalbee.api.module_api import ModuleApi


class TaskStatus(str):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class TaskApi(ModuleApi):

    def _endpoint_prefix(self) -> str:
        return "tasks"

    def update_status(self, task_id: int, status: TaskStatus) -> None:
        method = f"{self.endpoint}/{task_id}/status"
        data = {"status": status}
        self._api.put(method, data=data)
