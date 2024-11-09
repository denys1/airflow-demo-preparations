from airflow.sensors.filesystem import FileSensor


class SmartFileSensor(FileSensor):

    poke_context_fields = ('filepath', 'fs_conn_id')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = (
            not self.soft_fail
            and super().is_smart_sensor_compatible()
        )
        return result
