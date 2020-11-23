class PointCloudData:
    def __init__(self):
        self.timestamps = []
        self.timestamp_types = []
        self.slot_ids = []
        self.lidar_ids = []
        self.coord1s = []
        self.coord2s = []
        self.coord3s = []
        self.intensities = []
        self.return_nums = []

    def add_entry(self, timestamp, timestamp_type, slot_id, lidar_id, coord1, coord2, coord3, intensity, return_num):
        self.timestamps.append(timestamp)
        self.timestamp_types.append(timestamp_type)
        self.slot_ids.append(slot_id)
        self.lidar_ids.append(lidar_id)
        self.coord1s.append(coord1)
        self.coord2s.append(coord2)
        self.coord3s.append(coord3)
        self.intensities.append(intensity)
        if return_num is not None:
            self.return_nums.append(return_num)
