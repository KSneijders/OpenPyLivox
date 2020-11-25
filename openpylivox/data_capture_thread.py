import select
import struct
import threading
from pathlib import Path

from openpylivox import helper
from openpylivox.enums import DataType, FileType, FirmwareType
from openpylivox.point_cloud_data import PointCloudData


class DataCaptureThread:

    def __init__(self, sensor_ip, data_socket, imu_socket, file_path_and_name, file_type, secs_to_wait, duration,
                 firmware_type, show_messages, format_spaces, device_type):

        self.start_time = -1
        self.sensor_ip = sensor_ip
        self.d_socket = data_socket
        self.i_socket = imu_socket
        self.file_path_and_name = file_path_and_name
        self.file_type = file_type
        self.secs_to_wait = secs_to_wait
        self.duration = duration
        self.firmware_type = firmware_type
        self.started = True
        self.is_capturing = False
        self.data_type = -1
        self.num_pts = 0
        self.null_pts = 0
        self.imu_records = 0
        self.msg = helper.Msg(
            show_message=show_messages,
            sensor_ip=sensor_ip,
            format_spaces=format_spaces,
            default_arrow="-->"
        )
        self._format_spaces = format_spaces
        self._device_type = device_type
        self.system_status = -1
        self.temp_status = -1
        self.volt_status = -1
        self.motor_status = -1
        self.dirty_status = -1
        self.firmware_status = -1
        self.pps_status = -1
        self.device_status = -1
        self.fan_status = -1
        self.self_heating_status = -1
        self.ptp_status = -1
        self.time_sync_status = -1

        if duration == 0:
            self.duration = helper.get_seconds_in_x_years(years=4)

        if self.file_type == FileType.StoredASCII:
            callback = self.run
        elif self.file_type == FileType.RealtimeASCII:
            callback = self.run_realtime_csv
        elif self.file_type == FileType.RealtimeBINARY:
            callback = self.run_realtime_bin
        else:
            raise ValueError("Unknown filetype.")

        self.thread = threading.Thread(target=callback, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        version, _ = self.loop_until_capturing(verify=True)

        # check data packet is as expected (first byte anyways)
        if version != 5:
            self.msg.prefix_print("Incorrect lidar packet version")
            return

        point_cloud_data = PointCloudData()

        # delayed start to capturing data check (secs_to_wait parameter)
        self.loop_until_wait_time_is_over(self.start_time)

        self.msg.prefix_print("CAPTURING DATA...", f_spaces=2)
        self.duration = helper.adjust_duration(self.firmware_type, self.duration)

        timestamp_sec = self.start_time

        # main loop that captures the desired point cloud data
        while self.started:
            time_since_start = timestamp_sec - self.start_time

            if time_since_start <= self.duration:

                # read data from receive buffer
                if select.select([self.d_socket], [], [], 0)[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)

                    # version = helper.bytes_to_int(data_pc[0:1])  # Unused (?)
                    slot_id = helper.bytes_to_int(data_pc[1:2])
                    lidar_id = helper.bytes_to_int(data_pc[2:3])
                    # byte 3 is reserved

                    # update lidar status information
                    self.update_status(data_pc[4:8])

                    timestamp_type = helper.bytes_to_int(data_pc[8:9])
                    timestamp_sec = helper.get_timestamp(data_pc[10:18], timestamp_type)

                    byte_pos = 18

                    if self.firmware_type in [FirmwareType.SINGLE_RETURN, FirmwareType.DOUBLE_RETURN]:
                        timestamp_step = 0.00001
                    elif self.firmware_type == FirmwareType.TRIPLE_RETURN:
                        timestamp_step = 0.000016666
                    else:
                        raise ValueError("Unknown firmware type.")

                    # to account for first point's timestamp being increment in the loop
                    timestamp_sec -= timestamp_step

                    for i in range(0, 100):
                        # X coordinate / distance
                        coord1 = data_pc[byte_pos:byte_pos + 4]
                        byte_pos += 4

                        if self.data_type == DataType.CARTESIAN:
                            # Y coordinate
                            coord2 = data_pc[byte_pos:byte_pos + 4]
                            byte_pos += 4
                            # Z coordinate
                            coord3 = data_pc[byte_pos:byte_pos + 4]
                            byte_pos += 4
                            # intensity
                            intensity = data_pc[byte_pos:byte_pos + 1]
                            byte_pos += 1
                        elif self.data_type == DataType.SPHERICAL:
                            # zenith
                            coord2 = data_pc[byte_pos:byte_pos + 2]
                            byte_pos += 2
                            # azimuth
                            coord3 = data_pc[byte_pos:byte_pos + 2]
                            byte_pos += 2
                            # intensity
                            intensity = data_pc[byte_pos:byte_pos + 1]
                            byte_pos += 1
                        else:
                            raise ValueError("Unknown datatype.")

                        return_num = None
                        if self.firmware_type == FirmwareType.SINGLE_RETURN:
                            timestamp_sec += timestamp_step
                        else:
                            mod_firmware_type = i % self.firmware_type
                            timestamp_sec += float(not mod_firmware_type) * timestamp_step
                            return_num = mod_firmware_type + 1

                        point_cloud_data.add_entry(
                            timestamp=timestamp_sec,
                            timestamp_type=timestamp_type,
                            slot_id=slot_id,
                            lidar_id=lidar_id,
                            coord1=coord1,
                            coord2=coord2,
                            coord3=coord3,
                            intensity=intensity,
                            return_num=return_num
                        )
            else:
                self.started = False
                self.is_capturing = False
                break

        # make sure some data was captured
        len_data = len(point_cloud_data.coord1s)
        if len_data > 0:
            self.msg.prefix_print(f"writing data to ASCII file: {self.file_path_and_name}", f_spaces=2)

            csv_file = open(self.file_path_and_name, "w")
            num_pts = 0
            null_pts = 0

            # TODO: apply coordinate transformations to the raw X, Y, Z point cloud data based on the extrinsic
            #  parameters rotation definitions and the sequence they are applied is always a bit of a head
            #  scratcher, lots of different definitions Geospatial/Traditional Photogrammetry/Computer
            #  Vision/North America/Europe all use different approaches

            if self.data_type == DataType.CARTESIAN:  # Cartesian
                csv_file.write("//X,Y,Z,Intensity,Time\n")
            elif self.data_type == DataType.SPHERICAL:  # Spherical
                csv_file.write("//Distance,Zenith,Azimuth,Intensity,Time\n")
            else:
                raise ValueError("Unknown firmware type.")

            for i in range(len_data):
                if self.data_type == DataType.CARTESIAN:
                    coord1 = round(float(struct.unpack('<i', point_cloud_data.coord1s[i])[0]) / 1000.0, 3)
                    coord2 = round(float(struct.unpack('<i', point_cloud_data.coord2s[i])[0]) / 1000.0, 3)
                    coord3 = round(float(struct.unpack('<i', point_cloud_data.coord3s[i])[0]) / 1000.0, 3)

                    if coord1 or coord2 or coord3:
                        num_pts += 1
                elif self.data_type == DataType.SPHERICAL:
                    coord1 = round(float(struct.unpack('<I', point_cloud_data.coord1s[i])[0]) / 1000.0, 3)
                    coord2 = round(float(struct.unpack('<H', point_cloud_data.coord2s[i])[0]) / 100.0, 2)
                    coord3 = round(float(struct.unpack('<H', point_cloud_data.coord3s[i])[0]) / 100.0, 2)

                    if coord1:
                        num_pts += 1
                    else:
                        null_pts += 1
                else:
                    raise ValueError("Unknown datatype.")

                return_nums_end = "\n"
                if self.firmware_type in [FirmwareType.DOUBLE_RETURN, FirmwareType.TRIPLE_RETURN]:
                    return_nums_end = str(point_cloud_data.return_nums[i]) + return_nums_end

                csv_file.write(
                    f"{coord1},{coord2},{coord3}," +
                    f"{helper.bytes_to_int(point_cloud_data.intensities[i])}," +
                    f"{round(point_cloud_data.timestamps[i], 6)}" +
                    return_nums_end
                )

            self.num_pts = num_pts
            self.null_pts = null_pts

            self.msg.prefix_print(f"closed ASCII file: {self.file_path_and_name}", f_spaces=2)
            self.msg.space_print(20, f"(points: {num_pts} good, {null_pts} null, {num_pts + null_pts} total)")
            csv_file.close()
        else:
            self.msg.prefix_print("WARNING: no point cloud data was captured")

    def run_realtime_csv(self):
        # read point cloud data packet to get packet version and datatype
        # keep looping to 'consume' data that we don't want included in the captured point cloud data
        version, _ = self.loop_until_capturing(verify=True)

        # check data packet is as expected (first byte anyways)
        if version != 5:
            self.msg.prefix_print("Incorrect lidar packet version")
            return

        # delayed start to capturing data check (secsToWait parameter)
        self.loop_until_wait_time_is_over(self.start_time)
        self.msg.prefix_print("CAPTURING DATA...")

        # duration adjustment (trying to get exactly 100,000 points / sec)
        if self.duration != helper.get_seconds_in_x_years(years=4):
            if self.firmware_type == FirmwareType.SINGLE_RETURN:
                self.duration += (0.001 * (self.duration / 2.0))
            elif self.firmware_type == FirmwareType.DOUBLE_RETURN:
                self.duration += (0.0005 * (self.duration / 2.0))
            elif self.firmware_type == FirmwareType.TRIPLE_RETURN:
                self.duration += (0.00055 * (self.duration / 2.0))

        timestamp_sec = self.start_time

        self.msg.prefix_print(f"writing real-time data to ASCII file: {self.file_path_and_name}")
        csv_file = open(self.file_path_and_name, "w", 1)

        num_pts = 0
        null_pts = 0

        # gather and write header info
        if self.data_type == DataType.CARTESIAN:  # Cartesian
            header_string = "//X,Y,Z,Intensity,Time"
        elif self.data_type == DataType.SPHERICAL:  # Spherical
            header_string = "//Distance,Zenith,Azimuth,Intensity,Time"
        else:
            raise ValueError("Unknown datatype.")

        if self.firmware_type in [FirmwareType.DOUBLE_RETURN, FirmwareType.TRIPLE_RETURN]:
            header_string += ",ReturnNum"
        csv_file.write(header_string + "\n")

        # main loop that captures the desired point cloud data
        while self.started:
            time_since_start = timestamp_sec - self.start_time
            if time_since_start <= self.duration:

                # read data from receive buffer
                if select.select([self.d_socket], [], [], 0)[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)

                    # update lidar status information
                    self.update_status(data_pc[4:8])

                    timestamp_type = helper.bytes_to_int(data_pc[8:9])
                    timestamp_sec = helper.get_timestamp(data_pc[10:18], timestamp_type)

                    byte_pos = 18

                    if self.firmware_type in [FirmwareType.SINGLE_RETURN, FirmwareType.DOUBLE_RETURN]:
                        timestamp_step = 0.00001
                    elif self.firmware_type == FirmwareType.TRIPLE_RETURN:
                        timestamp_step = 0.000016666
                    else:
                        raise ValueError("Unknown firmware type.")

                    # to account for first point's timestamp being increment in the loop
                    timestamp_sec -= timestamp_step

                    for i in range(0, 100):
                        return_num = None
                        if self.firmware_type == FirmwareType.SINGLE_RETURN:
                            timestamp_sec += timestamp_step
                        else:
                            mod_firmware_type = i % self.firmware_type
                            timestamp_sec += float(not mod_firmware_type) * timestamp_step
                            return_num = mod_firmware_type + 1
                        timestamp_sec += timestamp_step

                        if self.data_type == DataType.CARTESIAN:
                            coord2 = struct.unpack('<i', data_pc[byte_pos + 4:byte_pos + 8])[0]  # Y coordinate
                            if coord2:
                                coord1 = struct.unpack('<i', data_pc[byte_pos:byte_pos + 4])[0]  # X coordinate
                                byte_pos += 8
                                coord3 = struct.unpack('<i', data_pc[byte_pos:byte_pos + 4])[0]  # Z coordinate
                                byte_pos += 4
                                intensity = helper.bytes_to_int(data_pc[byte_pos:byte_pos + 1])  # intensity

                                num_pts += 1
                            else:
                                null_pts += 1
                                byte_pos += 13
                                continue
                        elif self.data_type == DataType.SPHERICAL:
                            coord1 = struct.unpack('<I', data_pc[byte_pos:byte_pos + 4])[0]  # Distance coordinate
                            if coord1:
                                byte_pos += 4
                                coord2 = struct.unpack('<H', data_pc[byte_pos:byte_pos + 2])[0]  # Zenith coordinate
                                byte_pos += 2
                                coord3 = struct.unpack('<H', data_pc[byte_pos:byte_pos + 2])[0]  # Azimuth coordinate
                                byte_pos += 2
                                intensity = helper.bytes_to_int(data_pc[byte_pos:byte_pos + 1])  # intensity
                                byte_pos += 1

                                num_pts += 1
                            else:
                                null_pts += 1
                                byte_pos += 9
                                continue
                        else:
                            raise ValueError("Unknown DataType.")

                        return_nums_end = "\n"
                        if self.firmware_type in [FirmwareType.DOUBLE_RETURN, FirmwareType.TRIPLE_RETURN]:
                            return_nums_end = str(return_num) + return_nums_end

                        division = 3
                        if self.data_type == DataType.SPHERICAL:
                            division = 2
                        csv_file.write(
                            f"{round(float(coord1) / 1000.0, 3)},"
                            f"{round(float(coord2) / pow(10, division), division)},"
                            f"{round(float(coord3) / pow(10, division), division)},"
                            f"{intensity},{round(timestamp_sec, 6)},{return_nums_end}"
                        )
            # duration check (exit point)
            else:
                self.started = False
                self.is_capturing = False
                break

        self.num_pts = num_pts
        self.null_pts = null_pts

        self.msg.prefix_print(f"closed ASCII file: {self.file_path_and_name}")
        self.msg.space_print(32, f"(points: {num_pts} good, {null_pts} null, {num_pts + null_pts} total)")
        csv_file.close()

    def run_realtime_bin(self):
        # used to check if the sensor is a Mid-100
        device_check = 0
        try:
            device_check = int(self._device_type[4:7])
        except:  # Todo: Add type of exception. To know which, use a non Mid-100 and comment this code
            pass

        version, _ = self.loop_until_capturing(verify=True)

        # check data packet is as expected (first byte anyways)
        if version != 5:
            self.msg.prefix_print("Incorrect lidar packet version")
            return

        self.loop_until_wait_time_is_over(self.start_time, check_i_socket=True)
        self.msg.prefix_print("CAPTURING DATA...")

        timestamp_sec = self.start_time

        self.msg.prefix_print(f"writing real-time data to BINARY file: {self.file_path_and_name}")
        bin_file = open(self.file_path_and_name, "wb")
        imu_file = None
        imu_reporting = False

        num_pts = 0
        null_pts = 0
        imu_records = 0

        # write header info to know how to parse the data later
        bin_file.write(str.encode("OPENPYLIVOX"))
        bin_file.write(struct.pack('<h', self.firmware_type))
        bin_file.write(struct.pack('<h', self.data_type))

        # main loop that captures the desired point cloud data
        while self.started:
            time_since_start = timestamp_sec - self.start_time
            if time_since_start <= self.duration:

                # read points from data buffer
                if select.select([self.d_socket], [], [], 0)[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)

                    # update lidar status information
                    self.update_status(data_pc[4:8])
                    data_type = int.from_bytes(data_pc[9:10], byteorder='little')
                    timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                    timestamp_sec = helper.get_timestamp(data_pc[10:18], timestamp_type)

                    byte_pos = 18

                    loops = 100
                    timestamp_step = 0.00001
                    if data_type in [2, 3]:  # [Cartesian, Spherical] -> Horizon and Tele-15 sensors (single return)
                        loops = 96
                        timestamp_step = 0.000004167
                    elif data_type in [4, 5]:  # [Cartesian, Spherical] -> Horizon and Tele-15 sensors (dual return)
                        loops = 48
                        timestamp_step = 0.000002083
                    if self.firmware_type == FirmwareType.TRIPLE_RETURN:
                        timestamp_step = 0.000016666

                    return_num = None
                    timestamp_sec -= timestamp_step
                    for i in range(loops):
                        if self.firmware_type in [FirmwareType.DOUBLE_RETURN, FirmwareType.TRIPLE_RETURN]:
                            mod_firmware_type = i % self.firmware_type
                            timestamp_sec += float(not mod_firmware_type) * timestamp_step
                            return_num = mod_firmware_type + 1
                        else:
                            timestamp_sec += timestamp_step

                        if data_type in [DataType.CARTESIAN, 2, 4]:  # 2,4 are Cartesian
                            coord = struct.unpack('<i', data_pc[byte_pos + 4:byte_pos + 8])[0]
                        elif data_type in [DataType.CARTESIAN, 3, 5]:  # 3, 5 are Spherical
                            coord = struct.unpack('<I', data_pc[byte_pos:byte_pos + 4])[0]
                        else:
                            raise ValueError("Unknown datatype.")

                        # Only check for `device == 100` in specific scenario
                        check_for_device_type = (
                                self.firmware_type == FirmwareType.SINGLE_RETURN and
                                data_type == DataType.CARTESIAN
                        )

                        if data_type == DataType.CARTESIAN:
                            jump_bytes = 13
                        elif data_type == DataType.SPHERICAL:
                            jump_bytes = 9
                        elif data_type == 2:  # Cartesian -> Horizon and Tele-15 sensors (single return)
                            jump_bytes = 14
                        elif data_type == 3:  # Spherical -> Horizon and Tele-15 sensors (single return)
                            jump_bytes = 10
                        elif data_type == 4:  # Cartesian -> Horizon and Tele-15 sensors (dual return)
                            jump_bytes = 28
                        elif data_type == 5:  # Spherical -> Horizon and Tele-15 sensors (dual return)
                            jump_bytes = 16
                        else:
                            raise ValueError("Unknown datatype.")

                        if (check_for_device_type and device_check == 100) or coord:
                            num_pts += 1
                            bin_file.write(data_pc[byte_pos:byte_pos + jump_bytes])
                            bin_file.write(struct.pack('<d', timestamp_sec))
                            if self.firmware_type in [FirmwareType.DOUBLE_RETURN, FirmwareType.TRIPLE_RETURN]:
                                bin_file.write(str.encode(str(return_num)))
                        else:
                            null_pts += 1
                        byte_pos += jump_bytes

                # IMU data capture
                if select.select([self.i_socket], [], [], 0)[0]:
                    imu_data, addr2 = self.i_socket.recvfrom(50)

                    data_type = helper.bytes_to_int(imu_data[9:10])
                    timestamp_type = helper.bytes_to_int(imu_data[8:9])
                    timestamp_sec = helper.get_timestamp(imu_data[10:18], timestamp_type)

                    byte_pos = 18

                    # Horizon and Tele-15 IMU data packet
                    if data_type == 6:
                        if not imu_reporting:
                            imu_reporting = True
                            path_file = Path(self.file_path_and_name)
                            filename = path_file.stem
                            extension = path_file.suffix
                            imu_file = open(filename + "_IMU" + extension, "wb")
                            imu_file.write(str.encode("OPENPYLIVOX_IMU"))

                        imu_file.write(imu_data[byte_pos:byte_pos + 24])
                        imu_file.write(struct.pack('<d', timestamp_sec))
                        imu_records += 1
            else:  # duration check (exit point)
                self.started = False
                self.is_capturing = False
                break

        self.num_pts = num_pts
        self.null_pts = null_pts
        self.imu_records = imu_records

        self.msg.prefix_print(f"closed BINARY file: {self.file_path_and_name}")
        self.msg.space_print(32, f"(points: {num_pts} good, {null_pts} null, {num_pts + null_pts} total)")
        if self._device_type == "Horizon" or self._device_type == "Tele-15":
            self.msg.space_print(32, f"(IMU records: {imu_records})")

        bin_file.close()

        if imu_reporting:
            imu_file.close()

    def loop_until_capturing(self, verify=True):
        """
        Read point cloud data packet to get packet version and datatype. Keep looping to 'consume' data that we don't
        want included in the captured point cloud data.
        """
        break_by_capture = False
        version = None
        while self.started:
            select_test = select.select([self.d_socket], [], [], 0)
            if select_test[0]:
                data_pc, addr = self.d_socket.recvfrom(1500)
                version = helper.bytes_to_int(data_pc[0:1])
                self.data_type = helper.bytes_to_int(data_pc[9:10])
                timestamp_type = helper.bytes_to_int(data_pc[8:9])
                timestamp1 = helper.get_timestamp(data_pc[10:18], timestamp_type)
                self.update_status(data_pc[4:8])
                if self.is_capturing:
                    self.start_time = timestamp1
                    break_by_capture = True
                    break
        if verify:
            if version is None:
                raise ValueError("Unable to detect version.")
            if not break_by_capture:
                raise ValueError("Unable to start capturing. Not started yet.")
        return version, break_by_capture

    def loop_until_wait_time_is_over(self, start_time, check_i_socket=False):
        while self.started:
            time_since_start = start_time - self.start_time
            if time_since_start <= self.secs_to_wait:
                # read data from receive buffer and keep 'consuming' it
                if select.select([self.d_socket], [], [], 0)[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)
                    timestamp_type = helper.bytes_to_int(data_pc[8:9])
                    start_time = helper.get_timestamp(data_pc[10:18], timestamp_type)
                    self.update_status(data_pc[4:8])
                if check_i_socket:
                    if select.select([self.i_socket], [], [], 0)[0]:
                        imu_data, addr2 = self.i_socket.recvfrom(50)
            else:
                self.start_time = start_time
                break

    # parse lidar status codes and update object properties, can provide real-time warning/error message display
    def update_status(self, data_pc):

        status_bits = str(bin(helper.bytes_to_int(data_pc[0:1])))[2:].zfill(8)
        status_bits += str(bin(helper.bytes_to_int(data_pc[1:2])))[2:].zfill(8)
        status_bits += str(bin(helper.bytes_to_int(data_pc[2:3])))[2:].zfill(8)
        status_bits += str(bin(helper.bytes_to_int(data_pc[3:4])))[2:].zfill(8)

        self.temp_status = int(status_bits[0:2], 2)
        self.volt_status = int(status_bits[2:4], 2)
        self.motor_status = int(status_bits[4:6], 2)
        self.dirty_status = int(status_bits[6:8], 2)
        self.firmware_status = int(status_bits[8:9], 2)
        self.pps_status = int(status_bits[9:10], 2)
        self.device_status = int(status_bits[10:11], 2)
        self.fan_status = int(status_bits[11:12], 2)
        self.self_heating_status = int(status_bits[12:13], 2)
        self.ptp_status = int(status_bits[13:14], 2)
        self.time_sync_status = int(status_bits[14:16], 2)
        self.system_status = int(status_bits[30:], 2)

        # check if the system status in NOT normal
        if self.system_status:
            if self.system_status == 1:
                if self.temp_status == 1:
                    self.msg.prefix_print("* WARNING: temperature *")
                if self.volt_status == 1:
                    self.msg.prefix_print("* WARNING: voltage *")
                if self.motor_status == 1:
                    self.msg.prefix_print("* WARNING: motor *")
                if self.dirty_status == 1:
                    self.msg.prefix_print("* WARNING: dirty or blocked *")
                if self.device_status == 1:
                    self.msg.prefix_print("* WARNING: approaching end of service life *")
                if self.fan_status == 1:
                    self.msg.prefix_print("* WARNING: fan *")
            elif self.system_status == 2:
                if self.temp_status == 2:
                    self.msg.prefix_print("*** ERROR: TEMPERATURE ***")
                if self.volt_status == 2:
                    self.msg.prefix_print("*** ERROR: VOLTAGE ***")
                if self.motor_status == 2:
                    self.msg.prefix_print("*** ERROR: MOTOR ***")
                if self.firmware_status == 1:
                    self.msg.prefix_print("*** ERROR: ABNORMAL FIRMWARE ***")

    # returns latest status Codes from within the point cloud data packet
    def status_codes(self):
        return [
            self.system_status,
            self.temp_status,
            self.volt_status,
            self.motor_status,
            self.dirty_status,
            self.firmware_status,
            self.pps_status,
            self.device_status,
            self.fan_status,
            self.self_heating_status,
            self.ptp_status,
            self.time_sync_status
        ]

    def stop(self):
        self.started = False
        self.thread.join()
