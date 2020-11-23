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
        self.msg = helper.Msg(show_message=show_messages)
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
        if version == 5:
            point_cloud_data = PointCloudData()

            # delayed start to capturing data check (secs_to_wait parameter)
            self.loop_until_wait_time_is_over(self.start_time)

            self.msg.print(f"   {self.sensor_ip}{self._format_spaces * 2}   -->     CAPTURING DATA...")
            self.duration = helper.adjust_duration(self.firmware_type, self.duration)

            timestamp_sec = self.start_time

            # main loop that captures the desired point cloud data
            while self.started:
                time_since_start = timestamp_sec - self.start_time

                if time_since_start <= self.duration:

                    # read data from receive buffer
                    if select.select([self.d_socket], [], [], 0)[0]:
                        data_pc, addr = self.d_socket.recvfrom(1500)

                        # version = helper.bytes_to_int(data_pc[0:1])  # Unused
                        slot_id = helper.bytes_to_int(data_pc[1:2])
                        lidar_id = helper.bytes_to_int(data_pc[2:3])

                        # byte 3 is reserved

                        # update lidar status information
                        self.updateStatus(data_pc[4:8])

                        timestamp_type = helper.bytes_to_int(data_pc[8:9])
                        timestamp_sec = self.getTimestamp(data_pc[10:18], timestamp_type)

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

                self.msg.print(
                    f"   {self.sensor_ip}{self._format_spaces * 2}   -->     "
                    f"writing data to ASCII file: {self.file_path_and_name}"
                )

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
                        "{0:.6f}".format(point_cloud_data.timestamps[i]) +
                        return_nums_end
                    )

                self.num_pts = num_pts
                self.null_pts = null_pts

                self.msg.print(f"   {self.sensor_ip}{self._format_spaces * 2}   -->     "
                               f"closed ASCII file: {self.file_path_and_name}")
                self.msg.print(f"{' ' * 20}(points: {num_pts} good, {null_pts} null, {num_pts + null_pts} total)")
                csv_file.close()

            else:
                self.msg.print(f"   {self.sensor_ip}{self._format_spaces}   -->     "
                               f"WARNING: no point cloud data was captured")

        else:
            self.msg.print(f"   {self.sensor_ip}{self._format_spaces}   -->     Incorrect lidar packet version")

    def run_realtime_csv(self):
        # read point cloud data packet to get packet version and datatype
        # keep looping to 'consume' data that we don't want included in the captured point cloud data
        version, _ = self.loop_until_capturing(verify=True)

        # check data packet is as expected (first byte anyways)
        if version == 5:
            # delayed start to capturing data check (secsToWait parameter)
            self.loop_until_wait_time_is_over(self.start_time)
            self.msg.print(f"   {self.sensor_ip}{self._format_spaces}   -->     CAPTURING DATA...")

            # duration adjustment (trying to get exactly 100,000 points / sec)
            if self.duration != helper.get_seconds_in_x_years(years=4):
                if self.firmware_type == FirmwareType.SINGLE_RETURN:
                    self.duration += (0.001 * (self.duration / 2.0))
                elif self.firmware_type == FirmwareType.DOUBLE_RETURN:
                    self.duration += (0.0005 * (self.duration / 2.0))
                elif self.firmware_type == FirmwareType.TRIPLE_RETURN:
                    self.duration += (0.00055 * (self.duration / 2.0))

            timestamp_sec = self.start_time

            self.msg.print(
                "   " + self.sensor_ip + self._format_spaces + "   -->     writing real-time data to ASCII file: " + self.file_path_and_name)
            csvFile = open(self.file_path_and_name, "w", 1)

            numPts = 0
            nullPts = 0

            # write header info
            if self.firmware_type == FirmwareType.SINGLE_RETURN:
                if self.data_type == DataType.CARTESIAN:  # Cartesian
                    csvFile.write("//X,Y,Z,Inten-sity,Time\n")
                elif self.data_type == DataType.SPHERICAL:  # Spherical
                    csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time\n")
            elif self.firmware_type in [FirmwareType.DOUBLE_RETURN, FirmwareType.TRIPLE_RETURN]:
                if self.data_type == DataType.CARTESIAN:  # Cartesian
                    csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                elif self.data_type == DataType.SPHERICAL:  # Spherical
                    csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")

            # main loop that captures the desired point cloud data
            while True:
                if self.started:
                    time_since_start = timestamp_sec - self.start_time

                    if time_since_start <= self.duration:

                        # read data from receive buffer
                        if select.select([self.d_socket], [], [], 0)[0]:
                            data_pc, addr = self.d_socket.recvfrom(1500)

                            # version = int.from_bytes(data_pc[0:1], byteorder='little')
                            # slot_id = int.from_bytes(data_pc[1:2], byteorder='little')
                            # lidar_id = int.from_bytes(data_pc[2:3], byteorder='little')

                            # byte 3 is reserved

                            # update lidar status information
                            self.updateStatus(data_pc[4:8])

                            timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                            timestamp_sec = self.getTimestamp(data_pc[10:18], timestamp_type)

                            bytePos = 18

                            # single return firmware (most common)
                            if self.firmware_type == FirmwareType.SINGLE_RETURN:
                                # to account for first point's timestamp being increment in the loop
                                timestamp_sec -= 0.00001

                                # Cartesian Coordinate System
                                if self.data_type == DataType.CARTESIAN:
                                    for i in range(0, 100):

                                        # Y coordinate (check for non-zero)
                                        coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                        # timestamp
                                        timestamp_sec += 0.00001

                                        if coord2:
                                            # X coordinate
                                            coord1 = struct.unpack('<i', data_pc[bytePos:bytePos + 4])[0]
                                            bytePos += 8
                                            # Z coordinate
                                            coord3 = struct.unpack('<i', data_pc[bytePos:bytePos + 4])[0]
                                            bytePos += 4
                                            # intensity
                                            intensity = int.from_bytes(data_pc[bytePos:bytePos + 1],
                                                                       byteorder='little')
                                            bytePos += 1

                                            numPts += 1
                                            csvFile.write(
                                                "{0:.3f}".format(float(coord1) / 1000.0) + "," + "{0:.3f}".format(
                                                    float(coord2) / 1000.0) + "," + "{0:.3f}".format(
                                                    float(coord3) / 1000.0) + "," + str(
                                                    intensity) + "," + "{0:.6f}".format(timestamp_sec) + "\n")
                                        else:
                                            nullPts += 1
                                            bytePos += 13

                                # Spherical Coordinate System
                                elif self.data_type == DataType.SPHERICAL:
                                    for i in range(0, 100):

                                        # Distance coordinate (check for non-zero)
                                        coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                        # timestamp
                                        timestamp_sec += 0.00001

                                        if coord1:
                                            bytePos += 4
                                            # Zenith coordinate
                                            coord2 = struct.unpack('<H', data_pc[bytePos:bytePos + 2])[0]
                                            bytePos += 2
                                            # Azimuth coordinate
                                            coord3 = struct.unpack('<H', data_pc[bytePos:bytePos + 2])[0]
                                            bytePos += 2
                                            # intensity
                                            intensity = int.from_bytes(data_pc[bytePos:bytePos + 1],
                                                                       byteorder='little')
                                            bytePos += 1

                                            numPts += 1
                                            csvFile.write(
                                                "{0:.3f}".format(float(coord1) / 1000.0) + "," + "{0:.2f}".format(
                                                    float(coord2) / 100.0) + "," + "{0:.2f}".format(
                                                    float(coord3) / 100.0) + "," + str(
                                                    intensity) + "," + "{0:.6f}".format(timestamp_sec) + "\n")
                                        else:
                                            nullPts += 1
                                            bytePos += 9

                            # double return firmware
                            elif self.firmware_type == FirmwareType.DOUBLE_RETURN:
                                # to account for first point's timestamp being increment in the loop
                                timestamp_sec -= 0.00001

                                # Cartesian Coordinate System
                                if self.data_type == DataType.CARTESIAN:
                                    for i in range(0, 100):
                                        returnNum = 1

                                        # Y coordinate (check for non-zero)
                                        coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                        zeroORtwo = i % 2

                                        # timestamp
                                        timestamp_sec += float(not (zeroORtwo)) * 0.00001

                                        # return number
                                        returnNum += zeroORtwo * 1

                                        if coord2:
                                            # X coordinate
                                            coord1 = struct.unpack('<i', data_pc[bytePos:bytePos + 4])[0]
                                            bytePos += 8
                                            # Z coordinate
                                            coord3 = struct.unpack('<i', data_pc[bytePos:bytePos + 4])[0]
                                            bytePos += 4
                                            # intensity
                                            intensity = int.from_bytes(data_pc[bytePos:bytePos + 1],
                                                                       byteorder='little')
                                            bytePos += 1

                                            numPts += 1
                                            csvFile.write(
                                                "{0:.3f}".format(float(coord1) / 1000.0) + "," + "{0:.3f}".format(
                                                    float(coord2) / 1000.0) + "," + "{0:.3f}".format(
                                                    float(coord3) / 1000.0) + "," + str(
                                                    intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," + str(
                                                    returnNum) + "\n")
                                        else:
                                            nullPts += 1
                                            bytePos += 13

                                            # Spherical Coordinate System
                                elif self.data_type == DataType.SPHERICAL:
                                    for i in range(0, 100):
                                        returnNum = 1

                                        # Distance coordinate (check for non-zero)
                                        coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                        zeroORtwo = i % 2

                                        # timestamp
                                        timestamp_sec += float(not (zeroORtwo)) * 0.00001

                                        # return number
                                        returnNum += zeroORtwo * 1

                                        if coord1:
                                            bytePos += 4
                                            # Zenith coordinate
                                            coord2 = struct.unpack('<H', data_pc[bytePos:bytePos + 2])[0]
                                            bytePos += 2
                                            # Azimuth coordinate
                                            coord3 = struct.unpack('<H', data_pc[bytePos:bytePos + 2])[0]
                                            bytePos += 2
                                            # intensity
                                            intensity = int.from_bytes(data_pc[bytePos:bytePos + 1],
                                                                       byteorder='little')
                                            bytePos += 1

                                            numPts += 1
                                            csvFile.write(
                                                "{0:.3f}".format(float(coord1) / 1000.0) + "," + "{0:.2f}".format(
                                                    float(coord2) / 100.0) + "," + "{0:.2f}".format(
                                                    float(coord3) / 100.0) + "," + str(
                                                    intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," + str(
                                                    returnNum) + "\n")
                                        else:
                                            nullPts += 1
                                            bytePos += 9


                            # triple return firmware
                            elif self.firmware_type == FirmwareType.TRIPLE_RETURN:
                                # to account for first point's timestamp being increment in the loop
                                timestamp_sec -= 0.000016666

                                # Cartesian Coordinate System
                                if self.data_type == DataType.CARTESIAN:
                                    for i in range(0, 100):
                                        returnNum = 1

                                        # Y coordinate (check for non-zero)
                                        coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                        zeroORoneORtwo = i % 3

                                        # timestamp
                                        timestamp_sec += float(not (zeroORoneORtwo)) * 0.000016666

                                        # return number
                                        returnNum += zeroORoneORtwo * 1

                                        if coord2:
                                            # X coordinate
                                            coord1 = struct.unpack('<i', data_pc[bytePos:bytePos + 4])[0]
                                            bytePos += 8
                                            # Z coordinate
                                            coord3 = struct.unpack('<i', data_pc[bytePos:bytePos + 4])[0]
                                            bytePos += 4
                                            # intensity
                                            intensity = int.from_bytes(data_pc[bytePos:bytePos + 1],
                                                                       byteorder='little')
                                            bytePos += 1

                                            numPts += 1
                                            csvFile.write(
                                                "{0:.3f}".format(float(coord1) / 1000.0) + "," + "{0:.3f}".format(
                                                    float(coord2) / 1000.0) + "," + "{0:.3f}".format(
                                                    float(coord3) / 1000.0) + "," + str(
                                                    intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," + str(
                                                    returnNum) + "\n")
                                        else:
                                            nullPts += 1
                                            bytePos += 13


                                # Spherical Coordinate System
                                elif self.data_type == DataType.SPHERICAL:
                                    for i in range(0, 100):
                                        returnNum = 1

                                        # Distance coordinate (check for non-zero)
                                        coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                        zeroORoneORtwo = i % 3

                                        # timestamp
                                        timestamp_sec += float(not (zeroORoneORtwo)) * 0.000016666

                                        # return number
                                        returnNum += zeroORoneORtwo * 1

                                        if coord1:
                                            bytePos += 4
                                            # Zenith coordinate
                                            coord2 = struct.unpack('<H', data_pc[bytePos:bytePos + 2])[0]
                                            bytePos += 2
                                            # Azimuth coordinate
                                            coord3 = struct.unpack('<H', data_pc[bytePos:bytePos + 2])[0]
                                            bytePos += 2
                                            # intensity
                                            intensity = int.from_bytes(data_pc[bytePos:bytePos + 1],
                                                                       byteorder='little')
                                            bytePos += 1

                                            numPts += 1
                                            csvFile.write(
                                                "{0:.3f}".format(float(coord1) / 1000.0) + "," + "{0:.2f}".format(
                                                    float(coord2) / 100.0) + "," + "{0:.2f}".format(
                                                    float(coord3) / 100.0) + "," + str(
                                                    intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," + str(
                                                    returnNum) + "\n")
                                        else:
                                            nullPts += 1
                                            bytePos += 9

                    # duration check (exit point)
                    else:
                        self.started = False
                        self.is_capturing = False
                        break
                # thread still running check (exit point)
                else:
                    break

            self.num_pts = numPts
            self.null_pts = nullPts

            self.msg.print(
                    "   " + self.sensor_ip + self._format_spaces + "   -->     closed ASCII file: " + self.file_path_and_name)
            self.msg.print("                                (points: " + str(numPts) + " good, " + str(
                    nullPts) + " null, " + str(numPts + nullPts) + " total)")

            csvFile.close()

        self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     Incorrect lidar packet version")

    def run_realtime_bin(self):

        # read point cloud data packet to get packet version and datatype
        # keep looping to 'consume' data that we don't want included in the captured point cloud data

        breakByCapture = False

        # used to check if the sensor is a Mid-100
        deviceCheck = 0
        try:
            deviceCheck = int(self._device_type[4:7])
        except:
            pass

        while True:

            if self.started:
                selectTest = select.select([self.d_socket], [], [], 0)
                if selectTest[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)
                    version = int.from_bytes(data_pc[0:1], byteorder='little')
                    self.data_type = int.from_bytes(data_pc[9:10], byteorder='little')
                    timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                    timestamp1 = self.getTimestamp(data_pc[10:18], timestamp_type)
                    self.updateStatus(data_pc[4:8])
                    if self.is_capturing:
                        self.start_time = timestamp1
                        breakByCapture = True
                        break
            else:
                break

        if breakByCapture:

            # check data packet is as expected (first byte anyways)
            if version == 5:

                # delayed start to capturing data check (secsToWait parameter)
                timestamp2 = self.start_time
                while True:
                    if self.started:
                        timeSinceStart = timestamp2 - self.start_time
                        if timeSinceStart <= self.secs_to_wait:
                            # read data from receive buffer and keep 'consuming' it
                            if select.select([self.d_socket], [], [], 0)[0]:
                                data_pc, addr = self.d_socket.recvfrom(1500)
                                timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                                timestamp2 = self.getTimestamp(data_pc[10:18], timestamp_type)
                                self.updateStatus(data_pc[4:8])
                            if select.select([self.i_socket], [], [], 0)[0]:
                                imu_data, addr2 = self.i_socket.recvfrom(50)
                        else:
                            self.start_time = timestamp2
                            break
                    else:
                        break

                self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     CAPTURING DATA...")

                timestamp_sec = self.start_time

                self.msg.print(
                    "   " + self.sensor_ip + self._format_spaces + "   -->     writing real-time data to BINARY file: " + self.file_path_and_name)
                binFile = open(self.file_path_and_name, "wb")
                IMU_file = None

                IMU_reporting = False
                numPts = 0
                nullPts = 0
                imu_records = 0

                # write header info to know how to parse the data later
                binFile.write(str.encode("OPENPYLIVOX"))
                binFile.write(struct.pack('<h', self.firmware_type))
                binFile.write(struct.pack('<h', self.data_type))

                # main loop that captures the desired point cloud data
                while True:
                    if self.started:
                        timeSinceStart = timestamp_sec - self.start_time

                        if timeSinceStart <= self.duration:

                            # read points from data buffer
                            if select.select([self.d_socket], [], [], 0)[0]:
                                data_pc, addr = self.d_socket.recvfrom(1500)

                                # version = int.from_bytes(data_pc[0:1], byteorder='little')
                                # slot_id = int.from_bytes(data_pc[1:2], byteorder='little')
                                # lidar_id = int.from_bytes(data_pc[2:3], byteorder='little')

                                # byte 3 is reserved

                                # update lidar status information
                                self.updateStatus(data_pc[4:8])
                                dataType = int.from_bytes(data_pc[9:10], byteorder='little')
                                timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                                timestamp_sec = self.getTimestamp(data_pc[10:18], timestamp_type)

                                bytePos = 18

                                # single return firmware (relevant for Mid-40 and Mid-100)
                                # Horizon and Tele-15 sensors also fall under this 'if' statement
                                if self.firmware_type == FirmwareType.SINGLE_RETURN:

                                    # Cartesian Coordinate System
                                    if dataType == 0:
                                        # to account for first point's timestamp being increment in the loop
                                        timestamp_sec -= 0.00001

                                        for i in range(0, 100):

                                            # Y coordinate (check for non-zero)
                                            coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                            # timestamp
                                            timestamp_sec += 0.00001

                                            if deviceCheck == 100:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 13])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                            else:
                                                if coord2:
                                                    numPts += 1
                                                    binFile.write(data_pc[bytePos:bytePos + 13])
                                                    binFile.write(struct.pack('<d', timestamp_sec))
                                                else:
                                                    nullPts += 1

                                            bytePos += 13

                                    # Spherical Coordinate System
                                    elif dataType == 1:
                                        # to account for first point's timestamp being increment in the loop
                                        timestamp_sec -= 0.00001

                                        for i in range(0, 100):

                                            # Distance coordinate (check for non-zero)
                                            coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                            # timestamp
                                            timestamp_sec += 0.00001

                                            if coord1:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 9])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                            else:
                                                nullPts += 1

                                            bytePos += 9

                                    # Horizon and Tele-15 Cartesian (single return)
                                    elif dataType == 2:
                                        # to account for first point's timestamp being increment in the loop
                                        timestamp_sec -= 0.000004167
                                        for i in range(0, 96):

                                            # Y coordinate (check for non-zero)
                                            coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                            # timestamp
                                            timestamp_sec += 0.000004167

                                            if coord2:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 14])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                            else:
                                                nullPts += 1

                                            bytePos += 14

                                    # Horizon and Tele-15 Spherical (single return)
                                    elif dataType == 3:
                                        # to account for first point's timestamp being increment in the loop
                                        timestamp_sec -= 0.000004167
                                        for i in range(0, 96):

                                            # Distance coordinate (check for non-zero)
                                            coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                            # timestamp
                                            timestamp_sec += 0.00001

                                            if coord1:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 10])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                            else:
                                                nullPts += 1

                                            bytePos += 10

                                    # Horizon and Tele-15 Cartesian (dual return)
                                    elif dataType == 4:
                                        # to account for first point's timestamp being increment in the loop
                                        timestamp_sec -= 0.000002083
                                        for i in range(0, 48):

                                            # Y coordinate (check for non-zero)
                                            coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                            # timestamp
                                            timestamp_sec += 0.000002083

                                            if coord2:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 28])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                            else:
                                                nullPts += 1

                                            bytePos += 28

                                    # Horizon and Tele-15 Spherical (dual return)
                                    elif dataType == 5:
                                        # to account for first point's timestamp being increment in the loop
                                        timestamp_sec -= 0.000002083
                                        for i in range(0, 48):

                                            # Distance coordinate (check for non-zero)
                                            coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                            # timestamp
                                            timestamp_sec += 0.00001

                                            if coord1:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 16])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                            else:
                                                nullPts += 1

                                            bytePos += 16

                                # double return firmware (Mid-40 and Mid-100 only)
                                elif self.firmware_type == FirmwareType.DOUBLE_RETURN:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.00001

                                    # Cartesian Coordinate System
                                    if self.data_type == DataType.CARTESIAN:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # Y coordinate (check for non-zero)
                                            coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                            zeroORtwo = i % 2

                                            # timestamp
                                            timestamp_sec += float(not (zeroORtwo)) * 0.00001

                                            # return number
                                            returnNum += zeroORtwo * 1

                                            if deviceCheck == 100:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 13])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                                binFile.write(str.encode(str(returnNum)))
                                            else:
                                                if coord2:
                                                    numPts += 1
                                                    binFile.write(data_pc[bytePos:bytePos + 13])
                                                    binFile.write(struct.pack('<d', timestamp_sec))
                                                    binFile.write(str.encode(str(returnNum)))
                                                else:
                                                    nullPts += 1

                                            bytePos += 13

                                    # Spherical Coordinate System
                                    elif self.data_type == DataType.SPHERICAL:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # Distance coordinate (check for non-zero)
                                            coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                            zeroORtwo = i % 2

                                            # timestamp
                                            timestamp_sec += float(not (zeroORtwo)) * 0.00001

                                            # return number
                                            returnNum += zeroORtwo * 1

                                            if coord1:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 9])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                                binFile.write(str.encode(str(returnNum)))
                                            else:
                                                nullPts += 1

                                            bytePos += 9

                                # triple return firmware (Mid-40 and Mid-100 only)
                                elif self.firmware_type == FirmwareType.TRIPLE_RETURN:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.000016667

                                    # Cartesian Coordinate System
                                    if self.data_type == DataType.CARTESIAN:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # Y coordinate (check for non-zero)
                                            coord2 = struct.unpack('<i', data_pc[bytePos + 4:bytePos + 8])[0]

                                            zeroORoneORtwo = i % 3

                                            # timestamp
                                            timestamp_sec += float(not (zeroORoneORtwo)) * 0.000016666

                                            # return number
                                            returnNum += zeroORoneORtwo * 1

                                            if deviceCheck == 100:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 13])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                                binFile.write(str.encode(str(returnNum)))
                                            else:
                                                if coord2:
                                                    numPts += 1
                                                    binFile.write(data_pc[bytePos:bytePos + 13])
                                                    binFile.write(struct.pack('<d', timestamp_sec))
                                                    binFile.write(str.encode(str(returnNum)))
                                                else:
                                                    nullPts += 1

                                            bytePos += 13

                                    # Spherical Coordinate System
                                    elif self.data_type == DataType.SPHERICAL:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # Distance coordinate (check for non-zero)
                                            coord1 = struct.unpack('<I', data_pc[bytePos:bytePos + 4])[0]

                                            zeroORoneORtwo = i % 3

                                            # timestamp
                                            timestamp_sec += float(not (zeroORoneORtwo)) * 0.000016666

                                            # return number
                                            returnNum += zeroORoneORtwo * 1

                                            if coord1:
                                                numPts += 1
                                                binFile.write(data_pc[bytePos:bytePos + 9])
                                                binFile.write(struct.pack('<d', timestamp_sec))
                                                binFile.write(str.encode(str(returnNum)))
                                            else:
                                                nullPts += 1

                                            bytePos += 9

                            # IMU data capture
                            if select.select([self.i_socket], [], [], 0)[0]:
                                imu_data, addr2 = self.i_socket.recvfrom(50)

                                # version = int.from_bytes(imu_data[0:1], byteorder='little')
                                # slot_id = int.from_bytes(imu_data[1:2], byteorder='little')
                                # lidar_id = int.from_bytes(imu_data[2:3], byteorder='little')

                                # byte 3 is reserved

                                # update lidar status information
                                # self.updateStatus(imu_data[4:8])

                                dataType = int.from_bytes(imu_data[9:10], byteorder='little')
                                timestamp_type = int.from_bytes(imu_data[8:9], byteorder='little')
                                timestamp_sec = self.getTimestamp(imu_data[10:18], timestamp_type)

                                bytePos = 18

                                # Horizon and Tele-15 IMU data packet
                                if dataType == 6:
                                    if not IMU_reporting:
                                        IMU_reporting = True
                                        path_file = Path(self.file_path_and_name)
                                        filename = path_file.stem
                                        exten = path_file.suffix
                                        IMU_file = open(filename + "_IMU" + exten, "wb")
                                        IMU_file.write(str.encode("OPENPYLIVOX_IMU"))

                                    IMU_file.write(imu_data[bytePos:bytePos + 24])
                                    IMU_file.write(struct.pack('<d', timestamp_sec))
                                    imu_records += 1

                        # duration check (exit point)
                        else:
                            self.started = False
                            self.is_capturing = False
                            break
                    # thread still running check (exit point)
                    else:
                        break

                self.num_pts = numPts
                self.null_pts = nullPts
                self.imu_records = imu_records

                self.msg.print(
                        "   " + self.sensor_ip + self._format_spaces + "   -->     closed BINARY file: " + self.file_path_and_name)
                self.msg.print("                                (points: " + str(numPts) + " good, " + str(
                    nullPts) + " null, " + str(numPts + nullPts) + " total)")
                if self._device_type == "Horizon" or self._device_type == "Tele-15":
                    self.msg.print("                                (IMU records: " + str(imu_records) + ")")

                binFile.close()

                if IMU_reporting:
                    IMU_file.close()

            else:
                self.msg.print(
                    "   " + self.sensor_ip + self._format_spaces + "   -->     Incorrect packet version")

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
                timestamp1 = self.getTimestamp(data_pc[10:18], timestamp_type)
                self.updateStatus(data_pc[4:8])
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

    def loop_until_wait_time_is_over(self, start_time):
        while self.started:
            time_since_start = start_time - self.start_time
            if time_since_start <= self.secs_to_wait:
                # read data from receive buffer and keep 'consuming' it
                if select.select([self.d_socket], [], [], 0)[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)
                    timestamp_type = helper.bytes_to_int(data_pc[8:9])
                    start_time = self.getTimestamp(data_pc[10:18], timestamp_type)
                    self.updateStatus(data_pc[4:8])
            else:
                self.start_time = start_time
                break

    def getTimestamp(self, data_pc, timestamp_type):

        # nanosecond timestamp
        if timestamp_type == 0 or timestamp_type == 1 or timestamp_type == 4:
            timestamp_sec = round(float(struct.unpack('<Q', data_pc[0:8])[0]) / 1000000000.0, 6)  # convert to seconds

        # UTC timestamp, microseconds past the hour
        elif timestamp_type == 3:
            timestamp_year = int.from_bytes(data_pc[0:1], byteorder='little')
            timestamp_month = int.from_bytes(data_pc[1:2], byteorder='little')
            timestamp_day = int.from_bytes(data_pc[2:3], byteorder='little')
            timestamp_hour = int.from_bytes(data_pc[3:4], byteorder='little')
            timestamp_sec = round(float(struct.unpack('<L', data_pc[4:8])[0]) / 1000000.0, 6)  # convert to seconds

            timestamp_sec += timestamp_hour * 3600.  # seconds into the day

            # TODO: check and adjust for hour, day, month and year crossovers

        return timestamp_sec

    # parse lidar status codes and update object properties, can provide real-time warning/error message display
    def updateStatus(self, data_pc):

        status_bits = str(bin(int.from_bytes(data_pc[0:1], byteorder='little')))[2:].zfill(8)
        status_bits += str(bin(int.from_bytes(data_pc[1:2], byteorder='little')))[2:].zfill(8)
        status_bits += str(bin(int.from_bytes(data_pc[2:3], byteorder='little')))[2:].zfill(8)
        status_bits += str(bin(int.from_bytes(data_pc[3:4], byteorder='little')))[2:].zfill(8)

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
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     * WARNING: temperature *")
                if self.volt_status == 1:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     * WARNING: voltage *")
                if self.motor_status == 1:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     * WARNING: motor *")
                if self.dirty_status == 1:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     * WARNING: dirty or blocked *")
                if self.device_status == 1:
                    self.msg.print(
                        "   " + self.sensor_ip + self._format_spaces + "   -->     * WARNING: approaching end of service life *")
                if self.fan_status == 1:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     * WARNING: fan *")
            elif self.system_status == 2:
                if self.temp_status == 2:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     *** ERROR: TEMPERATURE ***")
                if self.volt_status == 2:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     *** ERROR: VOLTAGE ***")
                if self.motor_status == 2:
                    self.msg.print("   " + self.sensor_ip + self._format_spaces + "   -->     *** ERROR: MOTOR ***")
                if self.firmware_status == 1:
                    self.msg.print(
                        "   " + self.sensor_ip + self._format_spaces + "   -->     *** ERROR: ABNORMAL FIRMWARE ***")

    # returns latest status Codes from within the point cloud data packet
    def statusCodes(self):

        return [self.system_status, self.temp_status, self.volt_status, self.motor_status, self.dirty_status,
                self.firmware_status, self.pps_status, self.device_status, self.fan_status, self.self_heating_status,
                self.ptp_status, self.time_sync_status]

    def stop(self):

        self.started = False
        self.thread.join()
