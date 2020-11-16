import select
import struct
import threading
from pathlib import Path


class _dataCaptureThread(object):

    def __init__(self, sensorIP, data_socket, imu_socket, filePathAndName, fileType, secsToWait, duration, firmwareType,
                 showMessages, format_spaces, deviceType):

        self.startTime = -1
        self.sensorIP = sensorIP
        self.d_socket = data_socket
        self.i_socket = imu_socket
        self.filePathAndName = filePathAndName
        # fileType 0 = Stored ASCII, 1 = Real-time ASCII, 2 = Real-time BINARY
        self.fileType = fileType
        self.secsToWait = secsToWait
        self.duration = duration
        self.firmwareType = firmwareType
        self.started = True
        self.isCapturing = False
        self.dataType = -1
        self.numPts = 0
        self.nullPts = 0
        self.imu_records = 0
        self._showMessages = showMessages
        self._format_spaces = format_spaces
        self._deviceType = deviceType
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
            years = 4
            amount_of_leap_days = int(years/4)
            seconds_per_day = 60 * 60 * 24
            # 4 Years of time in seconds
            self.duration = seconds_per_day * (365 * years + amount_of_leap_days)

        self.thread = None

        if self.fileType == 1:
            self.thread = threading.Thread(target=self.run_realtime_csv, args=())
        elif self.fileType == 2:
            self.thread = threading.Thread(target=self.run_realtime_bin, args=())
        else:
            self.thread = threading.Thread(target=self.run, args=())

        self.thread.daemon = True
        self.thread.start()

    def run(self):

        # read point cloud data packet to get packet version and datatype
        # keep looping to 'consume' data that we don't want included in the captured point cloud data

        breakByCapture = False
        while True:

            if self.started:
                selectTest = select.select([self.d_socket], [], [], 0)
                if selectTest[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)
                    version = int.from_bytes(data_pc[0:1], byteorder='little')
                    self.dataType = int.from_bytes(data_pc[9:10], byteorder='little')
                    timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                    timestamp1 = self.getTimestamp(data_pc[10:18], timestamp_type)
                    self.updateStatus(data_pc[4:8])
                    if self.isCapturing:
                        self.startTime = timestamp1
                        breakByCapture = True
                        break
            else:
                break

        if breakByCapture:

            # check data packet is as expected (first byte anyways)
            if version == 5:

                # lists to capture point cloud data stream info
                # TODO: should use an object, I know, I know!
                timestamps = []
                timestamp_types = []
                slot_ids = []
                lidar_ids = []
                coord1s = []
                coord2s = []
                coord3s = []
                intensities = []
                returnNums = []

                # delayed start to capturing data check (secsToWait parameter)
                timestamp2 = self.startTime
                while True:
                    if self.started:
                        timeSinceStart = timestamp2 - self.startTime
                        if timeSinceStart <= self.secsToWait:
                            # read data from receive buffer and keep 'consuming' it
                            if select.select([self.d_socket], [], [], 0)[0]:
                                data_pc, addr = self.d_socket.recvfrom(1500)
                                timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                                timestamp2 = self.getTimestamp(data_pc[10:18], timestamp_type)
                                self.updateStatus(data_pc[4:8])
                        else:
                            self.startTime = timestamp2
                            break
                    else:
                        break

                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + self._format_spaces + "   -->     CAPTURING DATA...")

                # duration adjustment (trying to get exactly 100,000 points / sec)
                if self.duration != 126230400:
                    if self.firmwareType == 1:
                        self.duration += (0.001 * (self.duration / 2.0))
                    elif self.firmwareType == 2:
                        self.duration += (0.0005 * (self.duration / 2.0))
                    elif self.firmwareType == 3:
                        self.duration += (0.00055 * (self.duration / 2.0))

                timestamp_sec = self.startTime
                # main loop that captures the desired point cloud data
                while True:
                    if self.started:
                        timeSinceStart = timestamp_sec - self.startTime

                        if timeSinceStart <= self.duration:

                            # read data from receive buffer
                            if select.select([self.d_socket], [], [], 0)[0]:
                                data_pc, addr = self.d_socket.recvfrom(1500)

                                version = int.from_bytes(data_pc[0:1], byteorder='little')
                                slot_id = int.from_bytes(data_pc[1:2], byteorder='little')
                                lidar_id = int.from_bytes(data_pc[2:3], byteorder='little')

                                # byte 3 is reserved

                                # update lidar status information
                                self.updateStatus(data_pc[4:8])

                                timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                                timestamp_sec = self.getTimestamp(data_pc[10:18], timestamp_type)

                                bytePos = 18

                                # single return firmware (most common)
                                if self.firmwareType == 1:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.00001

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
                                        for i in range(0, 100):
                                            # X coordinate
                                            coord1 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # Y coordinate
                                            coord2 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # Z coordinate
                                            coord3 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # intensity
                                            intensity = data_pc[bytePos:bytePos + 1]
                                            bytePos += 1

                                            # timestamp
                                            timestamp_sec += 0.00001

                                            timestamps.append(timestamp_sec)
                                            timestamp_types.append(timestamp_type)
                                            slot_ids.append(slot_id)
                                            lidar_ids.append(lidar_id)
                                            coord1s.append(coord1)
                                            coord2s.append(coord2)
                                            coord3s.append(coord3)
                                            intensities.append(intensity)

                                    # Spherical Coordinate System
                                    elif self.dataType == 1:
                                        for i in range(0, 100):
                                            # distance
                                            coord1 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # zenith
                                            coord2 = data_pc[bytePos:bytePos + 2]
                                            bytePos += 2

                                            # azimuth
                                            coord3 = data_pc[bytePos:bytePos + 2]
                                            bytePos += 2

                                            # intensity
                                            intensity = data_pc[bytePos:bytePos + 1]
                                            bytePos += 1

                                            # timestamp
                                            timestamp_sec += 0.00001

                                            timestamps.append(timestamp_sec)
                                            timestamp_types.append(timestamp_type)
                                            slot_ids.append(slot_id)
                                            lidar_ids.append(lidar_id)
                                            coord1s.append(coord1)
                                            coord2s.append(coord2)
                                            coord3s.append(coord3)
                                            intensities.append(intensity)

                                # double return firmware
                                elif self.firmwareType == 2:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.00001

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # X coordinate
                                            coord1 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # Y coordinate
                                            coord2 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # Z coordinate
                                            coord3 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # intensity
                                            intensity = data_pc[bytePos:bytePos + 1]
                                            bytePos += 1

                                            zeroORtwo = i % 2

                                            # timestamp
                                            timestamp_sec += float(not (zeroORtwo)) * 0.00001

                                            # return number
                                            returnNum += zeroORtwo * 1

                                            timestamps.append(timestamp_sec)
                                            timestamp_types.append(timestamp_type)
                                            slot_ids.append(slot_id)
                                            lidar_ids.append(lidar_id)
                                            coord1s.append(coord1)
                                            coord2s.append(coord2)
                                            coord3s.append(coord3)
                                            intensities.append(intensity)
                                            returnNums.append(returnNum)

                                    # Spherical Coordinate System
                                    elif self.dataType == 1:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # distance
                                            coord1 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # zenith
                                            coord2 = data_pc[bytePos:bytePos + 2]
                                            bytePos += 2

                                            # azimuth
                                            coord3 = data_pc[bytePos:bytePos + 2]
                                            bytePos += 2

                                            # intensity
                                            intensity = data_pc[bytePos:bytePos + 1]
                                            bytePos += 1

                                            zeroORtwo = i % 2

                                            # timestamp
                                            timestamp_sec += float(not (zeroORtwo)) * 0.00001

                                            # return number
                                            returnNum += zeroORtwo * 1

                                            timestamps.append(timestamp_sec)
                                            timestamp_types.append(timestamp_type)
                                            slot_ids.append(slot_id)
                                            lidar_ids.append(lidar_id)
                                            coord1s.append(coord1)
                                            coord2s.append(coord2)
                                            coord3s.append(coord3)
                                            intensities.append(intensity)
                                            returnNums.append(returnNum)

                                # triple return firmware
                                elif self.firmwareType == 3:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.000016666

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # X coordinate
                                            coord1 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # Y coordinate
                                            coord2 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # Z coordinate
                                            coord3 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # intensity
                                            intensity = data_pc[bytePos:bytePos + 1]
                                            bytePos += 1

                                            zeroORoneORtwo = i % 3

                                            # timestamp
                                            timestamp_sec += float(not (zeroORoneORtwo)) * 0.000016666

                                            # return number
                                            returnNum += zeroORoneORtwo * 1

                                            timestamps.append(timestamp_sec)
                                            timestamp_types.append(timestamp_type)
                                            slot_ids.append(slot_id)
                                            lidar_ids.append(lidar_id)
                                            coord1s.append(coord1)
                                            coord2s.append(coord2)
                                            coord3s.append(coord3)
                                            intensities.append(intensity)
                                            returnNums.append(returnNum)

                                    # Spherical Coordinate System
                                    elif self.dataType == 1:
                                        for i in range(0, 100):
                                            returnNum = 1

                                            # distance
                                            coord1 = data_pc[bytePos:bytePos + 4]
                                            bytePos += 4

                                            # zenith
                                            coord2 = data_pc[bytePos:bytePos + 2]
                                            bytePos += 2

                                            # azimuth
                                            coord3 = data_pc[bytePos:bytePos + 2]
                                            bytePos += 2

                                            # intensity
                                            intensity = data_pc[bytePos:bytePos + 1]
                                            bytePos += 1

                                            zeroORoneORtwo = i % 3

                                            # timestamp
                                            timestamp_sec += float(not (zeroORoneORtwo)) * 0.000016666

                                            # return number
                                            returnNum += zeroORoneORtwo * 1

                                            timestamps.append(timestamp_sec)
                                            timestamp_types.append(timestamp_type)
                                            slot_ids.append(slot_id)
                                            lidar_ids.append(lidar_id)
                                            coord1s.append(coord1)
                                            coord2s.append(coord2)
                                            coord3s.append(coord3)
                                            intensities.append(intensity)
                                            returnNums.append(returnNum)

                        # duration check (exit point)
                        else:
                            self.started = False
                            self.isCapturing = False
                            break
                    # thread still running check (exit point)
                    else:
                        break

                # make sure some data was captured
                lenData = len(coord1s)
                if lenData > 0:

                    if self._showMessages: print(
                        "   " + self.sensorIP + self._format_spaces + self._format_spaces + "   -->     writing data to ASCII file: " + self.filePathAndName)
                    csvFile = open(self.filePathAndName, "w")

                    numPts = 0
                    nullPts = 0

                    # TODO: apply coordinate transformations to the raw X, Y, Z point cloud data based on the extrinsic parameters
                    # rotation definitions and the sequence they are applied is always a bit of a head scratcher, lots of different definitions
                    # Geospatial/Traditional Photogrammetry/Computer Vision/North America/Europe all use different approaches

                    # single return fimware
                    if self.firmwareType == 1:

                        # Cartesian
                        if self.dataType == 0:
                            csvFile.write("//X,Y,Z,Inten-sity,Time\n")
                            for i in range(0, lenData):
                                coord1 = round(float(struct.unpack('<i', coord1s[i])[0]) / 1000.0, 3)
                                coord2 = round(float(struct.unpack('<i', coord2s[i])[0]) / 1000.0, 3)
                                coord3 = round(float(struct.unpack('<i', coord3s[i])[0]) / 1000.0, 3)
                                if coord1 or coord2 or coord3:
                                    numPts += 1
                                    csvFile.write("{0:.3f}".format(coord1) \
                                                  + "," + "{0:.3f}".format(coord2) \
                                                  + "," + "{0:.3f}".format(coord3) \
                                                  + "," + str(int.from_bytes(intensities[i], byteorder='little')) \
                                                  + "," + "{0:.6f}".format(timestamps[i]) + "\n")
                                else:
                                    nullPts += 1

                        # Spherical
                        elif self.dataType == 1:
                            csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time\n")
                            for i in range(0, lenData):
                                coord1 = round(float(struct.unpack('<I', coord1s[i])[0]) / 1000.0, 3)
                                coord2 = round(float(struct.unpack('<H', coord2s[i])[0]) / 100.0, 2)
                                coord3 = round(float(struct.unpack('<H', coord3s[i])[0]) / 100.0, 2)
                                if coord1:
                                    numPts += 1
                                    csvFile.write("{0:.3f}".format(coord1) \
                                                  + "," + "{0:.2f}".format(coord2) \
                                                  + "," + "{0:.2f}".format(coord3) \
                                                  + "," + str(int.from_bytes(intensities[i], byteorder='little')) \
                                                  + "," + "{0:.6f}".format(timestamps[i]) + "\n")
                                else:
                                    nullPts += 1

                    # multiple returns firmware
                    elif self.firmwareType == 2 or self.firmwareType == 3:

                        # Cartesian
                        if self.dataType == 0:
                            csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                            for i in range(0, lenData):
                                coord1 = round(float(struct.unpack('<i', coord1s[i])[0]) / 1000.0, 3)
                                coord2 = round(float(struct.unpack('<i', coord2s[i])[0]) / 1000.0, 3)
                                coord3 = round(float(struct.unpack('<i', coord3s[i])[0]) / 1000.0, 3)
                                if coord1 or coord2 or coord3:
                                    numPts += 1
                                    csvFile.write("{0:.3f}".format(coord1) \
                                                  + "," + "{0:.3f}".format(coord2) \
                                                  + "," + "{0:.3f}".format(coord3) \
                                                  + "," + str(int.from_bytes(intensities[i], byteorder='little')) \
                                                  + "," + "{0:.6f}".format(timestamps[i]) \
                                                  + "," + str(returnNums[i]) + "\n")
                                else:
                                    nullPts += 1

                        # Spherical
                        elif self.dataType == 1:
                            csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")
                            for i in range(0, lenData):
                                coord1 = round(float(struct.unpack('<I', coord1s[i])[0]) / 1000.0, 3)
                                coord2 = round(float(struct.unpack('<H', coord2s[i])[0]) / 100.0, 2)
                                coord3 = round(float(struct.unpack('<H', coord3s[i])[0]) / 100.0, 2)
                                if coord1:
                                    numPts += 1
                                    csvFile.write("{0:.3f}".format(coord1) \
                                                  + "," + "{0:.2f}".format(coord2) \
                                                  + "," + "{0:.2f}".format(coord3) \
                                                  + "," + str(int.from_bytes(intensities[i], byteorder='little')) \
                                                  + "," + "{0:.6f}".format(timestamps[i]) \
                                                  + "," + str(returnNums[i]) + "\n")
                                else:
                                    nullPts += 1

                    self.numPts = numPts
                    self.nullPts = nullPts

                    if self._showMessages:
                        print(
                            "   " + self.sensorIP + self._format_spaces + self._format_spaces + "   -->     closed ASCII file: " + self.filePathAndName)
                        print(
                            "                    (points: " + str(numPts) + " good, " + str(nullPts) + " null, " + str(
                                numPts + nullPts) + " total)")
                    csvFile.close()

                else:
                    if self._showMessages: print(
                        "   " + self.sensorIP + self._format_spaces + "   -->     WARNING: no point cloud data was captured")

            else:
                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     Incorrect lidar packet version")

    def run_realtime_csv(self):

        # read point cloud data packet to get packet version and datatype
        # keep looping to 'consume' data that we don't want included in the captured point cloud data

        breakByCapture = False
        while True:

            if self.started:
                selectTest = select.select([self.d_socket], [], [], 0)
                if selectTest[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)
                    version = int.from_bytes(data_pc[0:1], byteorder='little')
                    self.dataType = int.from_bytes(data_pc[9:10], byteorder='little')
                    timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                    timestamp1 = self.getTimestamp(data_pc[10:18], timestamp_type)
                    self.updateStatus(data_pc[4:8])
                    if self.isCapturing:
                        self.startTime = timestamp1
                        breakByCapture = True
                        break
            else:
                break

        if breakByCapture:

            # check data packet is as expected (first byte anyways)
            if version == 5:

                # delayed start to capturing data check (secsToWait parameter)
                timestamp2 = self.startTime
                while True:
                    if self.started:
                        timeSinceStart = timestamp2 - self.startTime
                        if timeSinceStart <= self.secsToWait:
                            # read data from receive buffer and keep 'consuming' it
                            if select.select([self.d_socket], [], [], 0)[0]:
                                data_pc, addr = self.d_socket.recvfrom(1500)
                                timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                                timestamp2 = self.getTimestamp(data_pc[10:18], timestamp_type)
                                self.updateStatus(data_pc[4:8])
                        else:
                            self.startTime = timestamp2
                            break
                    else:
                        break

                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     CAPTURING DATA...")

                # duration adjustment (trying to get exactly 100,000 points / sec)
                if self.duration != 126230400:
                    if self.firmwareType == 1:
                        self.duration += (0.001 * (self.duration / 2.0))
                    elif self.firmwareType == 2:
                        self.duration += (0.0005 * (self.duration / 2.0))
                    elif self.firmwareType == 3:
                        self.duration += (0.00055 * (self.duration / 2.0))

                timestamp_sec = self.startTime

                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     writing real-time data to ASCII file: " + self.filePathAndName)
                csvFile = open(self.filePathAndName, "w", 1)

                numPts = 0
                nullPts = 0

                # write header info
                if self.firmwareType == 1:  # single return firmware
                    if self.dataType == 0:  # Cartesian
                        csvFile.write("//X,Y,Z,Inten-sity,Time\n")
                    elif self.dataType == 1:  # Spherical
                        csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time\n")
                elif self.firmwareType == 2 or self.firmwareType == 3:  # double or triple return firmware
                    if self.dataType == 0:  # Cartesian
                        csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                    elif self.dataType == 1:  # Spherical
                        csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")

                # main loop that captures the desired point cloud data
                while True:
                    if self.started:
                        timeSinceStart = timestamp_sec - self.startTime

                        if timeSinceStart <= self.duration:

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
                                if self.firmwareType == 1:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.00001

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
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
                                    elif self.dataType == 1:
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
                                elif self.firmwareType == 2:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.00001

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
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
                                    elif self.dataType == 1:
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
                                elif self.firmwareType == 3:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.000016666

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
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
                                    elif self.dataType == 1:
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
                            self.isCapturing = False
                            break
                    # thread still running check (exit point)
                    else:
                        break

                self.numPts = numPts
                self.nullPts = nullPts

                if self._showMessages:
                    print(
                        "   " + self.sensorIP + self._format_spaces + "   -->     closed ASCII file: " + self.filePathAndName)
                    print("                                (points: " + str(numPts) + " good, " + str(
                        nullPts) + " null, " + str(numPts + nullPts) + " total)")

                csvFile.close()

            else:
                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     Incorrect lidar packet version")

    def run_realtime_bin(self):

        # read point cloud data packet to get packet version and datatype
        # keep looping to 'consume' data that we don't want included in the captured point cloud data

        breakByCapture = False

        # used to check if the sensor is a Mid-100
        deviceCheck = 0
        try:
            deviceCheck = int(self._deviceType[4:7])
        except:
            pass

        while True:

            if self.started:
                selectTest = select.select([self.d_socket], [], [], 0)
                if selectTest[0]:
                    data_pc, addr = self.d_socket.recvfrom(1500)
                    version = int.from_bytes(data_pc[0:1], byteorder='little')
                    self.dataType = int.from_bytes(data_pc[9:10], byteorder='little')
                    timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                    timestamp1 = self.getTimestamp(data_pc[10:18], timestamp_type)
                    self.updateStatus(data_pc[4:8])
                    if self.isCapturing:
                        self.startTime = timestamp1
                        breakByCapture = True
                        break
            else:
                break

        if breakByCapture:

            # check data packet is as expected (first byte anyways)
            if version == 5:

                # delayed start to capturing data check (secsToWait parameter)
                timestamp2 = self.startTime
                while True:
                    if self.started:
                        timeSinceStart = timestamp2 - self.startTime
                        if timeSinceStart <= self.secsToWait:
                            # read data from receive buffer and keep 'consuming' it
                            if select.select([self.d_socket], [], [], 0)[0]:
                                data_pc, addr = self.d_socket.recvfrom(1500)
                                timestamp_type = int.from_bytes(data_pc[8:9], byteorder='little')
                                timestamp2 = self.getTimestamp(data_pc[10:18], timestamp_type)
                                self.updateStatus(data_pc[4:8])
                            if select.select([self.i_socket], [], [], 0)[0]:
                                imu_data, addr2 = self.i_socket.recvfrom(50)
                        else:
                            self.startTime = timestamp2
                            break
                    else:
                        break

                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     CAPTURING DATA...")

                timestamp_sec = self.startTime

                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     writing real-time data to BINARY file: " + self.filePathAndName)
                binFile = open(self.filePathAndName, "wb")
                IMU_file = None

                IMU_reporting = False
                numPts = 0
                nullPts = 0
                imu_records = 0

                # write header info to know how to parse the data later
                binFile.write(str.encode("OPENPYLIVOX"))
                binFile.write(struct.pack('<h', self.firmwareType))
                binFile.write(struct.pack('<h', self.dataType))

                # main loop that captures the desired point cloud data
                while True:
                    if self.started:
                        timeSinceStart = timestamp_sec - self.startTime

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
                                if self.firmwareType == 1:

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
                                elif self.firmwareType == 2:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.00001

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
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
                                    elif self.dataType == 1:
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
                                elif self.firmwareType == 3:
                                    # to account for first point's timestamp being increment in the loop
                                    timestamp_sec -= 0.000016667

                                    # Cartesian Coordinate System
                                    if self.dataType == 0:
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
                                    elif self.dataType == 1:
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
                                        path_file = Path(self.filePathAndName)
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
                            self.isCapturing = False
                            break
                    # thread still running check (exit point)
                    else:
                        break

                self.numPts = numPts
                self.nullPts = nullPts
                self.imu_records = imu_records

                if self._showMessages:
                    print(
                        "   " + self.sensorIP + self._format_spaces + "   -->     closed BINARY file: " + self.filePathAndName)
                    print("                                (points: " + str(numPts) + " good, " + str(
                        nullPts) + " null, " + str(numPts + nullPts) + " total)")
                    if self._deviceType == "Horizon" or self._deviceType == "Tele-15":
                        print("                                (IMU records: " + str(imu_records) + ")")

                binFile.close()

                if IMU_reporting:
                    IMU_file.close()

            else:
                if self._showMessages: print(
                    "   " + self.sensorIP + self._format_spaces + "   -->     Incorrect packet version")

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
            if self._showMessages:
                if self.system_status == 1:
                    if self.temp_status == 1:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     * WARNING: temperature *")
                    if self.volt_status == 1:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     * WARNING: voltage *")
                    if self.motor_status == 1:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     * WARNING: motor *")
                    if self.dirty_status == 1:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     * WARNING: dirty or blocked *")
                    if self.device_status == 1:
                        print(
                            "   " + self.sensorIP + self._format_spaces + "   -->     * WARNING: approaching end of service life *")
                    if self.fan_status == 1:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     * WARNING: fan *")
                elif self.system_status == 2:
                    if self.temp_status == 2:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     *** ERROR: TEMPERATURE ***")
                    if self.volt_status == 2:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     *** ERROR: VOLTAGE ***")
                    if self.motor_status == 2:
                        print("   " + self.sensorIP + self._format_spaces + "   -->     *** ERROR: MOTOR ***")
                    if self.firmware_status == 1:
                        print(
                            "   " + self.sensorIP + self._format_spaces + "   -->     *** ERROR: ABNORMAL FIRMWARE ***")

    # returns latest status Codes from within the point cloud data packet
    def statusCodes(self):

        return [self.system_status, self.temp_status, self.volt_status, self.motor_status, self.dirty_status,
                self.firmware_status, self.pps_status, self.device_status, self.fan_status, self.self_heating_status,
                self.ptp_status, self.time_sync_status]

    def stop(self):

        self.started = False
        self.thread.join()
