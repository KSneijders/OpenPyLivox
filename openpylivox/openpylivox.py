#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Started on Mon. May 13th 2019

@author: Ryan Brazeal
@email: ryan.brazeal@ufl.edu

Program Name: openpylivox.py
Version: 1.1.0

Description: Python3 driver for UDP Communications with Lixov Lidar sensors

Livox SDK link: https://github.com/Livox-SDK/Livox-SDK/wiki/Livox-SDK-Communication-Protocol

Change Log:
    - v1.0.0 released - Sept. 13th 2019
    - v1.0.1 released - May 27th 2020
    - v1.0.2 and v1.0.3 released - May 29th 2020
    - v1.1.0 released - Sept. 11th 2020 (NEVER FORGET!)
    
"""

# standard modules
import binascii
import os
import select
import socket
import struct
import sys
import time
from pathlib import Path

# additional modules
import laspy
import numpy as np
from deprecated import deprecated
from tqdm import tqdm

from openpylivox import helper
from openpylivox.data_capture_thread import DataCaptureThread
from openpylivox.heartbeat_thread import HeartbeatThread
from openpylivox.helper import _parse_resp
import openpylivox.LivoxSDKDefines as sdkdefs


class OpenPyLivox:

    def __init__(self, show_messages=False):
        self._is_connected = False
        self._is_data = False
        self._is_writing = False
        self._data_socket = None
        self._cmd_socket = None
        self._imu_socket = None
        self._heartbeat = None
        self._firmware = "UNKNOWN"
        self._coord_system = -1
        self._x = None
        self._y = None
        self._z = None
        self._roll = None
        self._pitch = None
        self._yaw = None
        self._capture_stream = None
        self._serial = "UNKNOWN"
        self._ip_range_code = 0
        self._computer_ip = ""
        self._sensor_ip = ""
        self._data_port = -1
        self._cmd_port = -1
        self._imu_port = -1
        self._init_show_messages = show_messages
        self.msg = helper.Msg(show_message=show_messages)
        self._device_type = "UNKNOWN"
        self._mid100_sensors = []
        self._format_spaces = ""

    def _re_init(self):

        self._data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._cmd_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._imu_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._data_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._cmd_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._imu_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        lidar_sensor_ips, serial_nums, ip_range_codes, sensor_types = self._search_for_sensors(False)

        unique_serial_nums = []
        unique_sensors = []
        ip_groups = []
        id_groups = []

        for i in range(len(lidar_sensor_ips)):
            if i == 0:
                unique_serial_nums.append(serial_nums[i])
            else:
                matched = False
                for j in range(len(unique_serial_nums)):
                    if serial_nums[i] == unique_serial_nums[j]:
                        matched = True
                        break
                if not matched:
                    unique_serial_nums.append(serial_nums[i])

        for i in range(len(unique_serial_nums)):
            count = 0
            ips = ""
            ids = ""
            for j in range(len(serial_nums)):
                if serial_nums[j] == unique_serial_nums[i]:
                    count += 1
                    ips += lidar_sensor_ips[j] + ","
                    ids += str(ip_range_codes[j]) + ","
            if count == 1:
                unique_sensors.append(sensor_types[i])
            elif count == 2:
                unique_sensors.append("NA")
            elif count == 3:
                unique_sensors.append("Mid-100")

            ip_groups.append(ips[:-1])
            id_groups.append(ids[:-1])

        sensor_ips = []
        for i in range(len(ip_groups)):
            current_device = unique_sensors[i]
            ind_ips = ip_groups[i].split(',')
            for j in range(len(ind_ips)):
                ip_and_type = [ind_ips[j], current_device]
                sensor_ips.append(ip_and_type)

        found_match_ip = False
        for i in range(0, len(lidar_sensor_ips)):
            if lidar_sensor_ips[i] == self._sensor_ip:
                found_match_ip = True
                self._serial = serial_nums[i]
                self._ip_range_code = ip_range_codes[i]
                break

        if not found_match_ip:
            print("\n* ERROR: specified sensor IP:Command Port cannot connect to a Livox sensor *")
            print("* common causes are a wrong IP or the command port is being used already   *\n")
            time.sleep(0.1)
            sys.exit(2)

        return unique_serial_nums, unique_sensors, sensor_ips

    @staticmethod
    def _check_ip(input_ip):

        clean_ip = ""
        if input_ip:
            ip_parts = input_ip.split(".")
            if len(ip_parts) == 4:
                for i in range(0, 4):
                    integer_ip = int(ip_parts[i])
                    if 0 <= integer_ip <= 254:
                        clean_ip += str(integer_ip)
                        if i < 3:
                            clean_ip += "."
                    else:
                        clean_ip = ""
                        break

        return clean_ip

    @staticmethod
    def _check_port(input_port: int):

        port_num = int(input_port)
        if 0 <= port_num <= 65535:
            pass
        else:
            port_num = -1

        return port_num

    def _search_for_sensors(self, opt=False):

        server_sock_init = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_sock_init.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock_init.bind(("0.0.0.0", 55000))

        found_device = select.select([server_sock_init], [], [], 1)[0]

        ips = []
        serials = []
        ip_range_codes = []
        sensor_types = []

        if found_device:

            readData = True
            while readData:

                binData, addr = server_sock_init.recvfrom(34)

                if len(addr) == 2:
                    if addr[1] == 65000:
                        if len(ips) == 0:
                            goodData, cmdMessage, dataMessage, device_serial, typeMessage, ipRangeCode = self._info(
                                binData)
                            sensor_types.append(typeMessage)
                            ips.append(self._check_ip(addr[0]))
                            serials.append(device_serial)
                            ip_range_codes.append(ipRangeCode)

                        else:
                            existsAlready = False
                            for i in range(0, len(ips)):
                                if addr[0] == ips[i]:
                                    existsAlready = True
                            if existsAlready:
                                readData = False
                                break
                            else:
                                goodData, cmdMessage, dataMessage, device_serial, typeMessage, ipRangeCode = self._info(
                                    binData)
                                sensor_types.append(typeMessage)
                                ips.append(self._check_ip(addr[0]))
                                serials.append(device_serial)
                                ip_range_codes.append(ipRangeCode)

                else:
                    readData = False

        server_sock_init.close()
        time.sleep(0.2)

        if self._show_messages and opt:
            for i in range(0, len(ips)):
                print("   Found Livox Sensor w. serial #" + serials[i] + " at IP: " + ips[i])
            print()

        return ips, serials, ip_range_codes, sensor_types

    def _auto_computer_ip(self):

        try:
            hostname = socket.gethostname()
            self._computer_ip = socket.gethostbyname(hostname)

        except:
            self.computerIP = ""

    def _bind_ports(self):

        try:
            self._data_socket.bind((self._computer_ip, self._data_port))
            self._cmd_socket.bind((self._computer_ip, self._cmd_port))
            self._imu_socket.bind((self._computer_ip, self._imu_port))
            assigned_data_port = self._data_socket.getsockname()[1]
            assigned_cmd_port = self._cmd_socket.getsockname()[1]
            assigned_imu_port = self._imu_socket.getsockname()[1]

            time.sleep(0.1)

            return assigned_data_port, assigned_cmd_port, assigned_imu_port

        except socket.error as err:
            print(" *** ERROR: cannot bind to specified IP:Port(s), " + err)
            sys.exit(3)

    @staticmethod
    def turn_values_into_single_hex_string(data_array, type_array):
        hex_string = ""
        for index in range(len(data_array)):
            hex_string += str(binascii.hexlify(struct.pack(type_array[index], data_array[index])))[2:-1]
        return hex_string

    def send_command_receive_ack(self, command, expected_command_set, expected_command_id):
        if self._is_connected:
            self._wait_for_idle()
            self._cmd_socket.sendto(command, (self._sensor_ip, 65000))

            # check for proper response from lidar start request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                binData, addr = self._cmd_socket.recvfrom(16)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)
                # self.msg.print(ack + " / " + cmd_set + " / " + cmd_id)  # TODO: Put into logging
                if ack == "ACK (response)" and cmd_set == expected_command_set and cmd_id == str(expected_command_id):
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    return ret_code
                else:
                    return -1
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)
            return -2

    def send_command_receive_data(self, command, expected_command_set, expected_command_id, length_bytes):
        if self._is_connected:
            self._wait_for_idle()
            self._cmd_socket.sendto(command, (self._sensor_ip, 65000))

            # check for proper response from read extrinsics request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                bin_data, addr = self._cmd_socket.recvfrom(length_bytes)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)

                if ack == "ACK (response)" and cmd_set == expected_command_set and cmd_id == str(expected_command_id):
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    return ret_code, bin_data
                else:
                    return -1, None
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)
            return -2, None

    def _wait_for_idle(self):

        while self._heartbeat.idle_state != 9:
            time.sleep(0.1)

    def _disconnect_sensor(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_DISCONNECT, "General", 6)
        # self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar disconnect request")  # TODO: Proper logging
        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to disconnect")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect disconnect response")

    def _reboot_sensor(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_REBOOT, "General", 10)
        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to reboot")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect reboot response")

    def _query(self):
        response, data = self.send_command_receive_data(sdkdefs.CMD_QUERY, "General", 2, 20)
        if response == 0:
            _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, data)
            AA = str(int.from_bytes(ret_code_bin[1], byteorder='little')).zfill(2)
            BB = str(int.from_bytes(ret_code_bin[2], byteorder='little')).zfill(2)
            CC = str(int.from_bytes(ret_code_bin[3], byteorder='little')).zfill(2)
            DD = str(int.from_bytes(ret_code_bin[4], byteorder='little')).zfill(2)
            self._firmware = AA + "." + BB + "." + CC + DD
            # self.msg.print(f"Firmware version is {self._firmware}")
        elif response == 1:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to receive query results")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect query response")

    def _info(self, binData):

        goodData, cmdMessage, dataMessage, dataID, dataBytes = _parse_resp(self._show_messages, binData)
        ipRangeCode = None
        device_serial, typeMessage = "", ""

        if goodData:
            # received broadcast message
            if cmdMessage == "MSG (message)" and dataMessage == "General" and dataID == "0":
                device_broadcast_code = ""
                i = 0
                for i in range(0, 16):
                    device_broadcast_code += dataBytes[i].decode('ascii')

                ipRangeCode = int(device_broadcast_code[14:15])  # used to define L,M,R sensors in the Mid-100
                device_serial = device_broadcast_code[:-2]
                device_type = int.from_bytes(dataBytes[i + 1], byteorder='little')

                typeMessage = ""
                if device_type == 0:
                    typeMessage = "Hub    "
                elif device_type == 1:
                    typeMessage = "Mid-40 "
                elif device_type == 2:
                    typeMessage = "Tele-15"
                elif device_type == 3:
                    typeMessage = "Horizon"
                else:
                    typeMessage = "UNKNOWN"

        return goodData, cmdMessage, dataMessage, device_serial, typeMessage, ipRangeCode

    def discover(self, manualComputerIP=""):

        if not manualComputerIP:
            self._auto_computer_ip()
        else:
            self._computer_ip = manualComputerIP

        if self._computer_ip:
            print("\nUsing computer IP address: " + self._computer_ip + "\n")

            lidarSensorIPs, serialNums, ipRangeCodes, sensorTypes = self._search_for_sensors(False)

            unique_serialNums = []
            unique_sensors = []
            IP_groups = []
            ID_groups = []

            for i in range(len(lidarSensorIPs)):
                if i == 0:
                    unique_serialNums.append(serialNums[i])
                else:
                    matched = False
                    for j in range(len(unique_serialNums)):
                        if serialNums[i] == unique_serialNums[j]:
                            matched = True
                            break
                    if not matched:
                        unique_serialNums.append(serialNums[i])

            for i in range(len(unique_serialNums)):
                count = 0
                IPs = ""
                IDs = ""
                for j in range(len(serialNums)):
                    if serialNums[j] == unique_serialNums[i]:
                        count += 1
                        IPs += lidarSensorIPs[j] + ","
                        IDs += str(ipRangeCodes[j]) + ","
                if count == 1:
                    unique_sensors.append(sensorTypes[i])
                elif count == 3:
                    unique_sensors.append("Mid-100")
                IP_groups.append(IPs[:-1])
                ID_groups.append(IDs[:-1])

            if len(unique_serialNums) > 0:
                for i in range(len(unique_serialNums)):
                    IPs_list = IP_groups[i].split(',')
                    IDs_list = ID_groups[i].split(',')
                    IPs_mess = ""
                    last_IP_num = []
                    for j in range(len(IPs_list)):
                        last_IP_num.append(int(IPs_list[j].split('.')[3]))
                    last_IP_num.sort()
                    for j in range(len(last_IP_num)):
                        for k in range(len(IPs_list)):
                            if last_IP_num[j] == int(IPs_list[k].split('.')[3]):
                                numspaces = " "
                                if last_IP_num[j] < 100:
                                    numspaces += " "
                                if last_IP_num[j] < 10:
                                    numspaces += " "
                                IPs_mess += str(IPs_list[k]) + numspaces + "(ID: " + str(
                                    IDs_list[k]) + ")\n                 "
                                break
                    print("   *** Discovered a Livox sensor ***")
                    print("           Type: " + unique_sensors[i])
                    print("         Serial: " + unique_serialNums[i])
                    print("          IP(s): " + IPs_mess)

            else:
                print("Did not discover any Livox sensors, check communication and power cables and network settings")

        else:
            print("*** ERROR: Failed to auto determine computer IP address ***")

    def connect(self, computerIP, sensorIP, dataPort, cmdPort, imuPort, sensor_name_override=""):

        numFound = 0

        if not self._is_connected:

            self._computer_ip = self._check_ip(computerIP)
            self._sensor_ip = self._check_ip(sensorIP)
            self._data_port = self._check_port(dataPort)
            self._cmd_port = self._check_port(cmdPort)
            self._imu_port = self._check_port(imuPort)

            if self._computer_ip and self._sensor_ip and self._data_port != -1 and self._cmd_port != -1 and self._imu_port != -1:
                num_spaces = 15 - len(self._sensor_ip)
                for i in range(num_spaces):
                    self._format_spaces += " "
                unique_serialNums, unique_sensors, sensor_IPs = self._re_init()
                numFound = len(unique_sensors)
                time.sleep(0.1)

                for i in range(len(sensor_IPs)):
                    if self._sensor_ip == sensor_IPs[i][0]:
                        self._device_type = sensor_IPs[i][1]

                if sensor_name_override:
                    self._device_type = sensor_name_override

                self._data_port, self._cmd_port, self._imu_port = self._bind_ports()

                IP_parts = self._computer_ip.split(".")
                IPhex = str(hex(int(IP_parts[0]))).replace('0x', '').zfill(2)
                IPhex += str(hex(int(IP_parts[1]))).replace('0x', '').zfill(2)
                IPhex += str(hex(int(IP_parts[2]))).replace('0x', '').zfill(2)
                IPhex += str(hex(int(IP_parts[3]))).replace('0x', '').zfill(2)
                dataHexAll = str(hex(int(self._data_port))).replace('0x', '').zfill(4)
                dataHex = dataHexAll[2:] + dataHexAll[:-2]
                cmdHexAll = str(hex(int(self._cmd_port))).replace('0x', '').zfill(4)
                cmdHex = cmdHexAll[2:] + cmdHexAll[:-2]
                imuHexAll = str(hex(int(self._imu_port))).replace('0x', '').zfill(4)
                imuHex = imuHexAll[2:] + imuHexAll[:-2]
                cmdString = "AA011900000000DC580001" + IPhex + dataHex + cmdHex + imuHex
                binString = bytes(cmdString, encoding='utf-8')
                crc32checksum = helper.crc32from_str(binString)
                cmdString += crc32checksum
                binString = bytes(cmdString, encoding='utf-8')

                connect_request = bytes.fromhex((binString).decode('ascii'))
                self._cmd_socket.sendto(connect_request, (self._sensor_ip, 65000))

                # check for proper response from connection request
                if select.select([self._cmd_socket], [], [], 0.1)[0]:
                    binData, addr = self._cmd_socket.recvfrom(16)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "1":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                        if ret_code == 0:
                            self._is_connected = True
                            self._heartbeat = HeartbeatThread(1, self._cmd_socket, self._sensor_ip, 65000,
                                                              sdkdefs.CMD_HEARTBEAT, self._show_messages,
                                                              self._format_spaces)
                            time.sleep(0.15)
                            self._query()
                            self.msg.print("Connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip + " (ID: " + str(
                                    self._ip_range_code) + ")")
                        else:
                            self.msg.print("FAILED to connect to the Livox " + self._device_type + " at IP: " + self._sensor_ip)
                    else:
                        self.msg.print("FAILED to connect to the Livox " + self._device_type + " at IP: " + self._sensor_ip)
            else:
                self.msg.print("Invalid connection parameter(s)")
        else:
            self.msg.print("Already connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip)

        return numFound

    def auto_connect(self, manualComputerIP=""):

        numFound = 0

        if not manualComputerIP:
            self._auto_computer_ip()
        else:
            self._computer_ip = manualComputerIP

        if self._computer_ip:

            lidarSensorIPs, serialNums, ipRangeCodes, sensorTypes = self._search_for_sensors(False)

            unique_serialNums = []
            unique_sensors = []
            IP_groups = []
            ID_groups = []

            for i in range(len(lidarSensorIPs)):
                if i == 0:
                    unique_serialNums.append(serialNums[i])
                else:
                    matched = False
                    for j in range(len(unique_serialNums)):
                        if serialNums[i] == unique_serialNums[j]:
                            matched = True
                            break
                    if not matched:
                        unique_serialNums.append(serialNums[i])

            for i in range(len(unique_serialNums)):
                count = 0
                IPs = ""
                IDs = ""
                for j in range(len(serialNums)):
                    if serialNums[j] == unique_serialNums[i]:
                        count += 1
                        IPs += lidarSensorIPs[j] + ","
                        IDs += str(ipRangeCodes[j]) + ","
                if count == 1:
                    unique_sensors.append(sensorTypes[i])
                elif count == 2:
                    unique_sensors.append("NA")
                elif count == 3:
                    unique_sensors.append("Mid-100")
                IP_groups.append(IPs[:-1])
                ID_groups.append(IDs[:-1])

            status_message = ""

            if len(unique_serialNums) > 0:
                if self._show_messages:
                    status_message = "\nUsing computer IP address: " + self._computer_ip + "\n\n"
                for i in range(0, len(unique_serialNums)):
                    IPs_list = IP_groups[i].split(',')
                    IDs_list = ID_groups[i].split(',')
                    IPs_mess = ""
                    last_IP_num = []
                    for j in range(len(IPs_list)):
                        last_IP_num.append(int(IPs_list[j].split('.')[3]))
                    last_IP_num.sort()
                    for j in range(len(last_IP_num)):
                        for k in range(len(IPs_list)):
                            if last_IP_num[j] == int(IPs_list[k].split('.')[3]):
                                numspaces = " "
                                if last_IP_num[j] < 100:
                                    numspaces += " "
                                if last_IP_num[j] < 10:
                                    numspaces += " "
                                IPs_mess += str(IPs_list[k]) + numspaces + "(ID: " + str(
                                    IDs_list[k]) + ")\n                 "
                                break
                    if unique_sensors[i] != "NA":
                        status_message += "   *** Discovered a Livox sensor ***\n"
                        status_message += "           Type: " + unique_sensors[i] + "\n"
                        status_message += "         Serial: " + unique_serialNums[i] + "\n"
                        status_message += "          IP(s): " + IPs_mess + "\n"

            if len(unique_sensors) > 0 and unique_sensors[0] != "NA":
                status_message = status_message[:-1]
                print(status_message)
                self.msg.print("Attempting to auto-connect to the Livox " + unique_sensors[0] + " with S/N: " +
                          unique_serialNums[0])

                if unique_sensors[0] == "Mid-100":
                    sensor_IPs = None
                    sensor_IDs = None
                    for i in range(len(IP_groups)):
                        ind_IPs = IP_groups[i].split(',')
                        ind_IDs = ID_groups[i].split(',')
                        for j in range(len(ind_IPs)):
                            if lidarSensorIPs[0] == ind_IPs[j]:
                                sensor_IPs = ind_IPs
                                sensor_IDs = ind_IDs

                    sensorM = OpenPyLivox(self._init_show_messages)
                    sensorR = OpenPyLivox(self._init_show_messages)

                    for i in range(3):
                        if int(sensor_IDs[i]) == 1:
                            self.connect(self._computer_ip, sensor_IPs[i], 0, 0, 0, "Mid-100 (L)")
                            for j in range(3):
                                if int(sensor_IDs[j]) == 2:
                                    sensorM.connect(self._computer_ip, sensor_IPs[j], 0, 0, 0, "Mid-100 (M)")
                                    self._mid100_sensors.append(sensorM)
                                    for k in range(3):
                                        if int(sensor_IDs[k]) == 3:
                                            numFound = sensorR.connect(self._computer_ip, sensor_IPs[k], 0, 0, 0,
                                                                       "Mid-100 (R)")
                                            self._mid100_sensors.append(sensorR)
                                            break
                                    break
                            break
                else:
                    self._show_messages = False
                    numFound = self.connect(self._computer_ip, lidarSensorIPs[0], 0, 0, 0)
                    self._device_type = unique_sensors[0]
                    self.resetShowMessages()

                    self.msg.print("Connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip + " (ID: " + str(
                            self._ip_range_code) + ")")

        else:
            self.msg.print("*** ERROR: Failed to auto determine the computer IP address ***")

        return numFound

    def _disconnect(self):

        if self._is_connected:
            self._is_connected = False
            self._is_data = False
            if self._capture_stream is not None:
                self._capture_stream.stop()
                self._capture_stream = None
            time.sleep(0.1)
            self._is_writing = False

            try:
                self._disconnect_sensor()
                self._heartbeat.stop()
                time.sleep(0.2)
                self._heartbeat = None

                self._data_socket.close()
                self._cmd_socket.close()
                self.msg.print("Disconnected from the Livox " + self._device_type + " at IP: " + self._sensor_ip)

            except:
                self.msg.print("*** Error trying to disconnect from the Livox " + self._device_type + " at IP: " + self._sensor_ip)
        else:
            self.msg.print("Not connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip)

    def disconnect(self):
        self._disconnect()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._disconnect()

    def _reboot(self):

        if self._is_connected:
            self._is_connected = False
            self._is_data = False
            if self._capture_stream is not None:
                self._capture_stream.stop()
                self._capture_stream = None
            time.sleep(0.1)
            self._is_writing = False

            try:
                self._reboot_sensor()
                self._heartbeat.stop()
                time.sleep(0.2)
                self._heartbeat = None

                self._data_socket.close()
                self._cmd_socket.close()
                self.msg.print("Rebooting the Livox " + self._device_type + " at IP: " + self._sensor_ip)

            except:
                self.msg.print("*** Error trying to reboot from the Livox " + self._device_type + " at IP: " + self._sensor_ip)
        else:
            self.msg.print("Not connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip)

    def reboot(self):
        self._reboot()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._reboot()

    def _lidar_spin_up(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_LIDAR_START, "Lidar", 0)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar spin up request")
        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to spin up the lidar")
        elif response == 2:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     lidar is spinning up, please wait...")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect lidar spin up response")
        else:
            print(response)

    def lidarSpinUp(self):
        self._lidar_spin_up()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._lidar_spin_up()

        while True:
            time.sleep(0.1)
            states = []
            states.append(self._heartbeat.work_state)

            for i in range(len(self._mid100_sensors)):
                states.append(self._mid100_sensors[i]._heartbeat.work_state)

            stopper = False
            for i in range(len(states)):
                if states[i] == 1:
                    stopper = True
                else:
                    stopper = False

            if stopper:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     lidar is ready")
                for i in range(len(self._mid100_sensors)):
                    if self._mid100_sensors[i]._showMessages: print(
                        "   " + self._mid100_sensors[i]._sensor_ip + self._mid100_sensors[
                            i]._format_spaces + "   -->     lidar is ready")
                time.sleep(0.1)
                break

    def _lidar_spin_down(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_LIDAR_POWERSAVE, "Lidar", 0)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar spin down request")
        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to spin down the lidar")
        if response == 0:
            pass
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect lidar spin down response")

    def lidarSpinDown(self):
        self._lidar_spin_down()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._lidar_spin_down()

    def _lidar_stand_by(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_LIDAR_START, "Lidar", 0)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar stand-by request")
        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set lidar to stand-by")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect lidar stand-by response")

    def lidarStandBy(self):
        self._lidar_stand_by()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._lidar_stand_by()

    @deprecated(version='1.0.1', reason="You should use .dataStart_RT_B() instead")
    def dataStart(self):

        if self._is_connected:
            if not self._is_data:
                self._capture_stream = DataCaptureThread(self._sensor_ip, self._data_socket, None, "", 0, 0, 0, 0,
                                                         self._show_messages, self._format_spaces, self._device_type)
                time.sleep(0.12)
                self._wait_for_idle()
                self._cmd_socket.sendto(sdkdefs.CMD_DATA_START, (self._sensor_ip, 65000))
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent start data stream request")

                # check for proper response from data start request
                if select.select([self._cmd_socket], [], [], 0.1)[0]:
                    binData, addr = self._cmd_socket.recvfrom(16)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "4":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                        if ret_code == 1:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to start data stream")
                            if self._capture_stream is not None:
                                self._capture_stream.stop()
                            time.sleep(0.1)
                            self._is_data = False
                        else:
                            self._is_data = True
                    else:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect start data stream response")
            else:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     data stream already started")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def _data_start_rt(self):
        if not self._is_data:
            self._capture_stream = DataCaptureThread(self._sensor_ip, self._data_socket, None, "", 1, 0, 0, 0,
                                                     self._show_messages, self._format_spaces, self._device_type)
            time.sleep(0.12)

            response = self.send_command_receive_ack(sdkdefs.CMD_DATA_START, "General", 4)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent start data stream request")
            if response == 1:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to start data stream")
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                time.sleep(0.1)
                self._is_data = False
            elif response == 0:
                self._is_data = True
            elif response == -1:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect start data stream response")
        else:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     data stream already started")

    @deprecated(version='1.1.0', reason="You should use .dataStart_RT_B() instead")
    def dataStart_RT(self):
        self._data_start_rt()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._data_start_rt()

    def _data_start_rt_b(self):
        if not self._is_data:
            self._capture_stream = DataCaptureThread(self._sensor_ip, self._data_socket, self._imu_socket, "", 2, 0, 0,
                                                     0, self._show_messages, self._format_spaces, self._device_type)
            time.sleep(0.12)

            response = self.send_command_receive_ack(sdkdefs.CMD_DATA_START, "General", 4)
            if response == 0:
                self._is_data = True
            elif response == 1:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to start data stream")
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                time.sleep(0.1)
                self._is_data = False
            elif response == -1:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect start data stream response")

        else:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     data stream already started")

    def dataStart_RT_B(self):
        self._data_start_rt_b()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._data_start_rt_b()

    def _data_stop(self):
        if not self._is_data:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     data stream already stopped")
            return
        response = self.send_command_receive_ack(sdkdefs.CMD_DATA_STOP, "General", 4)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent stop data stream request")
        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to stop data stream")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect stop data stream response")

    def dataStop(self):
        self._data_stop()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._data_stop()

    def setDynamicIP(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_DYNAMIC_IP, "General", 8)
        if response == 0:
            self.msg.print("Changed IP from " + self._sensor_ip + " to dynamic IP (DHCP assigned)")
            self.disconnect()
            self.msg.print("\n********** PROGRAM ENDED - MUST REBOOT SENSOR **********\n")
            sys.exit(4)
        else:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to change to dynamic IP (DHCP assigned)")

    def setStaticIP(self, ipAddress):

        if self._is_connected:
            ipRange = ""
            if self._ip_range_code == 1:
                ipRange = "192.168.1.11 to .80"
            elif self._ip_range_code == 2:
                ipRange = "192.168.1.81 to .150"
            elif self._ip_range_code == 3:
                ipRange = "192.168.1.151 to .220"
            ipAddress = self._check_ip(ipAddress)
            if ipAddress:
                IP_parts = ipAddress.split(".")
                IPhex1 = str(hex(int(IP_parts[0]))).replace('0x', '').zfill(2)
                IPhex2 = str(hex(int(IP_parts[1]))).replace('0x', '').zfill(2)
                IPhex3 = str(hex(int(IP_parts[2]))).replace('0x', '').zfill(2)
                IPhex4 = str(hex(int(IP_parts[3]))).replace('0x', '').zfill(2)
                formattedIP = IP_parts[0].strip() + "." + IP_parts[1].strip() + "." + IP_parts[2].strip() + "." + \
                              IP_parts[3].strip()
                cmdString = "AA011400000000a824000801" + IPhex1 + IPhex2 + IPhex3 + IPhex4
                binString = bytes(cmdString, encoding='utf-8')
                crc32checksum = helper.crc32from_str(binString)
                cmdString += crc32checksum
                binString = bytes(cmdString, encoding='utf-8')

                staticIP_request = bytes.fromhex((binString).decode('ascii'))
                self._wait_for_idle()
                self._cmd_socket.sendto(staticIP_request, (self._sensor_ip, 65000))

                # check for proper response from static IP request
                if select.select([self._cmd_socket], [], [], 0.1)[0]:
                    binData, addr = self._cmd_socket.recvfrom(16)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "8":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')

                        if ret_code == 0:
                            self.msg.print("Changed IP from " + self._sensor_ip + " to a static IP of " + formattedIP)
                            self.disconnect()

                            self.msg.print("\n********** PROGRAM ENDED - MUST REBOOT SENSOR **********\n")
                            sys.exit(5)
                        else:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to change static IP (must be " + ipRange + ")")
                    else:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to change static IP (must be " + ipRange + ")")
            else:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to change static IP (must be " + ipRange + ")")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def _set_cartesian_cs(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_CARTESIAN_CS, "General", 5)
        self.msg.print(
            "   " + self._sensor_ip + self._format_spaces + "   <--     sent change to Cartesian coordinates request")
        if response == 0:
            self._coord_system = 0
        elif response == 1:
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set Cartesian coordinate output")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect change coordinate system response (Cartesian)")

    def setCartesianCS(self):
        self._set_cartesian_cs()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_cartesian_cs()

    def _set_spherical_cs(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_SPHERICAL_CS, "General", 5)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent change to Spherical coordinates request")
        if response == 0:
            self._coord_system = 1
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set Spherical coordinate output")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect change coordinate system response (Spherical)")

    def setSphericalCS(self):
        self._set_spherical_cs()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_spherical_cs()

    def readExtrinsic(self):
        response, data = self.send_command_receive_data(sdkdefs.CMD_READ_EXTRINSIC, "Lidar", 2, 40)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent read extrinsic parameters request")
        if response == 0:
            self._roll = struct.unpack('<f', data[12:16])[0]
            self._pitch = struct.unpack('<f', data[16:20])[0]
            self._yaw = struct.unpack('<f', data[20:24])[0]
            self._x = float(struct.unpack('<i', data[24:28])[0]) / 1000.
            self._y = float(struct.unpack('<i', data[28:32])[0]) / 1000.
            self._z = float(struct.unpack('<i', data[32:36])[0]) / 1000.

            # called only to print the extrinsic parameters to the screen if .showMessages(True)
            ack = self.extrinsicParameters()
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to read extrinsic parameters")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect read extrinsics response")
        elif response == -2:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def setExtrinsicToZero(self):
        response = self.send_command_receive_ack(sdkdefs.CMD_WRITE_ZERO_EO, "Lidar", 1)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent set extrinsic parameters to zero request")
        if response == 0:
            self.readExtrinsic()
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set extrinsic parameters to zero")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set extrinsics to zero response")

    def setExtrinsicTo(self, x, y, z, roll, pitch, yaw):
        hex_string_to_add_to_cmd_string = self.turn_values_into_single_hex_string([float(roll),
                                                                                   float(pitch),
                                                                                   float(yaw),
                                                                                   int(np.floor(x * 1000.)),
                                                                                   int(np.floor(y * 1000.)),
                                                                                   int(np.floor(z * 1000.))],
                                                                                  ['<f', '<f', '<f', '<i', '<i', '<i'])

        cmdString = "AA012700000000b5ed0101" + hex_string_to_add_to_cmd_string
        binString = bytes(cmdString, encoding='utf-8')
        crc32checksum = helper.crc32from_str(binString)
        cmdString += crc32checksum
        binString = bytes(cmdString, encoding='utf-8')
        setExtValues = bytes.fromhex((binString).decode('ascii'))

        response = self.send_command_receive_ack(setExtValues, "Lidar", 1)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent set extrinsic parameters request")
        if response == 0:
            self.readExtrinsic()
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set extrinsic parameters")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set extrinsic parameters response")

    def _update_utc(self, year, month, day, hour, microsec):

        if self._is_connected:

            yeari = int(year) - 2000
            if yeari < 0 or yeari > 255:
                yeari = 0

            monthi = int(month)
            if monthi < 1 or monthi > 12:
                monthi = 1

            dayi = int(day)
            if dayi < 1 or dayi > 31:
                dayi = 1

            houri = int(hour)
            if houri < 0 or houri > 23:
                houri = 0

            seci = int(microsec)
            if seci < 0 or seci > int(60 * 60 * 1000000):
                seci = 0

            year_b = str(binascii.hexlify(struct.pack('<B', yeari)))[2:-1]
            month_b = str(binascii.hexlify(struct.pack('<B', monthi)))[2:-1]
            day_b = str(binascii.hexlify(struct.pack('<B', dayi)))[2:-1]
            hour_b = str(binascii.hexlify(struct.pack('<B', houri)))[2:-1]
            sec_b = str(binascii.hexlify(struct.pack('<I', seci)))[2:-1]

            # test case Sept 10, 2020 at 17:15 UTC  -->  AA0117000000006439010A14090A1100E9A435D0337994
            cmdString = "AA0117000000006439010A" + year_b + month_b + day_b + hour_b + sec_b
            binString = bytes(cmdString, encoding='utf-8')
            crc32checksum = helper.crc32from_str(binString)
            cmdString += crc32checksum
            binString = bytes(cmdString, encoding='utf-8')
            setUTCValues = bytes.fromhex((binString).decode('ascii'))

            self._wait_for_idle()
            self._cmd_socket.sendto(setUTCValues, (self._sensor_ip, 65000))
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent update UTC request")

            # check for proper response from update UTC request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                binData, addr = self._cmd_socket.recvfrom(16)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                if ack == "ACK (response)" and cmd_set == "Lidar" and cmd_id == "10":
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    if ret_code == 1:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to update UTC values")
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect update UTC values response")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def updateUTC(self, year, month, day, hour, microsec):
        self._update_utc(year, month, day, hour, microsec)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._update_utc(year, month, day, hour, microsec)

    def _set_rain_fog_suppression(self, OnOff: bool):
        if OnOff:
            response = self.send_command_receive_ack(sdkdefs.CMD_RAIN_FOG_ON, "Lidar", 3)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent turn on rain/fog suppression request")
        else:
            response = self.send_command_receive_ack(sdkdefs.CMD_RAIN_FOG_OFF, "Lidar", 3)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent turn off rain/fog suppression request")

        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set rain/fog suppression value")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set rain/fog suppression response")

    def setRainFogSuppression(self, OnOff):
        self._set_rain_fog_suppression(OnOff)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_rain_fog_suppression(OnOff)

    def _set_fan(self, OnOff):
        if OnOff:
            response = self.send_command_receive_ack(sdkdefs.CMD_FAN_ON, "Lidar", 4)
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   <--     sent turn on fan request")
        else:
            response = self.send_command_receive_ack(sdkdefs.CMD_FAN_OFF, "Lidar", 4)
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   <--     sent turn off fan request")

        if response == 1:
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set fan value")
        elif response == -1:
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set fan response")

    def setFan(self, OnOff):
        self._set_fan(OnOff)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_fan(OnOff)

    def _get_fan(self):

        if self._is_connected:
            self._wait_for_idle()

            self._cmd_socket.sendto(sdkdefs.CMD_GET_FAN, (self._sensor_ip, 65000))
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent get fan state request")

            # check for proper response from get fan request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                binData, addr = self._cmd_socket.recvfrom(17)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                if ack == "ACK (response)" and cmd_set == "Lidar" and cmd_id == "5":
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    if ret_code == 1:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to get fan state value")
                    elif ret_code == 0:
                        value = struct.unpack('<B', binData[12:13])[0]
                        print("   " + self._sensor_ip + self._format_spaces + "   -->     fan state: " + str(value))
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect get fan state response")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def getFan(self):
        self._get_fan()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._get_fan()

    def setLidarReturnMode(self, Mode_ID):
        if Mode_ID == 0:
            response = self.send_command_receive_ack(sdkdefs.CMD_LIDAR_SINGLE_1ST, "Lidar", 6)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent single first return lidar mode request")
        elif Mode_ID == 1:
            response = self.send_command_receive_ack(sdkdefs.CMD_LIDAR_SINGLE_STRONGEST, "Lidar", 6)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent single strongest return lidar mode request")
        elif Mode_ID == 2:
            response = self.send_command_receive_ack(sdkdefs.CMD_LIDAR_DUAL, "Lidar", 6)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent dual return lidar mode request")
        else:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     Invalid mode_id for setting lidar return mode")
            return

        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set lidar mode value")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set lidar mode response")

    def setIMUdataPush(self, OnOff: bool):
        if OnOff:
            response = self.send_command_receive_ack(sdkdefs.CMD_IMU_DATA_ON, "Lidar", 8)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent start IMU data push request")
        else:
            response = self.send_command_receive_ack(sdkdefs.CMD_IMU_DATA_OFF, "Lidar", 8)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent stop IMU data push request")

        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set IMU data push value")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set IMU data push response")

    def getIMUdataPush(self):

        if self._is_connected:
            self._wait_for_idle()

            self._cmd_socket.sendto(sdkdefs.CMD_GET_IMU, (self._sensor_ip, 65000))
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent get IMU push state request")

            # check for proper response from get IMU request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                binData, addr = self._cmd_socket.recvfrom(17)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                if ack == "ACK (response)" and cmd_set == "Lidar" and cmd_id == "9":
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    if ret_code == 1:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to get IMU push state value")
                    elif ret_code == 0:
                        value = struct.unpack('<B', binData[12:13])[0]
                        print("   " + self._sensor_ip + self._format_spaces + "   -->     IMU push state: " + str(value))
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     incorrect get IMU push state response")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    @deprecated(version='1.0.2', reason="You should use saveDataToFile instead")
    def saveDataToCSV(self, filePathAndName, secsToWait, duration):

        if self._is_connected:
            if self._is_data:
                if self._firmware != "UNKNOWN":
                    try:
                        firmwareType = sdkdefs.SPECIAL_FIRMWARE_TYPE_DICT[self._firmware]
                    except:
                        firmwareType = 1

                    if duration < 0:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, negative duration")
                    else:
                        # max duration = 4 years - 1 sec
                        if duration >= 126230400:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, duration too big")
                        else:

                            if secsToWait < 0:
                                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, negative time to wait")
                            else:
                                # max time to wait = 15 mins
                                if secsToWait > 900:
                                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, time to wait too big")
                                else:

                                    if filePathAndName == "":
                                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, file path and name missing")
                                    else:

                                        if filePathAndName[-4:].upper() != ".CSV":
                                            filePathAndName += ".csv"

                                        self._is_writing = True
                                        self._capture_stream.filePathAndName = filePathAndName
                                        self._capture_stream.secsToWait = secsToWait
                                        self._capture_stream.duration = duration
                                        self._capture_stream.firmwareType = firmwareType
                                        self._capture_stream._showMessages = self._show_messages
                                        time.sleep(0.1)
                                        self._capture_stream.isCapturing = True
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     unknown firmware version")
            else:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     WARNING: data stream not started, no CSV file created")

    @deprecated(version='1.0.2', reason="You should use closeFile instead")
    def closeCSV(self):
        if self._is_connected:
            if self._is_writing:
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                self._is_writing = False

    def _saveDataToFile(self, filePathAndName, secsToWait, duration):

        if self._is_connected:
            if self._is_data:
                if self._firmware != "UNKNOWN":
                    try:
                        firmwareType = sdkdefs.SPECIAL_FIRMWARE_TYPE_DICT[self._firmware]
                    except:
                        firmwareType = 1

                    if duration < 0:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, negative duration")
                    else:
                        # max duration = 4 years - 1 sec
                        if duration >= 126230400:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, duration too big")
                        else:

                            if secsToWait < 0:
                                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, negative time to wait")
                            else:
                                # max time to wait = 15 mins
                                if secsToWait > 900:
                                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, time to wait too big")
                                else:

                                    if filePathAndName == "":
                                        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     * ISSUE: saving data, file path and name missing")
                                    else:

                                        self._is_writing = True
                                        self._capture_stream.file_path_and_name = filePathAndName
                                        self._capture_stream.secs_to_wait = secsToWait
                                        self._capture_stream.duration = duration
                                        self._capture_stream.firmware_type = firmwareType
                                        self._capture_stream._show_messages = self._show_messages
                                        time.sleep(0.1)
                                        self._capture_stream.is_capturing = True
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     unknown firmware version")
            else:
                self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     WARNING: data stream not started, no data file created")

    def saveDataToFile(self, filePathAndName, secsToWait, duration):
        path_file = Path(filePathAndName)
        filename = path_file.stem
        exten = path_file.suffix
        self._saveDataToFile(filePathAndName, secsToWait, duration)
        for i in range(len(self._mid100_sensors)):
            new_file = ""
            if i == 0:
                new_file = filename + "_M" + exten
            elif i == 1:
                new_file = filename + "_R" + exten

            self._mid100_sensors[i]._saveDataToFile(new_file, secsToWait, duration)

    def _closeFile(self):
        if self._is_connected:
            if self._is_writing:
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                self._is_writing = False

    def closeFile(self):
        self._closeFile()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._closeFile()

    def _resetShowMessages(self):
        self._show_messages = self._init_show_messages

    def resetShowMessages(self):
        self._resetShowMessages()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._resetShowMessages()

    def _connectionParameters(self):
        if self._is_connected:
            return [self._computer_ip, self._sensor_ip, str(self._data_port), str(self._cmd_port), str(self._imu_port)]

    def connectionParameters(self):
        sensorIPs = []
        dataPorts = []
        cmdPorts = []
        imuPorts = []

        params = self._connectionParameters()
        sensorIPs.append(params[1])
        dataPorts.append(params[2])
        cmdPorts.append(params[3])
        imuPorts.append(params[4])

        for i in range(len(self._mid100_sensors)):
            params = self._mid100_sensors[i]._connectionParameters()
            sensorIPs.append(params[1])
            dataPorts.append(params[2])
            cmdPorts.append(params[3])
            imuPorts.append(params[4])

        if self._show_messages:
            print("      Computer IP Address:    " + params[0])
            print("      Sensor IP Address(es):  " + str(sensorIPs))
            print("      Data Port Number(s):    " + str(dataPorts))
            print("      Command Port Number(s): " + str(cmdPorts))
            if self._device_type == "Horizon" or self._device_type == "Tele-15":
                print("      IMU Port Number(s):     " + str(imuPorts))

        return [params[0], sensorIPs, dataPorts, cmdPorts, imuPorts]

    def extrinsicParameters(self):
        if self._is_connected:
            if self._show_messages:
                print("      x: " + str(self._x) + " m")
                print("      y: " + str(self._y) + " m")
                print("      z: " + str(self._z) + " m")
                print("      roll: " + "{0:.2f}".format(self._roll) + " deg")
                print("      pitch: " + "{0:.2f}".format(self._pitch) + " deg")
                print("      yaw: " + "{0:.2f}".format(self._yaw) + " deg")

            return [self._x, self._y, self._z, self._roll, self._pitch, self._yaw]

    def firmware(self):
        if self._is_connected:
            if self._show_messages:
                print("   " + self._sensor_ip + self._format_spaces + "   -->     F/W Version: " + self._firmware)
                for i in range(len(self._mid100_sensors)):
                    print("   " + self._mid100_sensors[i]._sensor_ip + self._mid100_sensors[
                        i]._format_spaces + "   -->     F/W Version: " + self._mid100_sensors[i]._firmware)

            return self._firmware

    def serialNumber(self):
        if self._is_connected:
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   -->     Serial # " + self._serial)

            return self._serial

    def showMessages(self, new_value):
        self._show_messages = bool(new_value)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._showMessages = bool(new_value)

    def lidarStatusCodes(self):
        if self._is_connected:
            if self._capture_stream is not None:
                codes = self._capture_stream.status_codes()
                if self._show_messages:
                    sys_mess = "UNKNOWN"
                    if codes[0] == 0:
                        sys_mess = "OK"
                    elif codes[0] == 1:
                        sys_mess = "* WARNING *"
                    elif codes[0] == 2:
                        sys_mess = "*** ERROR ***"

                    temp_mess = "UNKNOWN"
                    if codes[1] == 0:
                        temp_mess = "OK"
                    elif codes[1] == 1:
                        temp_mess = "High/Low Warning"
                    elif codes[1] == 2:
                        temp_mess = "Extremely High/Low Error"

                    volt_mess = "UNKNOWN"
                    if codes[2] == 0:
                        volt_mess = "OK"
                    elif codes[2] == 1:
                        volt_mess = "High Warning"
                    elif codes[2] == 2:
                        volt_mess = "Extremely High Error"

                    motor_mess = "UNKNOWN"
                    if codes[3] == 0:
                        motor_mess = "OK"
                    elif codes[3] == 1:
                        motor_mess = "Warning State"
                    elif codes[3] == 2:
                        motor_mess = "Error State"

                    dirty_mess = "UNKNOWN"
                    if codes[4] == 0:
                        dirty_mess = "OK"
                    elif codes[4] == 1:
                        dirty_mess = "Dirty/Blocked Warning"

                    firmware_mess = "UNKNOWN"
                    if codes[5] == 0:
                        firmware_mess = "OK"
                    elif codes[5] == 1:
                        firmware_mess = "Abnormal Error"

                    pps_mess = "UNKNOWN"
                    if codes[6] == 0:
                        pps_mess = "OK, but not detected"
                    elif codes[6] == 1:
                        pps_mess = "OK"

                    device_mess = "UNKNOWN"
                    if codes[7] == 0:
                        device_mess = "OK"
                    elif codes[7] == 1:
                        device_mess = "Approaching End of Service Life Warning"

                    fan_mess = "UNKNOWN"
                    if codes[8] == 0:
                        fan_mess = "OK"
                    elif codes[8] == 1:
                        fan_mess = "Fan Warning"

                    heating_mess = "UNKNOWN"
                    if codes[9] == 0:
                        heating_mess = "Low Temp. Heating ON"
                    elif codes[9] == 1:
                        heating_mess = "Low Temp. Heating OFF"

                    ptp_mess = "UNKNOWN"
                    if codes[10] == 0:
                        ptp_mess = "No 1588 Signal"
                    elif codes[10] == 1:
                        ptp_mess = "1588 Signal OK"

                    time_sync_mess = "UNKNOWN"
                    if codes[11] == 0:
                        time_sync_mess = "Internal clock sync."
                    elif codes[11] == 1:
                        time_sync_mess = "PTP 1588 sync."
                    elif codes[11] == 2:
                        time_sync_mess = "GPS sync."
                    elif codes[11] == 3:
                        time_sync_mess = "PPS sync."
                    elif codes[11] == 4:
                        time_sync_mess = "Abnormal time sync."

                    print("      System Status:         " + sys_mess)
                    print("      Temperature Status:    " + temp_mess)
                    print("      Voltage Status:        " + volt_mess)
                    print("      Motor Status:          " + motor_mess)
                    print("      Clean Status:          " + dirty_mess)
                    print("      Firmware Status:       " + firmware_mess)
                    print("      PPS Status:            " + pps_mess)
                    print("      Device Status:         " + device_mess)
                    print("      Fan Status:            " + fan_mess)
                    print("      Self Heating Status:   " + heating_mess)
                    print("      PTP Status:            " + ptp_mess)
                    print("      Time Sync. Status:     " + time_sync_mess)

                return codes

            else:
                if self._show_messages:
                    print("      System Status:         UNKNOWN")
                    print("      Temperature Status:    UNKNOWN")
                    print("      Voltage Status:        UNKNOWN")
                    print("      Motor Status:          UNKNOWN")
                    print("      Dirty Status:          UNKNOWN")
                    print("      Firmware Status:       UNKNOWN")
                    print("      PPS Status:            UNKNOWN")
                    print("      Device Status:         UNKNOWN")
                    print("      Fan Status:            UNKNOWN")
                    print("      Self Heating Status:   UNKNOWN")
                    print("      PTP Status:            UNKNOWN")
                    print("      Time Sync. Status:     UNKNOWN")

                return [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1]

    def _doneCapturing(self):
        # small sleep to ensure this command isn't continuously called if in a while True loop
        time.sleep(0.01)
        if self._capture_stream is not None:
            if self._capture_stream.duration != 126230400:
                return not (self._capture_stream.started)
            else:
                return True
        else:
            return True

    def doneCapturing(self):
        stop = []
        stop.append(self._doneCapturing())
        for i in range(len(self._mid100_sensors)):
            stop.append(self._mid100_sensors[i]._doneCapturing())

        return all(stop)


openpylivox = OpenPyLivox


def allDoneCapturing(sensors):
    stop = []
    for i in range(0, len(sensors)):
        if sensors[i]._capture_stream is not None:
            stop.append(sensors[i].doneCapturing())

    # small sleep to ensure this command isn't continuously called if in a while True loop
    time.sleep(0.01)
    return all(stop)


def _convertBin2CSV(filePathAndName, deleteBin):
    binFile = None
    csvFile = None
    imuFile = None
    imu_csvFile = None

    try:
        dataClass = 0
        if os.path.exists(filePathAndName) and os.path.isfile(filePathAndName):
            bin_size = Path(filePathAndName).stat().st_size - 15
            binFile = open(filePathAndName, "rb")

            checkMessage = (binFile.read(11)).decode('UTF-8')
            if checkMessage == "OPENPYLIVOX":
                with open(filePathAndName + ".csv", "w", 1) as csvFile:
                    firmwareType = struct.unpack('<h', binFile.read(2))[0]
                    dataType = struct.unpack('<h', binFile.read(2))[0]
                    divisor = 1

                    if firmwareType >= 1 and firmwareType <= 3:
                        if dataType >= 0 and dataType <= 5:
                            print("CONVERTING OPL BINARY DATA, PLEASE WAIT...")
                            if firmwareType == 1 and dataType == 0:
                                csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                                dataClass = 1
                                divisor = 21
                            elif firmwareType == 1 and dataType == 1:
                                csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")
                                dataClass = 2
                                divisor = 17
                            elif firmwareType > 1 and dataType == 0:
                                csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                                dataClass = 3
                                divisor = 22
                            elif firmwareType > 1 and dataType == 1:
                                csvFile.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")
                                dataClass = 4
                                divisor = 18
                            elif firmwareType == 1 and dataType == 2:
                                csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                dataClass = 5
                                divisor = 22
                            elif firmwareType == 1 and dataType == 3:
                                csvFile.write(
                                    "//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                dataClass = 6
                                divisor = 18
                            elif firmwareType == 1 and dataType == 4:
                                csvFile.write("//X,Y,Z,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                dataClass = 7
                                divisor = 36
                            elif firmwareType == 1 and dataType == 5:
                                csvFile.write(
                                    "//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                dataClass = 8
                                divisor = 24

                            num_recs = int(bin_size / divisor)
                            pbari = tqdm(total=num_recs, unit=" pts", desc="   ")

                            while True:
                                try:
                                    # Mid-40/100 Cartesian single return
                                    if dataClass == 1:
                                        coord1 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord3 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        intensity = int.from_bytes(binFile.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                        csvFile.write("{0:.3f}".format(coord1) + "," + "{0:.3f}".format(
                                            coord2) + "," + "{0:.3f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + ",1\n")

                                    # Mid-40/100 Spherical single return
                                    elif dataClass == 2:
                                        coord1 = float(struct.unpack('<I', binFile.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        intensity = int.from_bytes(binFile.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                        csvFile.write("{0:.3f}".format(coord1) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + ",1\n")

                                    # Mid-40/100 Cartesian multiple return
                                    elif dataClass == 3:
                                        coord1 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord3 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        intensity = int.from_bytes(binFile.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                        returnNum = (binFile.read(1)).decode('UTF-8')
                                        csvFile.write("{0:.3f}".format(coord1) + "," + "{0:.3f}".format(
                                            coord2) + "," + "{0:.3f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," + returnNum + "\n")

                                    # Mid-40/100 Spherical multiple return
                                    elif dataClass == 4:
                                        coord1 = float(struct.unpack('<I', binFile.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        intensity = int.from_bytes(binFile.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                        returnNum = (binFile.read(1)).decode('UTF-8')
                                        csvFile.write("{0:.3f}".format(coord1) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," + returnNum + "\n")

                                    # Horizon/Tele-15 Cartesian single return (SDK Data Type 2)
                                    elif dataClass == 5:
                                        coord1 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord3 = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        intensity = struct.unpack('<B', binFile.read(1))[0]
                                        tag_bits = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[
                                                   2:].zfill(8)
                                        spatial_conf = str(int(tag_bits[0:2], 2))
                                        intensity_conf = str(int(tag_bits[2:4], 2))
                                        returnType = str(int(tag_bits[4:6], 2))
                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                        csvFile.write("{0:.3f}".format(coord1) + "," + "{0:.3f}".format(
                                            coord2) + "," + "{0:.3f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + returnType + ","
                                                      + spatial_conf + "," + intensity_conf + "\n")

                                    # Horizon/Tele-15 Spherical single return (SDK Data Type 3)
                                    elif dataClass == 6:
                                        coord1 = float(struct.unpack('<I', binFile.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        intensity = struct.unpack('<B', binFile.read(1))[0]
                                        tag_bits = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[
                                                   2:].zfill(8)
                                        spatial_conf = str(int(tag_bits[0:2], 2))
                                        intensity_conf = str(int(tag_bits[2:4], 2))
                                        returnType = str(int(tag_bits[4:6], 2))
                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                        csvFile.write("{0:.3f}".format(coord1) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + returnType + ","
                                                      + spatial_conf + "," + intensity_conf + "\n")

                                    # Horizon/Tele-15 Cartesian dual return (SDK Data Type 4)
                                    elif dataClass == 7:
                                        coord1a = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord2a = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord3a = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        intensitya = struct.unpack('<B', binFile.read(1))[0]
                                        tag_bitsa = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_confa = str(int(tag_bitsa[0:2], 2))
                                        intensity_confa = str(int(tag_bitsa[2:4], 2))
                                        returnTypea = str(int(tag_bitsa[4:6], 2))

                                        coord1b = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord2b = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        coord3b = float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0
                                        intensityb = struct.unpack('<B', binFile.read(1))[0]
                                        tag_bitsb = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_confb = str(int(tag_bitsb[0:2], 2))
                                        intensity_confb = str(int(tag_bitsb[2:4], 2))
                                        returnTypeb = str(int(tag_bitsb[4:6], 2))

                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])

                                        csvFile.write("{0:.3f}".format(coord1a) + "," + "{0:.3f}".format(
                                            coord2a) + "," + "{0:.3f}".format(coord3a) + "," + str(
                                            intensitya) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + returnTypea + ","
                                                      + spatial_confa + "," + intensity_confa + "\n")

                                        csvFile.write("{0:.3f}".format(coord1b) + "," + "{0:.3f}".format(
                                            coord2b) + "," + "{0:.3f}".format(coord3b) + "," + str(
                                            intensityb) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",2," + returnTypeb + ","
                                                      + spatial_confb + "," + intensity_confb + "\n")

                                    # Horizon/Tele-15 Spherical dual return (SDK Data Type 5)
                                    elif dataClass == 8:
                                        coord2 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', binFile.read(2))[0]) / 100.0
                                        coord1a = float(struct.unpack('<I', binFile.read(4))[0]) / 1000.0
                                        intensitya = struct.unpack('<B', binFile.read(1))[0]
                                        tag_bitsa = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_confa = str(int(tag_bitsa[0:2], 2))
                                        intensity_confa = str(int(tag_bitsa[2:4], 2))
                                        returnTypea = str(int(tag_bitsa[4:6], 2))

                                        coord1b = float(struct.unpack('<I', binFile.read(4))[0]) / 1000.0
                                        intensityb = struct.unpack('<B', binFile.read(1))[0]
                                        tag_bitsb = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_confb = str(int(tag_bitsb[0:2], 2))
                                        intensity_confb = str(int(tag_bitsb[2:4], 2))
                                        returnTypeb = str(int(tag_bitsb[4:6], 2))

                                        timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])

                                        csvFile.write("{0:.3f}".format(coord1a) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensitya) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + returnTypea + ","
                                                      + spatial_confa + "," + intensity_confa + "\n")

                                        csvFile.write("{0:.3f}".format(coord1b) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensityb) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",2," + returnTypeb + ","
                                                      + spatial_confb + "," + intensity_confb + "\n")

                                    pbari.update(1)

                                except:
                                    break

                            pbari.close()
                            binFile.close()
                            print(
                                "   - Point data was converted successfully to CSV, see file: " + filePathAndName + ".csv")
                            if deleteBin:
                                os.remove(filePathAndName)
                                print("     * OPL point data binary file has been deleted")
                            print()
                            time.sleep(0.5)
                        else:
                            print("*** ERROR: The OPL point data binary file reported a wrong data type ***")
                            binFile.close()
                    else:
                        print("*** ERROR: The OPL point data binary file reported a wrong firmware type ***")
                        binFile.close()

                # check for and convert IMU BIN data (if it exists)
                path_file = Path(filePathAndName)
                filename = path_file.stem
                exten = path_file.suffix
                IMU_file = filename + "_IMU" + exten

                if os.path.exists(IMU_file) and os.path.isfile(IMU_file):
                    bin_size2 = Path(IMU_file).stat().st_size - 15
                    num_recs = int(bin_size2 / 32)
                    binFile2 = open(IMU_file, "rb")

                    checkMessage = (binFile2.read(15)).decode('UTF-8')
                    if checkMessage == "OPENPYLIVOX_IMU":
                        with open(IMU_file + ".csv", "w", 1) as csvFile2:
                            csvFile2.write("//gyro_x,gyro_y,gyro_z,acc_x,acc_y,acc_z,time\n")
                            pbari2 = tqdm(total=num_recs, unit=" records", desc="   ")
                            while True:
                                try:
                                    gyro_x = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    gyro_y = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    gyro_z = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    acc_x = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    acc_y = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    acc_z = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    timestamp_sec = "{0:.6f}".format(struct.unpack('<d', binFile2.read(8))[0])

                                    csvFile2.write(gyro_x + "," + gyro_y + "," + gyro_z + "," + acc_x + "," + acc_y +
                                                   "," + acc_z + "," + timestamp_sec + "\n")

                                    pbari2.update(1)

                                except:
                                    break

                            pbari2.close()
                            binFile2.close()
                            print("   - IMU data was converted successfully to CSV, see file: " + IMU_file + ".csv")
                            if deleteBin:
                                os.remove(IMU_file)
                                print("     * OPL IMU data binary file has been deleted")
                    else:
                        print("*** ERROR: The file was not recognized as an OpenPyLivox binary IMU data file ***")
                        binFile2.close()
            else:
                print("*** ERROR: The file was not recognized as an OpenPyLivox binary point data file ***")
                binFile.close()
    except:
        binFile.close()
        print("*** ERROR: An unknown error occurred while converting OPL binary data ***")


def convertBin2CSV(filePathAndName, deleteBin=False):
    print()
    path_file = Path(filePathAndName)
    filename = path_file.stem
    exten = path_file.suffix

    if os.path.isfile(filePathAndName):
        _convertBin2CSV(filePathAndName, deleteBin)

    if os.path.isfile(filename + "_M" + exten):
        _convertBin2CSV(filename + "_M" + exten, deleteBin)

    if os.path.isfile(filename + "_R" + exten):
        _convertBin2CSV(filename + "_R" + exten, deleteBin)


def _convertBin2LAS(filePathAndName, deleteBin):
    binFile = None
    csvFile = None
    imuFile = None
    imu_csvFile = None

    try:
        dataClass = 0
        if os.path.exists(filePathAndName) and os.path.isfile(filePathAndName):
            bin_size = Path(filePathAndName).stat().st_size - 15
            binFile = open(filePathAndName, "rb")

            checkMessage = (binFile.read(11)).decode('UTF-8')
            if checkMessage == "OPENPYLIVOX":
                firmwareType = struct.unpack('<h', binFile.read(2))[0]
                dataType = struct.unpack('<h', binFile.read(2))[0]
                divisor = 1

                if firmwareType >= 1 and firmwareType <= 3:
                    # LAS file creation only works with Cartesian data types (decided not to convert spherical obs.)
                    if dataType == 0 or dataType == 2 or dataType == 4:
                        print("CONVERTING OPL BINARY DATA, PLEASE WAIT...")

                        coord1s = []
                        coord2s = []
                        coord3s = []
                        intensity = []
                        times = []
                        returnNums = []

                        if firmwareType == 1 and dataType == 0:
                            dataClass = 1
                            divisor = 21
                        elif firmwareType > 1 and dataType == 0:
                            dataClass = 3
                            divisor = 22
                        elif firmwareType == 1 and dataType == 2:
                            dataClass = 5
                            divisor = 22
                        elif firmwareType == 1 and dataType == 4:
                            dataClass = 7
                            divisor = 36

                        num_recs = int(bin_size / divisor)
                        pbari = tqdm(total=num_recs, unit=" pts", desc="   ")

                        while True:
                            try:
                                # Mid-40/100 Cartesian single return
                                if dataClass == 1:
                                    coord1s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    intensity.append(int.from_bytes(binFile.read(1), byteorder='little'))
                                    times.append(float(struct.unpack('<d', binFile.read(8))[0]))
                                    returnNums.append(1)

                                # Mid-40/100 Cartesian multiple return
                                elif dataClass == 3:
                                    coord1s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    intensity.append(int.from_bytes(binFile.read(1), byteorder='little'))
                                    times.append(float(struct.unpack('<d', binFile.read(8))[0]))
                                    returnNums.append(int((binFile.read(1)).decode('UTF-8')))

                                # Horizon/Tele-15 Cartesian single return (SDK Data Type 2)
                                elif dataClass == 5:
                                    coord1s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    intensity.append(struct.unpack('<B', binFile.read(1))[0])
                                    tag_bits = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[2:].zfill(
                                        8)
                                    times.append(float(struct.unpack('<d', binFile.read(8))[0]))
                                    returnNums.append(1)

                                # Horizon/Tele-15 Cartesian dual return (SDK Data Type 4)
                                elif dataClass == 7:
                                    coord1s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    intensity.append(struct.unpack('<B', binFile.read(1))[0])
                                    tag_bits = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[2:].zfill(
                                        8)
                                    returnNums.append(1)

                                    coord1s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', binFile.read(4))[0]) / 1000.0)
                                    intensity.append(struct.unpack('<B', binFile.read(1))[0])
                                    tag_bits = str(bin(int.from_bytes(binFile.read(1), byteorder='little')))[2:].zfill(
                                        8)
                                    returnNums.append(2)

                                    timestamp_sec = float(struct.unpack('<d', binFile.read(8))[0])
                                    times.append(timestamp_sec)
                                    times.append(timestamp_sec)

                                pbari.update(1)

                            except:
                                break

                        # save lists of point data attributes to LAS file
                        hdr = laspy.header.Header()
                        hdr.version = "1.2"
                        hdr.data_format_id = 3

                        # the following ID fields must be less than or equal to 32 characters in length
                        System_ID = "OpenPyLivox"
                        Software_ID = "OpenPyLivox V1.1.0"

                        if len(System_ID) < 32:
                            missingLength = 32 - len(System_ID)
                            for i in range(0, missingLength):
                                System_ID += " "

                        if len(Software_ID) < 32:
                            missingLength = 32 - len(Software_ID)
                            for i in range(0, missingLength):
                                Software_ID += " "

                        hdr.system_id = System_ID
                        hdr.software_id = Software_ID

                        lasfile = laspy.file.File(filePathAndName + ".las", mode="w", header=hdr)

                        coord1s = np.asarray(coord1s, dtype=np.float32)
                        coord2s = np.asarray(coord2s, dtype=np.float32)
                        coord3s = np.asarray(coord3s, dtype=np.float32)

                        xmin = np.floor(np.min(coord1s))
                        ymin = np.floor(np.min(coord2s))
                        zmin = np.floor(np.min(coord3s))
                        lasfile.header.offset = [xmin, ymin, zmin]

                        lasfile.header.scale = [0.001, 0.001, 0.001]

                        lasfile.x = coord1s
                        lasfile.y = coord2s
                        lasfile.z = coord3s
                        lasfile.gps_time = np.asarray(times, dtype=np.float32)
                        lasfile.intensity = np.asarray(intensity, dtype=np.int16)
                        lasfile.return_num = np.asarray(returnNums, dtype=np.int8)

                        lasfile.close()

                        pbari.close()
                        binFile.close()
                        print(
                            "   - Point data was converted successfully to LAS, see file: " + filePathAndName + ".las")
                        if deleteBin:
                            os.remove(filePathAndName)
                            print("     * OPL point data binary file has been deleted")
                        print()
                        time.sleep(0.5)
                    else:
                        print("*** ERROR: Only Cartesian point data can be converted to an LAS file ***")
                        binFile.close()
                else:
                    print("*** ERROR: The OPL point data binary file reported a wrong firmware type ***")
                    binFile.close()

                # check for and convert IMU BIN data (if it exists)
                path_file = Path(filePathAndName)
                filename = path_file.stem
                exten = path_file.suffix
                IMU_file = filename + "_IMU" + exten

                if os.path.exists(IMU_file) and os.path.isfile(IMU_file):
                    bin_size2 = Path(IMU_file).stat().st_size - 15
                    num_recs = int(bin_size2 / 32)
                    binFile2 = open(IMU_file, "rb")

                    checkMessage = (binFile2.read(15)).decode('UTF-8')
                    if checkMessage == "OPENPYLIVOX_IMU":
                        with open(IMU_file + ".csv", "w", 1) as csvFile2:
                            csvFile2.write("//gyro_x,gyro_y,gyro_z,acc_x,acc_y,acc_z,time\n")
                            pbari2 = tqdm(total=num_recs, unit=" records", desc="   ")
                            while True:
                                try:
                                    gyro_x = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    gyro_y = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    gyro_z = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    acc_x = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    acc_y = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    acc_z = "{0:.6f}".format(struct.unpack('<f', binFile2.read(4))[0])
                                    timestamp_sec = "{0:.6f}".format(struct.unpack('<d', binFile2.read(8))[0])

                                    csvFile2.write(gyro_x + "," + gyro_y + "," + gyro_z + "," + acc_x + "," + acc_y +
                                                   "," + acc_z + "," + timestamp_sec + "\n")

                                    pbari2.update(1)

                                except:
                                    break

                            pbari2.close()
                            binFile2.close()
                            print("   - IMU data was converted successfully to CSV, see file: " + IMU_file + ".csv")
                            if deleteBin:
                                os.remove(IMU_file)
                                print("     * OPL IMU data binary file has been deleted")
                    else:
                        print("*** ERROR: The file was not recognized as an OpenPyLivox binary IMU data file ***")
                        binFile2.close()
            else:
                print("*** ERROR: The file was not recognized as an OpenPyLivox binary point data file ***")
                binFile.close()
    except:
        binFile.close()
        print("*** ERROR: An unknown error occurred while converting OPL binary data ***")


def convertBin2LAS(filePathAndName, deleteBin=False):
    print()
    path_file = Path(filePathAndName)
    filename = path_file.stem
    exten = path_file.suffix

    if os.path.isfile(filePathAndName):
        _convertBin2LAS(filePathAndName, deleteBin)

    if os.path.isfile(filename + "_M" + exten):
        _convertBin2LAS(filename + "_M" + exten, deleteBin)

    if os.path.isfile(filename + "_R" + exten):
        _convertBin2LAS(filename + "_R" + exten, deleteBin)
