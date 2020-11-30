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
import openpylivox.livox_sdk_defines as SDKDefs


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
        self.msg = helper.Msg(show_message=show_messages, default_arrow="-->")
        self._device_type = "UNKNOWN"
        self._mid100_sensors = []
        self._format_spaces = ""

    @property
    def _sensor_ip(self):
        return self._hidden_sensor_ip

    @_sensor_ip.setter
    def _sensor_ip(self, value):
        self._hidden_sensor_ip = value
        self.msg.sensor_ip = value

    @property
    def _format_spaces(self):
        return self._hidden_format_spaces

    @_format_spaces.setter
    def _format_spaces(self, value):
        self._hidden_format_spaces = value
        self.msg.format_spaces = value

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

            read_data = True
            while read_data:

                bin_data, addr = server_sock_init.recvfrom(34)

                if len(addr) == 2:
                    if addr[1] == 65000:
                        if len(ips) == 0:
                            good_data, cmd_message, data_message, device_serial, type_message, ip_range_code = \
                                self._info(bin_data)
                            sensor_types.append(type_message)
                            ips.append(self._check_ip(addr[0]))
                            serials.append(device_serial)
                            ip_range_codes.append(ip_range_code)

                        else:
                            exists_already = False
                            for i in range(0, len(ips)):
                                if addr[0] == ips[i]:
                                    exists_already = True
                            if exists_already:
                                read_data = False
                                break
                            else:
                                good_data, cmd_message, data_message, device_serial, type_message, ip_range_code = \
                                    self._info(bin_data)
                                sensor_types.append(type_message)
                                ips.append(self._check_ip(addr[0]))
                                serials.append(device_serial)
                                ip_range_codes.append(ip_range_code)

                else:
                    read_data = False

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
                bin_data, addr = self._cmd_socket.recvfrom(16)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)
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
        response = self.send_command_receive_ack(SDKDefs.CMD_DISCONNECT, "General", 6)
        # self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar disconnect request")
        # TODO: Put into logging
        if response == 1:
            self.msg.prefix_print("     FAILED to disconnect")
        elif response == -1:
            self.msg.prefix_print("     incorrect disconnect response")

    def _reboot_sensor(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_REBOOT, "General", 10)
        if response == 1:
            self.msg.prefix_print("     FAILED to reboot")
        elif response == -1:
            self.msg.prefix_print("     incorrect reboot response")

    def _query(self):
        response, data = self.send_command_receive_data(SDKDefs.CMD_QUERY, "General", 2, 20)
        if response == 0:
            _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, data)
            aa = str(int.from_bytes(ret_code_bin[1], byteorder='little')).zfill(2)
            bb = str(int.from_bytes(ret_code_bin[2], byteorder='little')).zfill(2)
            cc = str(int.from_bytes(ret_code_bin[3], byteorder='little')).zfill(2)
            dd = str(int.from_bytes(ret_code_bin[4], byteorder='little')).zfill(2)
            self._firmware = aa + "." + bb + "." + cc + dd
            # self.msg.print(f"Firmware version is {self._firmware}")
        elif response == 1:
            self.msg.prefix_print("     FAILED to receive query results")
        elif response == -1:
            self.msg.prefix_print("     incorrect query response")

    def _info(self, bin_data):

        good_data, cmd_message, data_message, data_id, data_bytes = _parse_resp(self._show_messages, bin_data)
        ip_range_code = None
        device_serial, type_message = "", ""

        if good_data:
            # received broadcast message
            if cmd_message == "MSG (message)" and data_message == "General" and data_id == "0":
                device_broadcast_code = ""
                i = 0
                for i in range(0, 16):
                    device_broadcast_code += data_bytes[i].decode('ascii')

                ip_range_code = int(device_broadcast_code[14:15])  # used to define L,M,R sensors in the Mid-100
                device_serial = device_broadcast_code[:-2]
                device_type = int.from_bytes(data_bytes[i + 1], byteorder='little')

                if device_type == 0:
                    type_message = "Hub    "
                elif device_type == 1:
                    type_message = "Mid-40 "
                elif device_type == 2:
                    type_message = "Tele-15"
                elif device_type == 3:
                    type_message = "Horizon"
                else:
                    type_message = "UNKNOWN"

        return good_data, cmd_message, data_message, device_serial, type_message, ip_range_code

    def discover(self, manual_computer_ip=""):

        if not manual_computer_ip:
            self._auto_computer_ip()
        else:
            self._computer_ip = manual_computer_ip

        if self._computer_ip:
            print("\nUsing computer IP address: " + self._computer_ip + "\n")

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
                elif count == 3:
                    unique_sensors.append("Mid-100")
                ip_groups.append(ips[:-1])
                id_groups.append(ids[:-1])

            if len(unique_serial_nums) > 0:
                for i in range(len(unique_serial_nums)):
                    ips_list = ip_groups[i].split(',')
                    ids_list = id_groups[i].split(',')
                    ips_mess = ""
                    last_ip_num = []
                    for j in range(len(ips_list)):
                        last_ip_num.append(int(ips_list[j].split('.')[3]))
                    last_ip_num.sort()
                    for j in range(len(last_ip_num)):
                        for k in range(len(ips_list)):
                            if last_ip_num[j] == int(ips_list[k].split('.')[3]):
                                num_spaces = " "
                                if last_ip_num[j] < 100:
                                    num_spaces += " "
                                if last_ip_num[j] < 10:
                                    num_spaces += " "
                                ips_mess += str(ips_list[k]) + num_spaces + "(ID: " + str(
                                    ids_list[k]) + ")\n                 "
                                break
                    print("   *** Discovered a Livox sensor ***")
                    print("           Type: " + unique_sensors[i])
                    print("         Serial: " + unique_serial_nums[i])
                    print("          IP(s): " + ips_mess)

            else:
                print("Did not discover any Livox sensors, check communication and power cables and network settings")

        else:
            print("*** ERROR: Failed to auto determine computer IP address ***")

    # TODO: Refactor. But holy shit this looks like hell.
    def connect(self, computer_ip, sensor_ip, data_port, cmd_port, imu_port, sensor_name_override=""):

        num_found = 0

        if not self._is_connected:

            self._computer_ip = self._check_ip(computer_ip)
            self._sensor_ip = self._check_ip(sensor_ip)
            self._data_port = self._check_port(data_port)
            self._cmd_port = self._check_port(cmd_port)
            self._imu_port = self._check_port(imu_port)

            if self._computer_ip and \
                    self._sensor_ip and \
                    self._data_port != -1 and \
                    self._cmd_port != -1 and \
                    self._imu_port != -1:
                num_spaces = 15 - len(self._sensor_ip)
                for i in range(num_spaces):
                    self._format_spaces += " "
                unique_serial_nums, unique_sensors, sensor_ips = self._re_init()
                num_found = len(unique_sensors)
                time.sleep(0.1)

                for i in range(len(sensor_ips)):
                    if self._sensor_ip == sensor_ips[i][0]:
                        self._device_type = sensor_ips[i][1]

                if sensor_name_override:
                    self._device_type = sensor_name_override

                self._data_port, self._cmd_port, self._imu_port = self._bind_ports()

                ip_parts = self._computer_ip.split(".")
                ip_hex = str(hex(int(ip_parts[0]))).replace('0x', '').zfill(2)
                ip_hex += str(hex(int(ip_parts[1]))).replace('0x', '').zfill(2)
                ip_hex += str(hex(int(ip_parts[2]))).replace('0x', '').zfill(2)
                ip_hex += str(hex(int(ip_parts[3]))).replace('0x', '').zfill(2)
                data_hex_all = str(hex(int(self._data_port))).replace('0x', '').zfill(4)
                data_hex = data_hex_all[2:] + data_hex_all[:-2]
                cmd_hex_all = str(hex(int(self._cmd_port))).replace('0x', '').zfill(4)
                cmd_hex = cmd_hex_all[2:] + cmd_hex_all[:-2]
                imu_hex_all = str(hex(int(self._imu_port))).replace('0x', '').zfill(4)
                imu_hex = imu_hex_all[2:] + imu_hex_all[:-2]
                cmd_string = "AA011900000000DC580001" + ip_hex + data_hex + cmd_hex + imu_hex
                bin_string = bytes(cmd_string, encoding='utf-8')
                crc32checksum = helper.crc32from_str(bin_string)
                cmd_string += crc32checksum
                bin_string = bytes(cmd_string, encoding='utf-8')

                connect_request = bytes.fromhex(bin_string.decode('ascii'))
                self._cmd_socket.sendto(connect_request, (self._sensor_ip, 65000))

                # check for proper response from connection request
                if select.select([self._cmd_socket], [], [], 0.1)[0]:
                    bin_data, addr = self._cmd_socket.recvfrom(16)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "1":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                        if ret_code == 0:
                            self._is_connected = True
                            self._heartbeat = HeartbeatThread(1, self._cmd_socket, self._sensor_ip, 65000,
                                                              SDKDefs.CMD_HEARTBEAT, self._show_messages,
                                                              self._format_spaces)
                            time.sleep(0.15)
                            self._query()
                            self.msg.print("Connected to the Livox " + self._device_type + " at IP: " +
                                           self._sensor_ip + " (ID: " + str(self._ip_range_code) + ")")
                        else:
                            self.msg.print("FAILED to connect to the Livox " + self._device_type + " at IP: " +
                                           self._sensor_ip)
                    else:
                        self.msg.print("FAILED to connect to the Livox " + self._device_type + " at IP: " +
                                       self._sensor_ip)
            else:
                self.msg.print("Invalid connection parameter(s)")
        else:
            self.msg.print("Already connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip)

        return num_found

    def auto_connect(self, manual_computer_ip=""):

        num_found = 0

        if not manual_computer_ip:
            self._auto_computer_ip()
        else:
            self._computer_ip = manual_computer_ip

        if self._computer_ip:

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

            status_message = ""

            if len(unique_serial_nums) > 0:
                if self._show_messages:
                    status_message = "\nUsing computer IP address: " + self._computer_ip + "\n\n"
                for i in range(0, len(unique_serial_nums)):
                    ips_list = ip_groups[i].split(',')
                    ids_list = id_groups[i].split(',')
                    ips_mess = ""
                    last_ip_num = []
                    for j in range(len(ips_list)):
                        last_ip_num.append(int(ips_list[j].split('.')[3]))
                    last_ip_num.sort()
                    for j in range(len(last_ip_num)):
                        for k in range(len(ips_list)):
                            if last_ip_num[j] == int(ips_list[k].split('.')[3]):
                                num_spaces = " "
                                if last_ip_num[j] < 100:
                                    num_spaces += " "
                                if last_ip_num[j] < 10:
                                    num_spaces += " "
                                ips_mess += str(ips_list[k]) + num_spaces + "(ID: " + str(
                                    ids_list[k]) + ")\n                 "
                                break
                    if unique_sensors[i] != "NA":
                        status_message += "   *** Discovered a Livox sensor ***\n"
                        status_message += "           Type: " + unique_sensors[i] + "\n"
                        status_message += "         Serial: " + unique_serial_nums[i] + "\n"
                        status_message += "          IP(s): " + ips_mess + "\n"

            if len(unique_sensors) > 0 and unique_sensors[0] != "NA":
                status_message = status_message[:-1]
                print(status_message)
                self.msg.print("Attempting to auto-connect to the Livox " + unique_sensors[0] + " with S/N: " +
                          unique_serial_nums[0])

                if unique_sensors[0] == "Mid-100":
                    sensor_ips = None
                    sensor_ids = None
                    for i in range(len(ip_groups)):
                        ind_ips = ip_groups[i].split(',')
                        ind_ids = id_groups[i].split(',')
                        for j in range(len(ind_ips)):
                            if lidar_sensor_ips[0] == ind_ips[j]:
                                sensor_ips = ind_ips
                                sensor_ids = ind_ids

                    sensor_m = OpenPyLivox(self._init_show_messages)
                    sensor_r = OpenPyLivox(self._init_show_messages)

                    for i in range(3):
                        if int(sensor_ids[i]) == 1:
                            self.connect(self._computer_ip, sensor_ips[i], 0, 0, 0, "Mid-100 (L)")
                            for j in range(3):
                                if int(sensor_ids[j]) == 2:
                                    sensor_m.connect(self._computer_ip, sensor_ips[j], 0, 0, 0, "Mid-100 (M)")
                                    self._mid100_sensors.append(sensor_m)
                                    for k in range(3):
                                        if int(sensor_ids[k]) == 3:
                                            num_found = sensor_r.connect(self._computer_ip, sensor_ips[k],
                                                                         0, 0, 0, "Mid-100 (R)")
                                            self._mid100_sensors.append(sensor_r)
                                            break
                                    break
                            break
                else:
                    self._show_messages = False
                    num_found = self.connect(self._computer_ip, lidar_sensor_ips[0], 0, 0, 0)
                    self._device_type = unique_sensors[0]
                    self.resetShowMessages()

                    self.msg.print("Connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip +
                                   " (ID: " + str(self._ip_range_code) + ")")

        else:
            self.msg.print("*** ERROR: Failed to auto determine the computer IP address ***")

        return num_found

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
                self.msg.print("*** Error trying to disconnect from the Livox " + self._device_type + " at IP: "
                               + self._sensor_ip)
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
                self.msg.print("*** Error trying to reboot from the Livox " + self._device_type + " at IP: " +
                               self._sensor_ip)
        else:
            self.msg.print("Not connected to the Livox " + self._device_type + " at IP: " + self._sensor_ip)

    def reboot(self):
        self._reboot()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._reboot()

    def _lidar_spin_up(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_LIDAR_START, "Lidar", 0)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar spin up request")
        if response == 1:
            self.msg.prefix_print("     FAILED to spin up the lidar")
        elif response == 2:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     lidar is spinning up, please wait...")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect lidar spin up response")
        else:
            print(response)

    def lidarSpinUp(self):
        self._lidar_spin_up()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._lidar_spin_up()

        while True:
            time.sleep(0.1)
            states = [self._heartbeat.work_state]

            for i in range(len(self._mid100_sensors)):
                states.append(self._mid100_sensors[i]._heartbeat.work_state)

            stopper = False
            for i in range(len(states)):
                if states[i] == 1:
                    stopper = True
                else:
                    stopper = False

            if stopper:
                self.msg.prefix_print("     lidar is ready")
                for i in range(len(self._mid100_sensors)):
                    if self._mid100_sensors[i]._showMessages:
                        print("   " + self._mid100_sensors[i]._sensor_ip + self._mid100_sensors[i]._format_spaces +
                              "   -->     lidar is ready")
                time.sleep(0.1)
                break

    def _lidar_spin_down(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_LIDAR_POWERSAVE, "Lidar", 0)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar spin down request")
        if response == 1:
            self.msg.prefix_print("     FAILED to spin down the lidar")
        if response == 0:
            pass
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect lidar spin down response")

    def lidarSpinDown(self):
        self._lidar_spin_down()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._lidar_spin_down()

    def _lidar_stand_by(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_LIDAR_START, "Lidar", 0)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent lidar stand-by request")
        if response == 1:
            self.msg.prefix_print("     FAILED to set lidar to stand-by")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect lidar stand-by response")

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
                self._cmd_socket.sendto(SDKDefs.CMD_DATA_START, (self._sensor_ip, 65000))
                self.msg.print("   " + self._sensor_ip + self._format_spaces +
                               "   <--     sent start data stream request")

                # check for proper response from data start request
                if select.select([self._cmd_socket], [], [], 0.1)[0]:
                    binData, addr = self._cmd_socket.recvfrom(16)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "4":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                        if ret_code == 1:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                           "   -->     FAILED to start data stream")
                            if self._capture_stream is not None:
                                self._capture_stream.stop()
                            time.sleep(0.1)
                            self._is_data = False
                        else:
                            self._is_data = True
                    else:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                       "   -->     incorrect start data stream response")
            else:
                self.msg.prefix_print("     data stream already started")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def _data_start_rt(self):
        if not self._is_data:
            self._capture_stream = DataCaptureThread(self._sensor_ip, self._data_socket, None, "", 1, 0, 0, 0,
                                                     self._show_messages, self._format_spaces, self._device_type)
            time.sleep(0.12)

            response = self.send_command_receive_ack(SDKDefs.CMD_DATA_START, "General", 4)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent start data stream request")
            if response == 1:
                self.msg.prefix_print("     FAILED to start data stream")
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                time.sleep(0.1)
                self._is_data = False
            elif response == 0:
                self._is_data = True
            elif response == -1:
                self.msg.print("   " + self._sensor_ip + self._format_spaces +
                               "   -->     incorrect start data stream response")
        else:
            self.msg.prefix_print("     data stream already started")

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

            response = self.send_command_receive_ack(SDKDefs.CMD_DATA_START, "General", 4)
            if response == 0:
                self._is_data = True
            elif response == 1:
                self.msg.prefix_print("     FAILED to start data stream")
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                time.sleep(0.1)
                self._is_data = False
            elif response == -1:
                self.msg.print("   " + self._sensor_ip + self._format_spaces +
                               "   -->     incorrect start data stream response")

        else:
            self.msg.prefix_print("     data stream already started")

    def dataStart_RT_B(self):
        self._data_start_rt_b()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._data_start_rt_b()

    def _data_stop(self):
        if not self._is_data:
            self.msg.prefix_print("     data stream already stopped")
            return
        response = self.send_command_receive_ack(SDKDefs.CMD_DATA_STOP, "General", 4)
        self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent stop data stream request")
        if response == 1:
            self.msg.prefix_print("     FAILED to stop data stream")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect stop data stream response")

    def dataStop(self):
        self._data_stop()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._data_stop()

    def setDynamicIP(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_DYNAMIC_IP, "General", 8)
        if response == 0:
            self.msg.print("Changed IP from " + self._sensor_ip + " to dynamic IP (DHCP assigned)")
            self.disconnect()
            self.msg.print("\n********** PROGRAM ENDED - MUST REBOOT SENSOR **********\n")
            sys.exit(4)
        else:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to change to dynamic IP (DHCP assigned)")

    def setStaticIP(self, ip_address):

        if self._is_connected:
            ip_range = ""
            if self._ip_range_code == 1:
                ip_range = "192.168.1.11 to .80"
            elif self._ip_range_code == 2:
                ip_range = "192.168.1.81 to .150"
            elif self._ip_range_code == 3:
                ip_range = "192.168.1.151 to .220"
            ip_address = self._check_ip(ip_address)
            if ip_address:
                ip_parts = ip_address.split(".")
                ip_hex_1 = str(hex(int(ip_parts[0]))).replace('0x', '').zfill(2)
                ip_hex_2 = str(hex(int(ip_parts[1]))).replace('0x', '').zfill(2)
                ip_hex_3 = str(hex(int(ip_parts[2]))).replace('0x', '').zfill(2)
                ip_hex_4 = str(hex(int(ip_parts[3]))).replace('0x', '').zfill(2)
                formatted_ip = ip_parts[0].strip() + "." + ip_parts[1].strip() + "." + ip_parts[2].strip() + "." + \
                               ip_parts[3].strip()
                cmd_string = "AA011400000000a824000801" + ip_hex_1 + ip_hex_2 + ip_hex_3 + ip_hex_4
                bin_string = bytes(cmd_string, encoding='utf-8')
                crc32checksum = helper.crc32from_str(bin_string)
                cmd_string += crc32checksum
                bin_string = bytes(cmd_string, encoding='utf-8')

                static_ip_request = bytes.fromhex((bin_string).decode('ascii'))
                self._wait_for_idle()
                self._cmd_socket.sendto(static_ip_request, (self._sensor_ip, 65000))

                # check for proper response from static IP request
                if select.select([self._cmd_socket], [], [], 0.1)[0]:
                    bin_data, addr = self._cmd_socket.recvfrom(16)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "8":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')

                        if ret_code == 0:
                            self.msg.print("Changed IP from " + self._sensor_ip + " to a static IP of " + formatted_ip)
                            self.disconnect()

                            self.msg.print("\n********** PROGRAM ENDED - MUST REBOOT SENSOR **********\n")
                            sys.exit(5)
                        else:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                           "   -->     FAILED to change static IP (must be " + ip_range + ")")
                    else:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                       "   -->     FAILED to change static IP (must be " + ip_range + ")")
            else:
                self.msg.print("   " + self._sensor_ip + self._format_spaces +
                               "   -->     FAILED to change static IP (must be " + ip_range + ")")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def _set_cartesian_cs(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_CARTESIAN_CS, "General", 5)
        self.msg.print(
            "   " + self._sensor_ip + self._format_spaces + "   <--     sent change to Cartesian coordinates request")
        if response == 0:
            self._coord_system = 0
        elif response == 1:
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set Cartesian coordinate output")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect change coordinate system response (Cartesian)")

    def setCartesianCS(self):
        self._set_cartesian_cs()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_cartesian_cs()

    def _set_spherical_cs(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_SPHERICAL_CS, "General", 5)
        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                       "   <--     sent change to Spherical coordinates request")
        if response == 0:
            self._coord_system = 1
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to set Spherical coordinate output")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect change coordinate system response (Spherical)")

    def setSphericalCS(self):
        self._set_spherical_cs()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_spherical_cs()

    def readExtrinsic(self):
        response, data = self.send_command_receive_data(SDKDefs.CMD_READ_EXTRINSIC, "Lidar", 2, 40)
        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                       "   <--     sent read extrinsic parameters request")
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
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to read extrinsic parameters")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect read extrinsics response")
        elif response == -2:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def setExtrinsicToZero(self):
        response = self.send_command_receive_ack(SDKDefs.CMD_WRITE_ZERO_EO, "Lidar", 1)
        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                       "   <--     sent set extrinsic parameters to zero request")
        if response == 0:
            self.readExtrinsic()
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to set extrinsic parameters to zero")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect set extrinsics to zero response")

    def setExtrinsicTo(self, x, y, z, roll, pitch, yaw):
        hex_string_to_add_to_cmd_string = self.turn_values_into_single_hex_string([float(roll),
                                                                                   float(pitch),
                                                                                   float(yaw),
                                                                                   int(np.floor(x * 1000.)),
                                                                                   int(np.floor(y * 1000.)),
                                                                                   int(np.floor(z * 1000.))],
                                                                                  ['<f', '<f', '<f', '<i', '<i', '<i'])

        cmd_string = "AA012700000000b5ed0101" + hex_string_to_add_to_cmd_string
        bin_string = bytes(cmd_string, encoding='utf-8')
        crc32checksum = helper.crc32from_str(bin_string)
        cmd_string += crc32checksum
        bin_string = bytes(cmd_string, encoding='utf-8')
        set_ext_values = bytes.fromhex((bin_string).decode('ascii'))

        response = self.send_command_receive_ack(set_ext_values, "Lidar", 1)
        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                       "   <--     sent set extrinsic parameters request")
        if response == 0:
            self.readExtrinsic()
        elif response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to set extrinsic parameters")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect set extrinsic parameters response")

    def _update_utc(self, year, month, day, hour, micro_sec):

        if self._is_connected:

            year_i = int(year) - 2000
            if year_i < 0 or year_i > 255:
                year_i = 0

            month_i = int(month)
            if month_i < 1 or month_i > 12:
                month_i = 1

            day_i = int(day)
            if day_i < 1 or day_i > 31:
                day_i = 1

            hour_i = int(hour)
            if hour_i < 0 or hour_i > 23:
                hour_i = 0

            sec_i = int(micro_sec)
            if sec_i < 0 or sec_i > int(60 * 60 * 1000000):
                sec_i = 0

            year_b = str(binascii.hexlify(struct.pack('<B', year_i)))[2:-1]
            month_b = str(binascii.hexlify(struct.pack('<B', month_i)))[2:-1]
            day_b = str(binascii.hexlify(struct.pack('<B', day_i)))[2:-1]
            hour_b = str(binascii.hexlify(struct.pack('<B', hour_i)))[2:-1]
            sec_b = str(binascii.hexlify(struct.pack('<I', sec_i)))[2:-1]

            # test case Sept 10, 2020 at 17:15 UTC  -->  AA0117000000006439010A14090A1100E9A435D0337994
            cmd_string = "AA0117000000006439010A" + year_b + month_b + day_b + hour_b + sec_b
            bin_string = bytes(cmd_string, encoding='utf-8')
            crc32checksum = helper.crc32from_str(bin_string)
            cmd_string += crc32checksum
            bin_string = bytes(cmd_string, encoding='utf-8')
            set_utc_values = bytes.fromhex((bin_string).decode('ascii'))

            self._wait_for_idle()
            self._cmd_socket.sendto(set_utc_values, (self._sensor_ip, 65000))
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent update UTC request")

            # check for proper response from update UTC request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                bin_data, addr = self._cmd_socket.recvfrom(16)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)

                if ack == "ACK (response)" and cmd_set == "Lidar" and cmd_id == "10":
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    if ret_code == 1:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                       "   -->     FAILED to update UTC values")
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                   "   -->     incorrect update UTC values response")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def updateUTC(self, year, month, day, hour, micro_sec):
        self._update_utc(year, month, day, hour, micro_sec)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._update_utc(year, month, day, hour, micro_sec)

    def _set_rain_fog_suppression(self, on_off: bool):
        if on_off:
            response = self.send_command_receive_ack(SDKDefs.CMD_RAIN_FOG_ON, "Lidar", 3)
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     sent turn on rain/fog suppression request")
        else:
            response = self.send_command_receive_ack(SDKDefs.CMD_RAIN_FOG_OFF, "Lidar", 3)
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     sent turn off rain/fog suppression request")

        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to set rain/fog suppression value")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect set rain/fog suppression response")

    def setRainFogSuppression(self, on_off):
        self._set_rain_fog_suppression(on_off)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_rain_fog_suppression(on_off)

    def _set_fan(self, on_off):
        if on_off:
            response = self.send_command_receive_ack(SDKDefs.CMD_FAN_ON, "Lidar", 4)
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   <--     sent turn on fan request")
        else:
            response = self.send_command_receive_ack(SDKDefs.CMD_FAN_OFF, "Lidar", 4)
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   <--     sent turn off fan request")

        if response == 1:
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   -->     FAILED to set fan value")
        elif response == -1:
            self.msg.print(
                "   " + self._sensor_ip + self._format_spaces + "   -->     incorrect set fan response")

    def setFan(self, on_off):
        self._set_fan(on_off)
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._set_fan(on_off)

    def _get_fan(self):

        if self._is_connected:
            self._wait_for_idle()

            self._cmd_socket.sendto(SDKDefs.CMD_GET_FAN, (self._sensor_ip, 65000))
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent get fan state request")

            # check for proper response from get fan request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                binData, addr = self._cmd_socket.recvfrom(17)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, binData)

                if ack == "ACK (response)" and cmd_set == "Lidar" and cmd_id == "5":
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    if ret_code == 1:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                       "   -->     FAILED to get fan state value")
                    elif ret_code == 0:
                        value = struct.unpack('<B', binData[12:13])[0]
                        print("   " + self._sensor_ip + self._format_spaces + "   -->     fan state: " + str(value))
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                   "   -->     incorrect get fan state response")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    def getFan(self):
        self._get_fan()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._get_fan()

    def setLidarReturnMode(self, Mode_ID):
        if Mode_ID == 0:
            response = self.send_command_receive_ack(SDKDefs.CMD_LIDAR_SINGLE_1ST, "Lidar", 6)
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     sent single first return lidar mode request")
        elif Mode_ID == 1:
            response = self.send_command_receive_ack(SDKDefs.CMD_LIDAR_SINGLE_STRONGEST, "Lidar", 6)
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     sent single strongest return lidar mode request")
        elif Mode_ID == 2:
            response = self.send_command_receive_ack(SDKDefs.CMD_LIDAR_DUAL, "Lidar", 6)
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     sent dual return lidar mode request")
        else:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     Invalid mode_id for setting lidar return mode")
            return

        if response == 1:
            self.msg.prefix_print("     FAILED to set lidar mode value")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect set lidar mode response")

    def setIMUdataPush(self, on_off: bool):
        if on_off:
            response = self.send_command_receive_ack(SDKDefs.CMD_IMU_DATA_ON, "Lidar", 8)
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   <--     sent start IMU data push request")
        else:
            response = self.send_command_receive_ack(SDKDefs.CMD_IMU_DATA_OFF, "Lidar", 8)
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent stop IMU data push request")

        if response == 1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     FAILED to set IMU data push value")
        elif response == -1:
            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                           "   -->     incorrect set IMU data push response")

    def getIMUdataPush(self):

        if self._is_connected:
            self._wait_for_idle()

            self._cmd_socket.sendto(SDKDefs.CMD_GET_IMU, (self._sensor_ip, 65000))
            self.msg.print("   " + self._sensor_ip + self._format_spaces + "   <--     sent get IMU push state request")

            # check for proper response from get IMU request
            if select.select([self._cmd_socket], [], [], 0.1)[0]:
                bin_data, addr = self._cmd_socket.recvfrom(17)
                _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)

                if ack == "ACK (response)" and cmd_set == "Lidar" and cmd_id == "9":
                    ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                    if ret_code == 1:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                       "   -->     FAILED to get IMU push state value")
                    elif ret_code == 0:
                        value = struct.unpack('<B', bin_data[12:13])[0]
                        print("   " + self._sensor_ip + self._format_spaces +
                              "   -->     IMU push state: " + str(value))
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                   "   -->     incorrect get IMU push state response")
        else:
            self.msg.print("Not connected to Livox sensor at IP: " + self._sensor_ip)

    @deprecated(version='1.0.2', reason="You should use saveDataToFile instead")
    def saveDataToCSV(self, file_path_and_name, secs_to_wait, duration):

        if self._is_connected:
            if self._is_data:
                if self._firmware != "UNKNOWN":
                    try:
                        firmware_type = SDKDefs.SPECIAL_FIRMWARE_TYPE_DICT[self._firmware]
                    except:
                        firmware_type = 1

                    if duration < 0:
                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                       "   -->     * ISSUE: saving data, negative duration")
                    else:
                        # max duration = 4 years - 1 sec
                        if duration >= 126230400:
                            self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                           "   -->     * ISSUE: saving data, duration too big")
                        else:

                            if secs_to_wait < 0:
                                self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                               "   -->     * ISSUE: saving data, negative time to wait")
                            else:
                                # max time to wait = 15 mins
                                if secs_to_wait > 900:
                                    self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                                   "   -->     * ISSUE: saving data, time to wait too big")
                                else:

                                    if file_path_and_name == "":
                                        self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                                       "   -->     * ISSUE: saving data, file path and name missing")
                                    else:

                                        if file_path_and_name[-4:].upper() != ".CSV":
                                            file_path_and_name += ".csv"

                                        self._is_writing = True
                                        self._capture_stream.filePathAndName = file_path_and_name
                                        self._capture_stream.secsToWait = secs_to_wait
                                        self._capture_stream.duration = duration
                                        self._capture_stream.firmwareType = firmware_type
                                        self._capture_stream._showMessages = self._show_messages
                                        time.sleep(0.1)
                                        self._capture_stream.isCapturing = True
                else:
                    self.msg.print("   " + self._sensor_ip + self._format_spaces +
                                   "   -->     unknown firmware version")
            else:
                self.msg.print("   " + self._sensor_ip + self._format_spaces +
                               "   -->     WARNING: data stream not started, no CSV file created")

    @deprecated(version='1.0.2', reason="You should use closeFile instead")
    def closeCSV(self):
        if self._is_connected:
            if self._is_writing:
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                self._is_writing = False

    def _save_data_to_file(self, file_path_and_name, secs_to_wait, duration):
        if self._is_connected:
            if self._is_data:
                if self._firmware != "UNKNOWN":
                    if self._firmware in SDKDefs.SPECIAL_FIRMWARE_TYPE_DICT.keys():
                        firmware_type = SDKDefs.SPECIAL_FIRMWARE_TYPE_DICT[self._firmware]
                    else:
                        firmware_type = 1

                    if duration < 0:
                        self.msg.prefix_print("     * ISSUE: saving data, negative duration")
                    elif duration >= 126230400:  # max duration = 4 years - 1 sec
                        self.msg.prefix_print("     * ISSUE: saving data, duration too big")
                    elif secs_to_wait < 0:
                        self.msg.prefix_print("     * ISSUE: saving data, negative time to wait")
                    elif secs_to_wait > 15 * 60:  # max time to wait = 15 mins
                        self.msg.prefix_print("     * ISSUE: saving data, time to wait too big")
                    elif file_path_and_name == "":
                        self.msg.prefix_print("     * ISSUE: saving data, file path and name missing")
                    else:
                        self._is_writing = True
                        self._capture_stream.file_path_and_name = file_path_and_name
                        self._capture_stream.secs_to_wait = secs_to_wait
                        self._capture_stream.duration = duration
                        self._capture_stream.firmware_type = firmware_type
                        self._capture_stream._show_messages = self._show_messages
                        time.sleep(0.1)
                        self._capture_stream.is_capturing = True
                else:
                    self.msg.prefix_print("     unknown firmware version")
            else:
                self.msg.prefix_print("     WARNING: data stream not started, no data file created")

    def saveDataToFile(self, file_path_and_name, secs_to_wait, duration):
        path_file = Path(file_path_and_name)
        filename = path_file.stem
        extension = path_file.suffix
        self._save_data_to_file(file_path_and_name, secs_to_wait, duration)
        for i in range(len(self._mid100_sensors)):
            new_file = ""
            if i == 0:
                new_file = filename + "_M" + extension
            elif i == 1:
                new_file = filename + "_R" + extension

            self._mid100_sensors[i]._save_data_to_file(new_file, secs_to_wait, duration)

    def _close_file(self):
        if self._is_connected:
            if self._is_writing:
                if self._capture_stream is not None:
                    self._capture_stream.stop()
                self._is_writing = False

    def closeFile(self):
        self._close_file()
        for i in range(len(self._mid100_sensors)):
            self._mid100_sensors[i]._close_file()

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
        sensor_ips = []
        data_ports = []
        cmd_ports = []
        imu_ports = []

        params = self._connectionParameters()
        sensor_ips.append(params[1])
        data_ports.append(params[2])
        cmd_ports.append(params[3])
        imu_ports.append(params[4])

        for i in range(len(self._mid100_sensors)):
            params = self._mid100_sensors[i]._connectionParameters()
            sensor_ips.append(params[1])
            data_ports.append(params[2])
            cmd_ports.append(params[3])
            imu_ports.append(params[4])

        if self._show_messages:
            print("      Computer IP Address:    " + params[0])
            print("      Sensor IP Address(es):  " + str(sensor_ips))
            print("      Data Port Number(s):    " + str(data_ports))
            print("      Command Port Number(s): " + str(cmd_ports))
            if self._device_type == "Horizon" or self._device_type == "Tele-15":
                print("      IMU Port Number(s):     " + str(imu_ports))

        return [params[0], sensor_ips, data_ports, cmd_ports, imu_ports]

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
            self.msg.prefix_print(f"     Serial # {self._serial}")

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

    def _done_capturing(self):
        # small sleep to ensure this command isn't continuously called if in a while True loop
        time.sleep(0.01)
        if self._capture_stream is not None:
            if self._capture_stream.duration != 126230400:
                return not self._capture_stream.started
            else:
                return True
        else:
            return True

    def doneCapturing(self):
        stop = [self._done_capturing()]
        for i in range(len(self._mid100_sensors)):
            stop.append(self._mid100_sensors[i]._done_capturing())

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


def _convert_bin2_csv(file_path_and_name, delete_bin):
    bin_file = None
    csv_file = None
    imu_file = None
    imu_csv_file = None

    try:
        data_class = 0
        if os.path.exists(file_path_and_name) and os.path.isfile(file_path_and_name):
            bin_size = Path(file_path_and_name).stat().st_size - 15
            bin_file = open(file_path_and_name, "rb")

            check_message = (bin_file.read(11)).decode('UTF-8')
            if check_message == "OPENPYLIVOX":
                with open(file_path_and_name + ".csv", "w", 1) as csv_file:
                    firmware_type = struct.unpack('<h', bin_file.read(2))[0]
                    data_type = struct.unpack('<h', bin_file.read(2))[0]
                    divisor = 1

                    if 1 <= firmware_type <= 3:
                        if 0 <= data_type <= 5:
                            print("CONVERTING OPL BINARY DATA, PLEASE WAIT...")
                            if firmware_type == 1 and data_type == 0:
                                csv_file.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                                data_class = 1
                                divisor = 21
                            elif firmware_type == 1 and data_type == 1:
                                csv_file.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")
                                data_class = 2
                                divisor = 17
                            elif firmware_type > 1 and data_type == 0:
                                csv_file.write("//X,Y,Z,Inten-sity,Time,ReturnNum\n")
                                data_class = 3
                                divisor = 22
                            elif firmware_type > 1 and data_type == 1:
                                csv_file.write("//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum\n")
                                data_class = 4
                                divisor = 18
                            elif firmware_type == 1 and data_type == 2:
                                csv_file.write("//X,Y,Z,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                data_class = 5
                                divisor = 22
                            elif firmware_type == 1 and data_type == 3:
                                csv_file.write(
                                    "//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                data_class = 6
                                divisor = 18
                            elif firmware_type == 1 and data_type == 4:
                                csv_file.write("//X,Y,Z,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                data_class = 7
                                divisor = 36
                            elif firmware_type == 1 and data_type == 5:
                                csv_file.write(
                                    "//Distance,Zenith,Azimuth,Inten-sity,Time,ReturnNum,ReturnType,sConf,iConf\n")
                                data_class = 8
                                divisor = 24

                            num_recs = int(bin_size / divisor)
                            pbari = tqdm(total=num_recs, unit=" pts", desc="   ")

                            while True:
                                try:
                                    # Mid-40/100 Cartesian single return
                                    if data_class == 1:
                                        coord1 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord3 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        intensity = int.from_bytes(bin_file.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
                                        csv_file.write("{0:.3f}".format(coord1) + "," + "{0:.3f}".format(
                                            coord2) + "," + "{0:.3f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + ",1\n")

                                    # Mid-40/100 Spherical single return
                                    elif data_class == 2:
                                        coord1 = float(struct.unpack('<I', bin_file.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        intensity = int.from_bytes(bin_file.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
                                        csv_file.write("{0:.3f}".format(coord1) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + ",1\n")

                                    # Mid-40/100 Cartesian multiple return
                                    elif data_class == 3:
                                        coord1 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord3 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        intensity = int.from_bytes(bin_file.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
                                        return_num = (bin_file.read(1)).decode('UTF-8')
                                        csv_file.write("{0:.3f}".format(coord1) + "," + "{0:.3f}".format(
                                            coord2) + "," + "{0:.3f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," +
                                                       return_num + "\n")

                                    # Mid-40/100 Spherical multiple return
                                    elif data_class == 4:
                                        coord1 = float(struct.unpack('<I', bin_file.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        intensity = int.from_bytes(bin_file.read(1), byteorder='little')
                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
                                        return_num = (bin_file.read(1)).decode('UTF-8')
                                        csv_file.write("{0:.3f}".format(coord1) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(timestamp_sec) + "," +
                                                       return_num + "\n")

                                    # Horizon/Tele-15 Cartesian single return (SDK Data Type 2)
                                    elif data_class == 5:
                                        coord1 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord3 = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        intensity = struct.unpack('<B', bin_file.read(1))[0]
                                        tag_bits = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[
                                                   2:].zfill(8)
                                        spatial_conf = str(int(tag_bits[0:2], 2))
                                        intensity_conf = str(int(tag_bits[2:4], 2))
                                        return_type = str(int(tag_bits[4:6], 2))
                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
                                        csv_file.write("{0:.3f}".format(coord1) + "," + "{0:.3f}".format(
                                            coord2) + "," + "{0:.3f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + return_type + ","
                                                       + spatial_conf + "," + intensity_conf + "\n")

                                    # Horizon/Tele-15 Spherical single return (SDK Data Type 3)
                                    elif data_class == 6:
                                        coord1 = float(struct.unpack('<I', bin_file.read(4))[0]) / 1000.0
                                        coord2 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        intensity = struct.unpack('<B', bin_file.read(1))[0]
                                        tag_bits = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[
                                                   2:].zfill(8)
                                        spatial_conf = str(int(tag_bits[0:2], 2))
                                        intensity_conf = str(int(tag_bits[2:4], 2))
                                        return_type = str(int(tag_bits[4:6], 2))
                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
                                        csv_file.write("{0:.3f}".format(coord1) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + return_type + ","
                                                       + spatial_conf + "," + intensity_conf + "\n")

                                    # Horizon/Tele-15 Cartesian dual return (SDK Data Type 4)
                                    elif data_class == 7:
                                        coord1a = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord2a = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord3a = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        intensity_a = struct.unpack('<B', bin_file.read(1))[0]
                                        tag_bits_a = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_conf_a = str(int(tag_bits_a[0:2], 2))
                                        intensity_conf_a = str(int(tag_bits_a[2:4], 2))
                                        return_type_a = str(int(tag_bits_a[4:6], 2))

                                        coord1b = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord2b = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        coord3b = float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0
                                        intensity_b = struct.unpack('<B', bin_file.read(1))[0]
                                        tag_bits_b = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_conf_b = str(int(tag_bits_b[0:2], 2))
                                        intensity_conf_b = str(int(tag_bits_b[2:4], 2))
                                        return_type_b = str(int(tag_bits_b[4:6], 2))

                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])

                                        csv_file.write("{0:.3f}".format(coord1a) + "," + "{0:.3f}".format(
                                            coord2a) + "," + "{0:.3f}".format(coord3a) + "," + str(
                                            intensity_a) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + return_type_a + ","
                                                       + spatial_conf_a + "," + intensity_conf_a + "\n")

                                        csv_file.write("{0:.3f}".format(coord1b) + "," + "{0:.3f}".format(
                                            coord2b) + "," + "{0:.3f}".format(coord3b) + "," + str(
                                            intensity_b) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",2," + return_type_b + ","
                                                       + spatial_conf_b + "," + intensity_conf_b + "\n")

                                    # Horizon/Tele-15 Spherical dual return (SDK Data Type 5)
                                    elif data_class == 8:
                                        coord2 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        coord3 = float(struct.unpack('<H', bin_file.read(2))[0]) / 100.0
                                        coord1a = float(struct.unpack('<I', bin_file.read(4))[0]) / 1000.0
                                        intensity_a = struct.unpack('<B', bin_file.read(1))[0]
                                        tag_bits_a = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_conf_a = str(int(tag_bits_a[0:2], 2))
                                        intensity_conf_a = str(int(tag_bits_a[2:4], 2))
                                        return_type_a = str(int(tag_bits_a[4:6], 2))

                                        coord1b = float(struct.unpack('<I', bin_file.read(4))[0]) / 1000.0
                                        intensity_b = struct.unpack('<B', bin_file.read(1))[0]
                                        tag_bits_b = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[
                                                    2:].zfill(8)
                                        spatial_conf_b = str(int(tag_bits_b[0:2], 2))
                                        intensity_conf_b = str(int(tag_bits_b[2:4], 2))
                                        return_type_b = str(int(tag_bits_b[4:6], 2))

                                        timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])

                                        csv_file.write("{0:.3f}".format(coord1a) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity_a) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",1," + return_type_a + ","
                                                       + spatial_conf_a + "," + intensity_conf_a + "\n")

                                        csv_file.write("{0:.3f}".format(coord1b) + "," + "{0:.2f}".format(
                                            coord2) + "," + "{0:.2f}".format(coord3) + "," + str(
                                            intensity_b) + "," + "{0:.6f}".format(
                                            timestamp_sec) + ",2," + return_type_b + ","
                                                       + spatial_conf_b + "," + intensity_conf_b + "\n")

                                    pbari.update(1)

                                except:
                                    break

                            pbari.close()
                            bin_file.close()
                            print(
                                "   - Point data was converted successfully to CSV, see file: " +
                                file_path_and_name + ".csv")
                            if delete_bin:
                                os.remove(file_path_and_name)
                                print("     * OPL point data binary file has been deleted")
                            print()
                            time.sleep(0.5)
                        else:
                            print("*** ERROR: The OPL point data binary file reported a wrong data type ***")
                            bin_file.close()
                    else:
                        print("*** ERROR: The OPL point data binary file reported a wrong firmware type ***")
                        bin_file.close()

                # check for and convert IMU BIN data (if it exists)
                path_file = Path(file_path_and_name)
                filename = path_file.stem
                extension = path_file.suffix
                imu_file = filename + "_IMU" + extension

                if os.path.exists(imu_file) and os.path.isfile(imu_file):
                    bin_size2 = Path(imu_file).stat().st_size - 15
                    num_recs = int(bin_size2 / 32)
                    bin_file2 = open(imu_file, "rb")

                    check_message = (bin_file2.read(15)).decode('UTF-8')
                    if check_message == "OPENPYLIVOX_IMU":
                        with open(imu_file + ".csv", "w", 1) as csvFile2:
                            csvFile2.write("//gyro_x,gyro_y,gyro_z,acc_x,acc_y,acc_z,time\n")
                            pbari2 = tqdm(total=num_recs, unit=" records", desc="   ")
                            while True:
                                try:
                                    gyro_x = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    gyro_y = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    gyro_z = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    acc_x = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    acc_y = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    acc_z = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    timestamp_sec = "{0:.6f}".format(struct.unpack('<d', bin_file2.read(8))[0])

                                    csvFile2.write(gyro_x + "," + gyro_y + "," + gyro_z + "," + acc_x + "," + acc_y +
                                                   "," + acc_z + "," + timestamp_sec + "\n")

                                    pbari2.update(1)

                                except:
                                    break

                            pbari2.close()
                            bin_file2.close()
                            print("   - IMU data was converted successfully to CSV, see file: " + imu_file + ".csv")
                            if delete_bin:
                                os.remove(imu_file)
                                print("     * OPL IMU data binary file has been deleted")
                    else:
                        print("*** ERROR: The file was not recognized as an OpenPyLivox binary IMU data file ***")
                        bin_file2.close()
            else:
                print("*** ERROR: The file was not recognized as an OpenPyLivox binary point data file ***")
                bin_file.close()
    except:
        bin_file.close()
        print("*** ERROR: An unknown error occurred while converting OPL binary data ***")


def convertBin2CSV(file_path_and_name, deleteBin=False):
    print()
    path_file = Path(file_path_and_name)
    filename = path_file.stem
    extension = path_file.suffix

    if os.path.isfile(file_path_and_name):
        _convert_bin2_csv(file_path_and_name, deleteBin)

    if os.path.isfile(filename + "_M" + extension):
        _convert_bin2_csv(filename + "_M" + extension, deleteBin)

    if os.path.isfile(filename + "_R" + extension):
        _convert_bin2_csv(filename + "_R" + extension, deleteBin)


def _convert_bin2_las(file_path_and_name, delete_bin):
    bin_file = None
    csv_file = None
    imu_file = None
    imu_csv_file = None

    try:
        data_class = 0
        if os.path.exists(file_path_and_name) and os.path.isfile(file_path_and_name):
            bin_size = Path(file_path_and_name).stat().st_size - 15
            bin_file = open(file_path_and_name, "rb")

            check_message = (bin_file.read(11)).decode('UTF-8')
            if check_message == "OPENPYLIVOX":
                firmware_type = struct.unpack('<h', bin_file.read(2))[0]
                data_type = struct.unpack('<h', bin_file.read(2))[0]
                divisor = 1

                if 1 <= firmware_type <= 3:
                    # LAS file creation only works with Cartesian data types (decided not to convert spherical obs.)
                    if data_type == 0 or data_type == 2 or data_type == 4:
                        print("CONVERTING OPL BINARY DATA, PLEASE WAIT...")

                        coord1s = []
                        coord2s = []
                        coord3s = []
                        intensity = []
                        times = []
                        return_nums = []

                        if firmware_type == 1 and data_type == 0:
                            data_class = 1
                            divisor = 21
                        elif firmware_type > 1 and data_type == 0:
                            data_class = 3
                            divisor = 22
                        elif firmware_type == 1 and data_type == 2:
                            data_class = 5
                            divisor = 22
                        elif firmware_type == 1 and data_type == 4:
                            data_class = 7
                            divisor = 36

                        num_recs = int(bin_size / divisor)
                        pbari = tqdm(total=num_recs, unit=" pts", desc="   ")

                        while True:
                            try:
                                # Mid-40/100 Cartesian single return
                                if data_class == 1:
                                    coord1s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    intensity.append(int.from_bytes(bin_file.read(1), byteorder='little'))
                                    times.append(float(struct.unpack('<d', bin_file.read(8))[0]))
                                    return_nums.append(1)

                                # Mid-40/100 Cartesian multiple return
                                elif data_class == 3:
                                    coord1s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    intensity.append(int.from_bytes(bin_file.read(1), byteorder='little'))
                                    times.append(float(struct.unpack('<d', bin_file.read(8))[0]))
                                    return_nums.append(int((bin_file.read(1)).decode('UTF-8')))

                                # Horizon/Tele-15 Cartesian single return (SDK Data Type 2)
                                elif data_class == 5:
                                    coord1s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    intensity.append(struct.unpack('<B', bin_file.read(1))[0])
                                    tag_bits = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[2:].zfill(
                                        8)
                                    times.append(float(struct.unpack('<d', bin_file.read(8))[0]))
                                    return_nums.append(1)

                                # Horizon/Tele-15 Cartesian dual return (SDK Data Type 4)
                                elif data_class == 7:
                                    coord1s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    intensity.append(struct.unpack('<B', bin_file.read(1))[0])
                                    tag_bits = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[2:].zfill(
                                        8)
                                    return_nums.append(1)

                                    coord1s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord2s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    coord3s.append(float(struct.unpack('<i', bin_file.read(4))[0]) / 1000.0)
                                    intensity.append(struct.unpack('<B', bin_file.read(1))[0])
                                    tag_bits = str(bin(int.from_bytes(bin_file.read(1), byteorder='little')))[2:].zfill(
                                        8)
                                    return_nums.append(2)

                                    timestamp_sec = float(struct.unpack('<d', bin_file.read(8))[0])
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
                        system_id = "OpenPyLivox"
                        software_id = "OpenPyLivox V1.1.0"

                        if len(system_id) < 32:
                            missing_length = 32 - len(system_id)
                            for i in range(0, missing_length):
                                system_id += " "

                        if len(software_id) < 32:
                            missing_length = 32 - len(software_id)
                            for i in range(0, missing_length):
                                software_id += " "

                        hdr.system_id = system_id
                        hdr.software_id = software_id

                        las_file = laspy.file.File(file_path_and_name + ".las", mode="w", header=hdr)

                        coord1s = np.asarray(coord1s, dtype=np.float32)
                        coord2s = np.asarray(coord2s, dtype=np.float32)
                        coord3s = np.asarray(coord3s, dtype=np.float32)

                        x_min = np.floor(np.min(coord1s))
                        y_min = np.floor(np.min(coord2s))
                        z_min = np.floor(np.min(coord3s))
                        las_file.header.offset = [x_min, y_min, z_min]

                        las_file.header.scale = [0.001, 0.001, 0.001]

                        las_file.x = coord1s
                        las_file.y = coord2s
                        las_file.z = coord3s
                        las_file.gps_time = np.asarray(times, dtype=np.float32)
                        las_file.intensity = np.asarray(intensity, dtype=np.int16)
                        las_file.return_num = np.asarray(return_nums, dtype=np.int8)

                        las_file.close()

                        pbari.close()
                        bin_file.close()
                        print(
                            "   - Point data was converted successfully to LAS, see file: " +
                            file_path_and_name + ".las")
                        if delete_bin:
                            os.remove(file_path_and_name)
                            print("     * OPL point data binary file has been deleted")
                        print()
                        time.sleep(0.5)
                    else:
                        print("*** ERROR: Only Cartesian point data can be converted to an LAS file ***")
                        bin_file.close()
                else:
                    print("*** ERROR: The OPL point data binary file reported a wrong firmware type ***")
                    bin_file.close()

                # check for and convert IMU BIN data (if it exists)
                path_file = Path(file_path_and_name)
                filename = path_file.stem
                extension = path_file.suffix
                imu_file = filename + "_IMU" + extension

                if os.path.exists(imu_file) and os.path.isfile(imu_file):
                    bin_size2 = Path(imu_file).stat().st_size - 15
                    num_recs = int(bin_size2 / 32)
                    bin_file2 = open(imu_file, "rb")

                    check_message = (bin_file2.read(15)).decode('UTF-8')
                    if check_message == "OPENPYLIVOX_IMU":
                        with open(imu_file + ".csv", "w", 1) as csvFile2:
                            csvFile2.write("//gyro_x,gyro_y,gyro_z,acc_x,acc_y,acc_z,time\n")
                            pbari2 = tqdm(total=num_recs, unit=" records", desc="   ")
                            while True:
                                try:
                                    gyro_x = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    gyro_y = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    gyro_z = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    acc_x = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    acc_y = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    acc_z = "{0:.6f}".format(struct.unpack('<f', bin_file2.read(4))[0])
                                    timestamp_sec = "{0:.6f}".format(struct.unpack('<d', bin_file2.read(8))[0])

                                    csvFile2.write(gyro_x + "," + gyro_y + "," + gyro_z + "," + acc_x + "," + acc_y +
                                                   "," + acc_z + "," + timestamp_sec + "\n")

                                    pbari2.update(1)

                                except:
                                    break

                            pbari2.close()
                            bin_file2.close()
                            print("   - IMU data was converted successfully to CSV, see file: " + imu_file + ".csv")
                            if delete_bin:
                                os.remove(imu_file)
                                print("     * OPL IMU data binary file has been deleted")
                    else:
                        print("*** ERROR: The file was not recognized as an OpenPyLivox binary IMU data file ***")
                        bin_file2.close()
            else:
                print("*** ERROR: The file was not recognized as an OpenPyLivox binary point data file ***")
                bin_file.close()
    except:
        bin_file.close()
        print("*** ERROR: An unknown error occurred while converting OPL binary data ***")


def convertBin2LAS(file_path_and_name, deleteBin=False):
    print()
    path_file = Path(file_path_and_name)
    filename = path_file.stem
    extension = path_file.suffix

    if os.path.isfile(file_path_and_name):
        _convert_bin2_las(file_path_and_name, deleteBin)

    if os.path.isfile(filename + "_M" + extension):
        _convert_bin2_las(filename + "_M" + extension, deleteBin)

    if os.path.isfile(filename + "_R" + extension):
        _convert_bin2_las(filename + "_R" + extension, deleteBin)
