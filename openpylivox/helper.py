import binascii
import struct

import crcmod

""" ################################################################
################################ CRC ###############################
################################################################ """


def crc16(data):
    return crcmod.mkCrcFun(0x11021, rev=True, initCrc=0x4C49)(data)


def crc16from_str(bin_string):
    crc_data_a = bytes.fromhex(bin_string.decode('ascii'))
    check_sum = crc16(crc_data_a)
    str_hex_check_sum = str(hex(check_sum))[2:]

    str_len = len(str_hex_check_sum)
    for i in range(str_len, 4):
        str_hex_check_sum = "0" + str_hex_check_sum

    byte1 = str_hex_check_sum[2:4]
    byte2 = str_hex_check_sum[0:2]

    return byte1 + byte2


def crc32(data):
    return crcmod.mkCrcFun(0x104C11DB7, rev=True, initCrc=0x564F580A, xorOut=0xFFFFFFFF)(data)


def crc32from_str(bin_string):
    crc_data_a = bytes.fromhex(bin_string.decode('ascii'))
    check_sum = crc32(crc_data_a)
    str_hex_check_sum = str(hex(check_sum))[2:]

    str_len = len(str_hex_check_sum)
    for i in range(str_len, 8):
        str_hex_check_sum = "0" + str_hex_check_sum

    byte1 = str_hex_check_sum[6:8]
    byte2 = str_hex_check_sum[4:6]
    byte3 = str_hex_check_sum[2:4]
    byte4 = str_hex_check_sum[0:2]

    return byte1 + byte2 + byte3 + byte4


""" ################################################################
################################ Int ###############################
################################################################ """


def bytes_to_int(bytes_int, byteorder='little'):
    return int.from_bytes(bytes_int, byteorder=byteorder)


def get_seconds_in_x_years(years):
    amount_of_leap_days = int(years / 4)
    seconds_per_day = 60 * 60 * 24
    # 4 Years of time in seconds
    return seconds_per_day * (365 * years + amount_of_leap_days)


""" ################################################################
########################### MSG OBJECT #############################
################################################################ """


class Msg:
    def __init__(self, show_message: bool, sensor_ip=None, format_spaces=None, default_arrow=None):
        self.show_message = show_message
        self.sensor_ip = sensor_ip
        self.format_spaces = format_spaces
        self.default_arrow = default_arrow

    def print(self, val):
        if self.show_message:
            print(val)

    def prefix_print(self, string, f_spaces=1, arrow=None):
        if not (self.sensor_ip and self.format_spaces):
            raise ValueError("Prefix print hasn't been initialised yet. Do so in the __init__.")
        if arrow is None and self.default_arrow is None:
            raise ValueError("Prefix default_arrow hasn't been defined.")
        elif arrow is None:
            arrow = self.default_arrow

        self.print(f"   {self.sensor_ip}{self.format_spaces * f_spaces}   {arrow}     {string}")

    def space_print(self, spaces, string):
        self.print(f"{' ' * spaces}{string}")


""" ################################################################
################################ ??? ###############################
################################################################ """


def _parse_resp(show_message, bin_data):
    data_bytes = []
    data_string = ""
    data_length = len(bin_data)
    for i in range(data_length):
        data_bytes.append(bin_data[i:i + 1])
        data_string += (binascii.hexlify(bin_data[i:i + 1])).decode("utf-8")

    crc16_data = b''
    for i in range(7):
        crc16_data += binascii.hexlify(data_bytes[i])

    crc16_data_a = bytes.fromhex(crc16_data.decode('ascii'))
    cmd_message, data_message, data_id, data = "", "", "", []
    data_is_valid = True

    frame_header_checksum_crc16 = int.from_bytes((data_bytes[7] + data_bytes[8]), byteorder='little')
    if frame_header_checksum_crc16 == crc16(crc16_data_a):

        crc32_data = b''
        for i in range(0, data_length - 4):
            crc32_data += binascii.hexlify(data_bytes[i])

        check_sum32_i = crc32(bytes.fromhex(crc32_data.decode('ascii')))

        frame_header_checksum_crc32 = int.from_bytes(
            (data_bytes[data_length - 4] + data_bytes[data_length - 3] +
             data_bytes[data_length - 2] + data_bytes[data_length - 1]), byteorder='little'
        )

        if frame_header_checksum_crc32 == check_sum32_i:

            frame_sof = int.from_bytes(data_bytes[0], byteorder='little')  # should be 170 = '\xAA'
            frame_version = int.from_bytes(data_bytes[1], byteorder='little')  # should be 1
            frame_length = int.from_bytes((data_bytes[2] + data_bytes[3]), byteorder='little')  # max value = 1400

            if frame_sof == 170 and frame_version == 1 and frame_length <= 1400:
                frame_cmd_type = int.from_bytes(data_bytes[4], byteorder='little')

                cmd_message = ""
                if frame_cmd_type == 0:
                    cmd_message = "CMD (request)"
                elif frame_cmd_type == 1:
                    cmd_message = "ACK (response)"
                elif frame_cmd_type == 2:
                    cmd_message = "MSG (message)"
                else:
                    data_is_valid = False

                frame_data_cmd_set = int.from_bytes(data_bytes[9], byteorder='little')

                data_message = ""
                if frame_data_cmd_set == 0:
                    data_message = "General"
                elif frame_data_cmd_set == 1:
                    data_message = "Lidar"
                elif frame_data_cmd_set == 2:
                    data_message = "Hub"
                else:
                    data_is_valid = False

                data_id = str(int.from_bytes(data_bytes[10], byteorder='little'))
                data = data_bytes[11:]
            else:
                data_is_valid = False
        else:
            data_is_valid = False
            if show_message:
                print("CRC32 Checksum Error")
    else:
        data_is_valid = False
        if show_message:
            print("CRC16 Checksum Error")

    return data_is_valid, cmd_message, data_message, data_id, data


def adjust_duration(firmware_type, duration):
    firmware_adjustments = {
        1: 0.001,
        2: 0.0005,
        3: 0.00055,
    }
    # duration adjustment (trying to get exactly 100,000 points / sec)
    if duration != 126230400:
        if firmware_type not in firmware_adjustments.keys():
            raise ValueError(f"Unknown firmware type: {firmware_type}")
        return duration + (firmware_adjustments.get(firmware_type) * (duration / 2.0))
    return duration


def get_timestamp(data_pc, timestamp_type):
    # nanosecond timestamp
    if timestamp_type in [0, 1, 4]:
        timestamp_sec = round(float(struct.unpack('<Q', data_pc[0:8])[0]) / 1000000000.0, 6)  # convert to seconds
    # UTC timestamp, microseconds past the hour
    elif timestamp_type == 3:
        timestamp_year = bytes_to_int(data_pc[0:1])
        timestamp_month = bytes_to_int(data_pc[1:2])
        timestamp_day = bytes_to_int(data_pc[2:3])
        timestamp_hour = bytes_to_int(data_pc[3:4])
        timestamp_sec = round(float(struct.unpack('<L', data_pc[4:8])[0]) / 1000000.0, 6)  # convert to seconds

        timestamp_sec += timestamp_hour * 3600.  # seconds into the day
    else:
        raise ValueError(f"Unknown timestamp type {timestamp_type}")

        # TODO: check and adjust for hour, day, month and year crossovers

    return timestamp_sec
