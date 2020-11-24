CMD_QUERY = bytes.fromhex((b'AA010F0000000004D70002AE8A8A7B').decode('ascii'))
CMD_HEARTBEAT = bytes.fromhex((b'AA010F0000000004D7000338BA8D0C').decode('ascii'))
CMD_DISCONNECT = bytes.fromhex((b'AA010F0000000004D70006B74EE77C').decode('ascii'))
CMD_READ_EXTRINSIC = bytes.fromhex((b'AA010F0000000004D70102EFBB9162').decode('ascii'))
CMD_GET_FAN = bytes.fromhex((b'AA010F0000000004D701054C2EF5FC').decode('ascii'))
CMD_GET_IMU = bytes.fromhex((b'AA010F0000000004D70109676243F5').decode('ascii'))

CMD_RAIN_FOG_ON = bytes.fromhex((b'AA011000000000B809010301D271D049').decode('ascii'))
CMD_RAIN_FOG_OFF = bytes.fromhex((b'AA011000000000B8090103004441D73E').decode('ascii'))
CMD_LIDAR_START = bytes.fromhex((b'AA011000000000B8090100011122FD62').decode('ascii'))
CMD_LIDAR_POWERSAVE = bytes.fromhex((b'AA011000000000B809010002AB73F4FB').decode('ascii'))
CMD_LIDAR_STANDBY = bytes.fromhex((b'AA011000000000B8090100033D43F38C').decode('ascii'))
CMD_DATA_STOP = bytes.fromhex((b'AA011000000000B809000400B4BD5470').decode('ascii'))
CMD_DATA_START = bytes.fromhex((b'AA011000000000B809000401228D5307').decode('ascii'))
CMD_CARTESIAN_CS = bytes.fromhex((b'AA011000000000B809000500F58C4F69').decode('ascii'))
CMD_SPHERICAL_CS = bytes.fromhex((b'AA011000000000B80900050163BC481E').decode('ascii'))
CMD_FAN_ON = bytes.fromhex((b'AA011000000000B80901040115E79106').decode('ascii'))
CMD_FAN_OFF = bytes.fromhex((b'AA011000000000B80901040083D79671').decode('ascii'))
CMD_LIDAR_SINGLE_1ST = bytes.fromhex((b'AA011000000000B80901060001B5A043').decode('ascii'))
CMD_LIDAR_SINGLE_STRONGEST = bytes.fromhex((b'AA011000000000B8090106019785A734').decode('ascii'))
CMD_LIDAR_DUAL = bytes.fromhex((b'AA011000000000B8090106022DD4AEAD').decode('ascii'))
CMD_IMU_DATA_ON = bytes.fromhex((b'AA011000000000B80901080119A824AA').decode('ascii'))
CMD_IMU_DATA_OFF = bytes.fromhex((b'AA011000000000B8090108008F9823DD').decode('ascii'))

CMD_REBOOT = bytes.fromhex((b'AA011100000000FC02000A000004477736').decode('ascii'))

CMD_DYNAMIC_IP = bytes.fromhex((b'AA011400000000A8240008000000000068F8DD50').decode('ascii'))
CMD_WRITE_ZERO_EO = bytes.fromhex((b'AA012700000000B5ED01010000000000000000000000000000000000000000000000004CDEA4E7').decode('ascii'))

SPECIAL_FIRMWARE_TYPE_DICT = {"03.03.0001": 2,
                               "03.03.0002": 3,
                               "03.03.0006": 2,
                               "03.03.0007": 3}
