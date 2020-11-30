from enum import IntEnum


class DataType(IntEnum):
    CARTESIAN = 0
    SPHERICAL = 1


class FileType(IntEnum):
    StoredASCII = 0
    RealtimeASCII = 1
    RealtimeBINARY = 2
    RealtimeBINQUEUE = 3


class FirmwareType(IntEnum):
    SINGLE_RETURN = 1
    """Relevant for Mid-40 and Mid-100"""
    DOUBLE_RETURN = 2
    """Mid-40 and Mid-100 only"""
    TRIPLE_RETURN = 3
    """Mid-40 and Mid-100 only"""
