import threading
import struct
import time
from multiprocessing import Queue


class DataQueueReaderThread:

    def __init__(self, queue):
        self.queue = queue
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        print("STARTING DATA QUEUE READER THREAD)")
        i = 0
        while True:
            data_point = self.queue.get()
            if not data_point:
                time.sleep(0.1)
                continue
            if data_point == -1:
                print("END OF QUEUE REACHED, BREAKING")
                break
            usable_data = decode_spherical_data(data_point)
            i += 1
            distance = usable_data[0]
            zenith = usable_data[1]
            azimuth = usable_data[2]
            intensity = usable_data[3]
            timestamp = usable_data[4]
            # TODO: Use this data! \o/
            if i == 1:
                print("FIRST VALUE READ FROM QUEUE")
            if i % 10000 == 0:
                print(i)
        print("We're done here")


def decode_spherical_data(binary_data_point):
    point_format = '<iHHBd'
    point = struct.unpack(point_format, binary_data_point)
    pt = [0, 0, 0, 0, 0]
    pt[0] = point[0] / 1000.  # Distance
    pt[1] = point[1] / 100.  # Zenimuth
    pt[2] = point[2] / 100.  # Azimuth
    pt[3] = point[3]  # Intensity
    pt[4] = point[4]  # Timestamp
    return pt