import select
import sys
import threading
import time

# from openpylivox.openpylivox import OpenPyLivox
from openpylivox.helper import _parse_resp


class HeartbeatThread:

    def __init__(self, interval, transmit_socket, send_to_IP, send_to_port, send_command, showMessages, format_spaces, host_obj):
        self.interval = interval
        self.IP = send_to_IP
        self.port = send_to_port
        self.t_socket = transmit_socket
        self.t_command = send_command
        self.started = True
        self.work_state = -1
        self.idle_state = 0
        self._showMessages = showMessages
        self._format_spaces = format_spaces
        self.host_obj = host_obj

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        while True:
            if self.started:
                self.t_socket.sendto(self.t_command, (self.IP, self.port))

                # check for proper response from heartbeat request
                if select.select([self.t_socket], [], [], 0.1)[0]:
                    binData, addr = self.t_socket.recvfrom(22)
                    # tempObj = OpenPyLivox()
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._showMessages, binData)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "3":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                        if ret_code != 0:
                            if self._showMessages: print(
                                "   " + self.IP + self._format_spaces + self._format_spaces + "   -->     incorrect heartbeat response")
                        else:
                            self.work_state = int.from_bytes(ret_code_bin[1], byteorder='little')

                            # TODO: read and store the lidar status codes from heartbeat response (right now only being read from data stream)

                            if self.work_state == 4:
                                print(
                                    "   " + self.IP + self._format_spaces + self._format_spaces + "   -->     *** ERROR: HEARTBEAT ERROR MESSAGE RECEIVED ***")
                                sys.exit(0)
                    elif ack == "MSG (message)" and cmd_set == "General" and cmd_id == "7":
                        # not given an option to hide this message!!
                        print(
                            "   " + self.IP + self._format_spaces + self._format_spaces + "   -->     *** ERROR: ABNORMAL STATUS MESSAGE RECEIVED ***")
                        sys.exit(1)
                    else:
                        if self._showMessages: print(
                            "   " + self.IP + self._format_spaces + self._format_spaces + "   -->     incorrect heartbeat response")

                for i in range(9, -1, -1):
                    self.idle_state = i
                    time.sleep(self.interval / 10.0)
            else:
                break

    def stop(self):
        self.started = False
        self.thread.join()
        self.idle_state = 9
