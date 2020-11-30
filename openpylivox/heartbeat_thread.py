import select
import sys
import threading
import time

# from openpylivox.openpylivox import OpenPyLivox
from openpylivox import helper
from openpylivox.helper import _parse_resp


class HeartbeatThread:

    def __init__(self, interval, transmit_socket, send_to_ip, send_to_port, send_command, show_messages, format_spaces):
        self.interval = interval
        self.ip = send_to_ip
        self.port = send_to_port
        self.t_socket = transmit_socket
        self.t_command = send_command
        self.started = True
        self.work_state = -1
        self.idle_state = 0
        self._show_messages = show_messages

        self.msg = helper.Msg(
            show_message=show_messages,
            sensor_ip=send_to_ip,
            format_spaces=format_spaces,
            default_arrow="-->"
        )
        self._format_spaces = format_spaces

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        while True:
            if self.started:
                self.t_socket.sendto(self.t_command, (self.ip, self.port))

                # check for proper response from heartbeat request
                if select.select([self.t_socket], [], [], 0.1)[0]:
                    bin_data, addr = self.t_socket.recvfrom(22)
                    _, ack, cmd_set, cmd_id, ret_code_bin = _parse_resp(self._show_messages, bin_data)

                    if ack == "ACK (response)" and cmd_set == "General" and cmd_id == "3":
                        ret_code = int.from_bytes(ret_code_bin[0], byteorder='little')
                        if ret_code != 0:
                            self.msg.prefix_print("incorrect heartbeat response", f_spaces=2)
                        else:
                            self.work_state = int.from_bytes(ret_code_bin[1], byteorder='little')

                            # TODO: read and store the lidar status codes from heartbeat response (right now only
                            #  being read from data stream)

                            if self.work_state == 4:
                                raise RuntimeError(
                                    f"{self.msg.get_prefix()}*** ERROR: HEARTBEAT ERROR MESSAGE RECEIVED ***"
                                )
                    elif ack == "MSG (message)" and cmd_set == "General" and cmd_id == "7":
                        raise RuntimeError(
                            f"{self.msg.get_prefix()}*** ERROR: ABNORMAL STATUS MESSAGE RECEIVED ***"
                        )
                    else:
                        self.msg.prefix_print("incorrect heartbeat response")

                for i in range(9, -1, -1):
                    self.idle_state = i
                    time.sleep(self.interval / 10.0)
            else:
                break

    def stop(self):
        self.started = False
        self.thread.join()
        self.idle_state = 9
