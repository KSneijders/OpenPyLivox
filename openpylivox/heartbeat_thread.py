import select
import threading
import time

from openpylivox.helper import _parse_resp


class HeartbeatThread:

    def __init__(self, interval, transmit_socket, send_to_ip, send_to_port, send_command, show_messages, format_spaces):
        self.interval = interval
        self.ip = send_to_ip
        self.port = send_to_port
        self.transmit_socket = transmit_socket
        self.send_command = send_command
        self.work_state = -1
        self._show_messages = show_messages
        self._format_spaces = format_spaces
        self.idle_state = True
        self.started = True

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        error_prefix = f"   {self.ip}{self._format_spaces * 2}   -->     "

        while self.started:
            self.idle_state = False
            self.transmit_socket.sendto(self.send_command, (self.ip, self.port))

            # check for proper response from heartbeat request
            if select.select([self.transmit_socket], [], [], 0.1)[0]:
                
                bin_data, addr = self.transmit_socket.recvfrom(22)
                response = _parse_resp(self._show_messages, bin_data)

                if response.data_message != "General":
                    self.incorrect_response()
                    break

                if response.cmd_message == "ACK (response)" and response.data_id == "3":

                    ret_code = int.from_bytes(response.data[0], byteorder='little')
                    if ret_code == 0:

                        self.work_state = int.from_bytes(response.data[1], byteorder='little')
                        # TODO: read and store the lidar status codes from heartbeat response (right now only
                        #  being read from data stream)

                        if self.work_state == 4:
                            raise RuntimeError(f"{error_prefix}*** ERROR: HEARTBEAT ERROR MESSAGE RECEIVED ***")

                    else:
                        self.incorrect_response()

                elif response.cmd_message == "MSG (message)" and response.data_id == "7":
                    raise RuntimeError(f"{error_prefix}*** ERROR: ABNORMAL STATUS MESSAGE RECEIVED ***")

                else:
                    self.incorrect_response()

            self.idle_state = True
            time.sleep(self.interval)

    def stop(self):
        self.started = False
        self.thread.join()
        self.idle_state = True

    def incorrect_response(self):
        error_prefix = f"   {self.ip}{self._format_spaces * 2}   -->     "

        if self._show_messages:
            print(f"{error_prefix}incorrect heartbeat response")
