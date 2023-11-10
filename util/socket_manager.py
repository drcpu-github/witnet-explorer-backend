import errno
import json
import os
import socket
import struct
import sys

class SocketManager(object):
    def __init__(self, ip, port, timeout):
        self.ip = ip
        self.port = int(port)
        self.timeout = timeout
        self.old_timeout = timeout

        self.create_socket()

    def init_app(self, app, extension):
        app.extensions = getattr(app, "extensions", {})
        if extension not in app.extensions:
            app.extensions[extension] = self

    def create_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Set socket options
        # Allows close and immediate reuse of an address, ignoring TIME_WAIT
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Always and immediately close a socket, ignoring pending data
        so_onoff, so_linger = 1, 0
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', so_onoff, so_linger))

    def connect(self):
        self.socket.connect((self.ip, self.port))

    def disconnect(self):
        self.socket.close()

    def recreate_socket(self):
        self.close_connection(stop_remote=False)
        self.create_socket()
        self.connect()

    def set_timeout(self, timeout):
        self.old_timeout = self.timeout
        self.timeout = timeout

    def reset_timeout(self):
        self.timeout = self.old_timeout

    def send_request(self, request):
        try:
            self.socket.send((json.dumps(request) + "\n").encode("utf-8"))
        except socket.error as e:
            if e.errno == errno.EPIPE:
                self.recreate_socket()
                self.send_request(request)
                # return False, "Remote socket disconnected"
            else:
                return False, e
        return True, ""

    def retrieve_response(self, request_id):
        # Set large timeout which releases the socket connection
        self.socket.settimeout(self.timeout)
        response = ""
        while True:
            try:
                response += self.socket.recv(1024).decode("utf-8")
                if len(response) == 0 or response[-1] == "\n":
                    break
            except socket.timeout:
                return {"error": {"code": 1, "message": f"Timed out after {self.timeout} seconds"}, "id": request_id}
            except socket.error as e:
                if e.errno == errno.ECONNRESET:
                    return {"error": {"code": 2, "message": f"Connection reset: {os.strerror(e.errno)} ({e.errno})"}, "id": request_id}
                else:
                    return {"error": {"code": 3, "message": f"Unhandled error: {os.strerror(e.errno)} ({e.errno})"}, "id": request_id}
        # Reset time-out and set to blocking mode
        self.socket.settimeout(None)

        if response != "":
            try:
                response = json.loads(response)
                # Retry retrieving the response
                if response["id"] != request_id:
                    return self.retrieve_response(request_id)
                # Return response
                else:
                    return response
            except json.decoder.JSONDecodeError:
                if len(response.split("\n")) > 1:
                    return {"error": {"code": 4, "message": "Received response for multiple requests"}, "id": request_id}
                else:
                    return {"error": {"code": 5, "message": f"Returned a malformed response: {response}"}, "id": request_id}
        else:
            return {"error": {"code": 6, "message": "Returned an empty response"}, "id": request_id}

    def query(self, request):
        # Get request id
        request_id = request["id"]

        # Send request
        success, error = self.send_request(request)
        if not success:
            return {"error": f"could not send request {request['method']}", "reason": str(error), "id": request_id}

        # Retrieve response and make sure the request and response ID match
        response = self.retrieve_response(request_id)

        # Parse error from response if necessary
        reason = "unknown"
        if type(response) is dict and "error" in response:
            reason = response["error"]
            if "reason" in response:
                reason = response["reason"]
            if "params" in request:
                return {"error": f"could not execute {request['method']} with parameters {request['params']}", "reason": reason, "id": response["id"]}
            else:
                return {"error": f"could not execute {request['method']}", "reason": reason, "id": response["id"]}

        return response

    def close_connection(self, stop_remote=True):
        try:
            if stop_remote:
                self.socket.send(b"\n")
            self.disconnect()
        except socket.error as e:
            pass
