"""``ThreadingHTTPServer`` without the reverse-DNS lookup.

``http.server.HTTPServer.server_bind`` calls ``socket.getfqdn(host)``. On macOS (seen on GitHub's
macOS runners) that lookup can stall for many seconds per server, which made the dev server and
the webhook receiver look dead at startup. The name is only used for CGI variables, so skip it.
"""

from __future__ import annotations

import socketserver
from http.server import ThreadingHTTPServer


class FastHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)
