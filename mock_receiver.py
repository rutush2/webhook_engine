import json
import logging
import random
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MockReceiver")


class WebhookHandler(BaseHTTPRequestHandler):
    def _send_response(self, status_code: int, message: dict):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(message).encode("utf-8"))

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""

        logger.info(f"Received POST request on path '{self.path}' | Bytes: {len(body)}")

        if self.path == "/webhook/success":
            self._send_response(200, {"status": "success", "received": True})

        elif self.path == "/webhook/error-500":
            self._send_response(500, {"error": "Internal Server Error", "retryable": True})

        elif self.path == "/webhook/error-400":
            self._send_response(400, {"error": "Bad Request: Invalid Payload", "retryable": False})

        elif self.path == "/webhook/rate-limit":
            self._send_response(429, {"error": "Too Many Requests", "retry_after_seconds": 5})

        elif self.path == "/webhook/timeout":
            time.sleep(6.0)
            self._send_response(200, {"status": "delayed_success"})

        elif self.path == "/webhook/flaky":
            if random.random() < 0.7:
                self._send_response(503, {"error": "Service Temporarily Unavailable"})
            else:
                self._send_response(200, {"status": "flaky_success"})

        else:
            self._send_response(404, {"error": "Endpoint Not Found"})

    def log_message(self, format, *args):
        return


def run_mock_server(host: str = "127.0.0.1", port: int = 8080):
    server_address = (host, port)
    httpd = HTTPServer(server_address, WebhookHandler)
    logger.info(f"Mock Webhook Receiver running on http://{host}:{port}")
    logger.info("Available routes:")
    logger.info("  - http://127.0.0.1:8080/webhook/success    (HTTP 200)")
    logger.info("  - http://127.0.0.1:8080/webhook/error-500  (HTTP 500)")
    logger.info("  - http://127.0.0.1:8080/webhook/error-400  (HTTP 400)")
    logger.info("  - http://127.0.0.1:8080/webhook/rate-limit (HTTP 429)")
    logger.info("  - http://127.0.0.1:8080/webhook/timeout    (Delays 6s)")
    logger.info("  - http://127.0.0.1:8080/webhook/flaky      (70% failure rate)")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("\nShutting down Mock Webhook Receiver...")
        httpd.server_close()


if __name__ == "__main__":
    run_mock_server()