"""Local receiver for development experiments with the course producer."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


REQUIRED_FIELDS = {
    "notification_type",
    "researcher",
    "experiment_id",
    "measurement_id",
    "cipher_data",
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        url = urlsplit(self.path)
        token = parse_qs(url.query).get("token", [""])[0]
        try:
            body = json.loads(
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
            )
        except (ValueError, json.JSONDecodeError):
            body = {}

        valid = (
            url.path == "/api/notify"
            and bool(token)
            and isinstance(body, dict)
            and REQUIRED_FIELDS.issubset(body)
            and body.get("notification_type") in {"Stabilized", "OutOfRange"}
        )
        self.send_response(200 if valid else 400)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"0.0" if valid else b'{"error":"invalid request"}')
        if valid:
            print(
                f"mock_notification type={body['notification_type']} "
                f"experiment={body['experiment_id']} "
                f"measurement={body['measurement_id']}",
                flush=True,
            )

    def log_message(self, _format: str, *args: object) -> None:
        # The default HTTP log includes the token-bearing request URL.
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 3000), Handler).serve_forever()
