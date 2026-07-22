"""Nạp biến môi trường từ file .env cạnh project.

Dùng chung cho dnse_stream (creds DNSE) và vnstock_license (VNSTOCK_API_KEY)
— không kéo thêm dependency python-dotenv chỉ cho việc này.
"""

import os

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(MODULE_DIR))
DEFAULT_ENV_FILE = os.path.join(PROJECT_ROOT, ".env")


def load_dotenv(path: str | None = None) -> None:
    """Nạp biến môi trường từ .env, KHÔNG ghi đè env đã có (env thật của
    deploy phải thắng file local). File thiếu/không đọc được → bỏ qua.

    Default resolve LÚC GỌI (không bind lúc def) để test monkeypatch được
    DEFAULT_ENV_FILE — cách ly test khỏi .env thật chứa creds."""
    if path is None:
        path = DEFAULT_ENV_FILE
    try:
        with open(path, encoding="utf-8-sig") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        return
