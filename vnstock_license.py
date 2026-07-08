"""Xác thực license vnstock trả phí (gói vnii) lúc khởi động app.

vnii tìm key theo thứ tự: env VNSTOCK_API_KEY → ~/.vnstock/api_key.json.
Ở đây nạp .env cạnh project vào env trước nên deploy máy khác chỉ cần mang
.env theo (hoặc set env var). Xác thực lỗi/thiếu key KHÔNG làm chết app —
vnstock vẫn chạy nhưng bị giới hạn tier Community 60 req/phút (Golden: 500).
"""

import logging
from typing import Callable, Optional

import envfile

logger = logging.getLogger(__name__)
# Project không cấu hình logging (root chỉ có lastResort in WARNING+), nên
# tự gắn handler cho riêng logger này để dòng tier LUÔN hiện lúc khởi động —
# đây là mục đích chính của module. Chỉ ảnh hưởng logger này, không đụng root.
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def ensure_license(
    lc_init_fn: Optional[Callable] = None,
    env_path: Optional[str] = None,
) -> Optional[dict]:
    """Verify license vnii, trả dict license info ({tier, ...}) hoặc None nếu lỗi.

    Gọi nhiều lần vô hại — vnii cache auth state ~60 phút."""
    envfile.load_dotenv(env_path)

    if lc_init_fn is None:
        try:
            from vnii import lc_init as lc_init_fn
        except ImportError as e:
            logger.warning(
                "thieu goi vnii (%s) — vnstock chay tier Community 60 req/phut", e
            )
            return None

    try:
        info = lc_init_fn(package_name="vnstock")
    except BaseException as e:  # noqa: BLE001 — vnii ném SystemExit khi key sai/thiếu
        logger.warning(
            "xac thuc license vnstock that bai (%s) — chay tier Community 60 req/phut",
            e,
        )
        return None

    tier = info.get("tier") if isinstance(info, dict) else info
    logger.info("vnstock license OK — tier=%s", tier)
    return info
