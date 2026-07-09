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


VNSTOCK_DATA_VERSION = "3.2.3"


def _download_and_install_vnii_package(package_name: str, version: str) -> bool:
    """Tải + cài 1 gói proprietary qua API vnii, KHÔNG dùng
    vnii.install_package()/download_and_install() tiện lợi có sẵn — 2 hàm đó
    đặt tên file tải về mặc định là "<package>.whl" khi response API thiếu
    field 'filename', nhưng nội dung thật lại là GZIP TARBALL (magic bytes
    \\x1f\\x8b, không phải PK/zip của wheel) → pip từ chối với lỗi "is not a
    valid wheel filename"/"is invalid" (verify thủ công 09/07/2026 với
    vnstock_data 3.2.3). Đây là bug phía vnii, không phải nhầm lẫn ở phía
    project — sửa bằng cách tự đặt lại đuôi .tar.gz (sdist) trước khi gọi
    PackageManager.install_package(), pip cài sdist bình thường."""
    from vnii.packages import PackageManager
    from vnii.utils import get_vnstock_directory

    pm = PackageManager(get_vnstock_directory())
    path = pm.download_package(package_name, version)
    fixed = path.parent / f"{package_name}-{version}.tar.gz"
    path = path.rename(fixed)
    return pm.install_package(path)


def ensure_vnstock_data(
    import_fn: Optional[Callable] = None,
    install_fn: Optional[Callable] = None,
    version: str = VNSTOCK_DATA_VERSION,
) -> bool:
    """Đảm bảo vnstock_data (gói sponsor proprietary) import được, tự tải+cài
    nếu thiếu. Trả True nếu sẵn sàng, False nếu không (data_source sẽ lỗi khi
    gọi thật — không chặn app khởi động, tự retry ở request kế nếu deploy có
    mạng lại).

    vnstock_data KHÔNG cài được qua pip — không nằm trên bất kỳ index nào,
    kể cả --extra-index-url của vnstocks.com. Nó là gói proprietary chỉ tải
    qua API của vnii bằng API key (đã nạp bởi ensure_license() gọi trước hàm
    này). requirements.txt vì vậy KHÔNG được pin vnstock_data.

    Gọi hàm này ở container startup (không phải build time) để tái dùng
    VNSTOCK_API_KEY runtime có sẵn — không cần thêm secret build-time riêng."""
    if import_fn is None:
        import importlib

        import_fn = lambda: importlib.import_module("vnstock_data")  # noqa: E731

    try:
        import_fn()
        return True
    except ImportError:
        pass

    if install_fn is None:
        install_fn = _download_and_install_vnii_package

    try:
        ok = install_fn(package_name="vnstock_data", version=version)
    except BaseException as e:  # noqa: BLE001 — vnii có thể ném SystemExit
        logger.warning("cai vnstock_data that bai (%s) — data_source se loi khi goi", e)
        return False

    if not ok:
        logger.warning("cai vnstock_data that bai — data_source se loi khi goi")
        return False

    import importlib

    importlib.invalidate_caches()
    logger.info("vnstock_data da san sang (moi cai, version=%s)", version)
    return True
