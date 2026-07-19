"""Xác thực license vnstock trả phí (gói vnii) lúc khởi động app.

vnii tìm key theo thứ tự: env VNSTOCK_API_KEY → ~/.vnstock/api_key.json.
Ở đây nạp .env cạnh project vào env trước nên deploy máy khác chỉ cần mang
.env theo (hoặc set env var). Xác thực lỗi/thiếu key KHÔNG làm chết app —
vnstock vẫn chạy nhưng bị giới hạn tier Community 60 req/phút (Golden: 500).
"""

import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
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


# 3.2.5: bản tối thiểu có nguồn ASEAN cho Quote.history (data_source._default_history
# dùng ASEAN để né anti-bot host VCI — xác nhận với hỗ trợ vnstock 19/07/2026).
VNSTOCK_DATA_VERSION = "3.2.5"


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


def patch_vnstock_data_retries(
    retries: int = 1,
    package_name: str = "vnstock_data",
    find_spec_fn: Optional[Callable] = None,
) -> bool:
    """Giảm số lần tenacity tự retry nội bộ của vnstock_data (mặc định 3) —
    PHẢI gọi TRƯỚC ensure_vnstock_data()/bất kỳ import vnstock_data nào khác
    trong tiến trình, nếu không sẽ không có tác dụng gì.

    Các method fetch của vnstock_data (vd Trading.price_board) decorate bằng
    @retry(stop=stop_after_attempt(Config.RETRIES)) — tenacity ĐỌC
    Config.RETRIES ĐÚNG 1 LẦN lúc module định nghĩa hàm (import time) rồi bake
    cứng số đó vào decorator; sửa Config.RETRIES sau khi package đã import
    không đổi được hành vi đã bake (verify thủ công 18/07/2026). Vì vậy phải
    nạp config.py ĐỘC LẬP (bypass __init__ package, importlib.util) rồi đăng
    ký thẳng vào sys.modules TRƯỚC — khi vnstock_data (hay submodule nào của
    nó) `from vnstock_data.config import Config`, Python thấy module đã có
    sẵn trong sys.modules nên dùng luôn bản đã patch, không chạy lại file gốc.

    KHÔNG giảm được REQUEST_TIMEOUT (mặc định 30s/lần) qua đường này — các
    method fetch (vd price_board) gọi send_request() không truyền timeout=,
    luôn dùng mặc định cứng 30 viết literal trong client.py, không đọc từ
    Config ở đường gọi đó.

    Trả True nếu patch được, False nếu quá muộn (đã import) hoặc không tìm
    thấy package — KHÔNG raise, chỉ log cảnh báo (patch fail không nên chặn
    app khởi động, chỉ mất tối ưu retry, vnstock_data vẫn dùng mặc định)."""
    if package_name in sys.modules or f"{package_name}.config" in sys.modules:
        logger.warning(
            "%s da duoc import truoc do — qua muon de giam RETRIES (decorator "
            "@retry da bake gia tri goc luc import)",
            package_name,
        )
        return False

    find_spec_fn = find_spec_fn or importlib.util.find_spec
    try:
        spec = find_spec_fn(package_name)
    except (ImportError, ValueError) as e:
        logger.warning("khong tim duoc %s de giam RETRIES (%s)", package_name, e)
        return False
    if spec is None or not spec.submodule_search_locations:
        logger.warning("khong tim duoc %s de giam RETRIES (spec rong)", package_name)
        return False

    cfg_path = os.path.join(spec.submodule_search_locations[0], "config.py")
    try:
        cfg_spec = importlib.util.spec_from_file_location(
            f"{package_name}.config", cfg_path
        )
        cfg_mod = importlib.util.module_from_spec(cfg_spec)
        cfg_spec.loader.exec_module(cfg_mod)
    except (FileNotFoundError, OSError, AttributeError) as e:
        logger.warning("khong nap duoc %s/config.py de giam RETRIES (%s)", package_name, e)
        return False

    cfg_mod.Config.RETRIES = retries
    sys.modules[f"{package_name}.config"] = cfg_mod
    logger.info("da giam %s RETRIES xuong %s truoc luc import", package_name, retries)
    return True


def ensure_vnstock_data(
    import_fn: Optional[Callable] = None,
    install_fn: Optional[Callable] = None,
    version: str = VNSTOCK_DATA_VERSION,
    version_fn: Optional[Callable] = None,
) -> bool:
    """Đảm bảo vnstock_data (gói sponsor proprietary) import được VÀ đúng bản
    pin VNSTOCK_DATA_VERSION — tự tải+cài nếu thiếu hoặc LỆCH VERSION (deploy
    có sẵn bản cũ sẽ được nâng cấp tự động lúc khởi động, không cần cài tay).
    Trả True nếu sẵn sàng, False nếu không (data_source sẽ lỗi khi gọi thật —
    không chặn app khởi động, tự retry ở request kế nếu deploy có mạng lại).

    So version bằng metadata pip (importlib.metadata) chứ KHÔNG import trước:
    - vnstock_data.__version__ là chuỗi STALE (bản 3.2.5 vẫn in 3.2.2) nên
      không tin được;
    - import bản cũ rồi mới cài thì sys.modules đã giữ bản cũ tới hết đời
      process — cài xong cũng không có tác dụng ngay boot này.
    Metadata thiếu (cài tay kiểu lạ) mà import vẫn chạy → giữ hành vi cũ, coi
    như sẵn sàng, không cài đè.

    vnstock_data KHÔNG cài được qua pip — không nằm trên bất kỳ index nào,
    kể cả --extra-index-url của vnstocks.com. Nó là gói proprietary chỉ tải
    qua API của vnii bằng API key (đã nạp bởi ensure_license() gọi trước hàm
    này). requirements.txt vì vậy KHÔNG được pin vnstock_data.

    Gọi hàm này ở container startup (không phải build time) để tái dùng
    VNSTOCK_API_KEY runtime có sẵn — không cần thêm secret build-time riêng."""
    if import_fn is None:
        import importlib

        import_fn = lambda: importlib.import_module("vnstock_data")  # noqa: E731
    if version_fn is None:
        from importlib.metadata import version as version_fn  # noqa: F811

    try:
        installed = version_fn("vnstock_data")
    except Exception:  # PackageNotFoundError hoặc metadata hỏng
        installed = None

    if installed is not None and installed != version:
        logger.info(
            "vnstock_data %s khac ban pin %s — cai lai ban pin", installed, version
        )
    else:
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


def ensure_user_profile(path: Optional[str] = None) -> bool:
    """Đảm bảo ~/.vnstock/user.json tồn tại với field "user" khác rỗng.

    vnstock_data.core.utils.env.idv() (gọi lúc IMPORT vnstock_data, ở
    connector/explorer __init__.py) raise SystemExit "Không tìm thấy thông
    tin người dùng hợp lệ" nếu thiếu file này hoặc field "user" rỗng — đây là
    kiểm tra THUẦN CLIENT-SIDE, không phải license/quota/server (verify
    09/07/2026: response HTTP mọi API call đều 200, lỗi không xuất hiện
    trong bất kỳ response nào — lỗi raise trực tiếp trong code vnstock_data
    trước khi kịp gọi mạng lấy dữ liệu thật).

    File này bình thường do vnstock_installer tạo lúc cài đặt TƯƠNG TÁC —
    container/CI không chạy bước đó nên thiếu. Gọi hàm này trước
    ensure_vnstock_data() ở container startup. Không ghi đè file đã có (để
    không mất dữ liệu profile thật nếu deploy có sẵn)."""
    if path is None:
        path = str(Path.home() / ".vnstock" / "user.json")

    if os.path.exists(path):
        return True

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"user": "vnstock_realtime"}, f)
        return True
    except OSError as e:
        logger.warning("khong tao duoc user.json (%s) — import vnstock_data se loi", e)
        return False
