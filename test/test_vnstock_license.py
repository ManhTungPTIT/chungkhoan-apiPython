"""Test vnstock_license offline — inject fake lc_init, không gọi mạng."""

import logging
import os

import vnstock_license


def _no_env(tmp_path):
    """Đường dẫn .env không tồn tại — cách ly test khỏi .env thật của project."""
    return str(tmp_path / "khong-co.env")


def test_thanh_cong_tra_license_va_log_tier(caplog, tmp_path):
    info = {"tier": "golden", "user": "Vu Le"}
    with caplog.at_level(logging.INFO):
        out = vnstock_license.ensure_license(
            lc_init_fn=lambda **kw: info, env_path=_no_env(tmp_path)
        )
    assert out == info
    assert "golden" in caplog.text


def test_lc_init_systemexit_tra_none_va_warning(caplog, tmp_path):
    def boom(**kw):
        raise SystemExit("Authentication failed")

    with caplog.at_level(logging.WARNING):
        out = vnstock_license.ensure_license(lc_init_fn=boom, env_path=_no_env(tmp_path))
    assert out is None
    assert "Community" in caplog.text


def test_lc_init_exception_thuong_tra_none(caplog, tmp_path):
    def boom(**kw):
        raise RuntimeError("mạng chết")

    with caplog.at_level(logging.WARNING):
        out = vnstock_license.ensure_license(lc_init_fn=boom, env_path=_no_env(tmp_path))
    assert out is None


def test_nap_env_file_truoc_khi_goi_lc_init(monkeypatch, tmp_path):
    """Key trong .env phải vào os.environ TRƯỚC khi lc_init chạy — vnii đọc
    VNSTOCK_API_KEY từ env."""
    monkeypatch.delenv("VNSTOCK_API_KEY", raising=False)
    f = tmp_path / ".env"
    f.write_text("VNSTOCK_API_KEY=key-tu-dotenv\n", encoding="utf-8")

    seen = {}

    def fake_lc_init(**kw):
        seen["key"] = os.environ.get("VNSTOCK_API_KEY")
        return {"tier": "golden"}

    vnstock_license.ensure_license(lc_init_fn=fake_lc_init, env_path=str(f))
    assert seen["key"] == "key-tu-dotenv"


# ===== ensure_vnstock_data =====


def _chua_cai(name):
    raise Exception(f"No package metadata was found for {name}")

# vnstock_data KHÔNG cài được qua pip (không có trên bất kỳ index nào) — chỉ
# tải qua vnii.install_package() bằng API key. ensure_vnstock_data tự cài nếu
# thiếu, để requirements.txt không cần (và không thể) pin nó.


def test_da_co_san_dung_ban_pin_khong_goi_install(caplog):
    install_calls = []
    out = vnstock_license.ensure_vnstock_data(
        import_fn=lambda: object(),
        install_fn=lambda **kw: install_calls.append(kw),
        version_fn=lambda name: vnstock_license.VNSTOCK_DATA_VERSION,
    )
    assert out is True
    assert install_calls == []


def test_version_lech_pin_cai_lai_ban_pin_khong_import_ban_cu(caplog):
    """Môi trường deploy đã có bản CŨ (vd 3.2.3 khi pin 3.2.5): phải cài lại
    bản pin NGAY, KHÔNG import bản cũ trước — import rồi thì sys.modules giữ
    bản cũ tới hết đời process, cài xong cũng không có tác dụng boot này."""

    def import_fn():
        raise AssertionError("không được import khi version lệch pin")

    seen = {}

    def install_fn(package_name, version):
        seen.update(package_name=package_name, version=version)
        return True

    with caplog.at_level(logging.INFO):
        out = vnstock_license.ensure_vnstock_data(
            import_fn=import_fn,
            install_fn=install_fn,
            version_fn=lambda name: "0.0.1",
        )
    assert out is True
    assert seen["version"] == vnstock_license.VNSTOCK_DATA_VERSION


def test_khong_co_metadata_van_import_duoc_thi_giu_nguyen(caplog):
    """Metadata pip thiếu (vd cài tay kiểu lạ) nhưng import chạy → giữ nguyên
    hành vi cũ: coi như sẵn sàng, không cài đè."""

    def version_fn(name):
        raise Exception("No package metadata was found for vnstock_data")

    install_calls = []
    out = vnstock_license.ensure_vnstock_data(
        import_fn=lambda: object(),
        install_fn=lambda **kw: install_calls.append(kw),
        version_fn=version_fn,
    )
    assert out is True
    assert install_calls == []


def test_chua_co_cai_thanh_cong(caplog):
    def import_fn():
        raise ModuleNotFoundError("No module named 'vnstock_data'")

    seen = {}

    def install_fn(package_name, version):
        seen.update(package_name=package_name, version=version)
        return True

    with caplog.at_level(logging.INFO):
        out = vnstock_license.ensure_vnstock_data(
            import_fn=import_fn, install_fn=install_fn, version_fn=_chua_cai
        )
    assert out is True
    assert seen == {"package_name": "vnstock_data", "version": vnstock_license.VNSTOCK_DATA_VERSION}
    assert "vnstock_data" in caplog.text


def test_chua_co_cai_that_bai_tra_false(caplog):
    def import_fn():
        raise ModuleNotFoundError

    with caplog.at_level(logging.WARNING):
        out = vnstock_license.ensure_vnstock_data(
            import_fn=import_fn, install_fn=lambda **kw: False, version_fn=_chua_cai
        )
    assert out is False
    assert "vnstock_data" in caplog.text


def test_install_nem_loi_khong_crash_tra_false(caplog):
    def import_fn():
        raise ModuleNotFoundError

    def install_fn(**kw):
        raise RuntimeError("mạng chết")

    with caplog.at_level(logging.WARNING):
        out = vnstock_license.ensure_vnstock_data(
            import_fn=import_fn, install_fn=install_fn, version_fn=_chua_cai
        )
    assert out is False


# ===== ensure_user_profile =====
# vnstock_data.core.utils.env.idv() (gọi lúc import ở connector/explorer
# __init__.py) raise SystemExit "Không tìm thấy thông tin người dùng hợp lệ"
# nếu ~/.vnstock/user.json thiếu hoặc field "user" rỗng — kiểm tra THUẦN
# CLIENT-SIDE, không liên quan license/quota/server. Container ephemeral
# (fresh filesystem) không có file này vì chỉ vnstock_installer (chạy tương
# tác) mới tạo nó — ensure_user_profile tự tạo để import vnstock_data không
# vỡ. Xác minh (09/07/2026): xóa user.json tái hiện đúng lỗi production,
# khôi phục file là hết lỗi ngay.


def test_da_co_user_json_khong_ghi_de(tmp_path):
    path = tmp_path / "user.json"
    path.write_text('{"user": "da co san"}', encoding="utf-8")
    out = vnstock_license.ensure_user_profile(path=str(path))
    assert out is True
    assert path.read_text(encoding="utf-8") == '{"user": "da co san"}'


def test_thieu_thi_tao_moi_voi_user_khac_rong(tmp_path):
    path = tmp_path / "sub" / "user.json"
    out = vnstock_license.ensure_user_profile(path=str(path))
    assert out is True
    assert path.exists()
    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["user"]  # truthy — đúng điều kiện idv() cần


def test_ghi_loi_tra_false_khong_crash(caplog, tmp_path):
    # "thu muc cha" thuc chat la 1 FILE -> makedirs/open deu loi
    fake_parent = tmp_path / "khong-phai-thu-muc"
    fake_parent.write_text("x", encoding="utf-8")
    path = fake_parent / "user.json"
    with caplog.at_level(logging.WARNING):
        out = vnstock_license.ensure_user_profile(path=str(path))
    assert out is False


# ===== patch_vnstock_data_retries =====
# vnstock_data dùng tenacity @retry(stop=stop_after_attempt(Config.RETRIES))
# trên các method fetch (vd price_board) — Config.RETRIES được ĐỌC 1 LẦN DUY
# NHẤT lúc module định nghĩa hàm (import time) rồi "bake" cứng vào decorator,
# nên set Config.RETRIES SAU khi package đã import không có tác dụng gì. Phải
# nạp config.py độc lập (bypass __init__ package) rồi đăng ký vào sys.modules
# TRƯỚC lần `import vnstock_data` đầu tiên trong tiến trình.
#
# Test dùng package_name GIẢ (không phải "vnstock_data" thật) để không đụng
# gói thật đã có thể đã import trong tiến trình pytest.


def _make_fake_package(tmp_path, package_name, initial_retries=3):
    """Tạo 1 package giả tối thiểu {package_name}/config.py với class Config
    có RETRIES, mô phỏng đúng shape thật của vnstock_data/config.py."""
    pkg_dir = tmp_path / package_name
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "config.py").write_text(
        f"class Config:\n    RETRIES = {initial_retries}\n", encoding="utf-8"
    )
    return pkg_dir


def _find_spec_for(pkg_dir):
    """find_spec_fn giả trỏ đúng vào thư mục package giả — thay cho
    importlib.util.find_spec thật (không cần cài package thật lên sys.path)."""
    import importlib.util

    def find_spec_fn(name):
        return importlib.util.spec_from_file_location(
            name, pkg_dir / "__init__.py", submodule_search_locations=[str(pkg_dir)]
        )

    return find_spec_fn


def test_patch_retries_thanh_cong_truoc_khi_import(monkeypatch, tmp_path):
    pkg_dir = _make_fake_package(tmp_path, "fake_vnstock_data_ok", initial_retries=3)
    monkeypatch.delitem(__import__("sys").modules, "fake_vnstock_data_ok", raising=False)
    monkeypatch.delitem(
        __import__("sys").modules, "fake_vnstock_data_ok.config", raising=False
    )

    out = vnstock_license.patch_vnstock_data_retries(
        retries=1,
        package_name="fake_vnstock_data_ok",
        find_spec_fn=_find_spec_for(pkg_dir),
    )

    assert out is True
    import sys

    patched = sys.modules["fake_vnstock_data_ok.config"]
    assert patched.Config.RETRIES == 1


def test_patch_retries_da_import_roi_tra_false(monkeypatch, tmp_path):
    """Package (hoặc .config) đã có trong sys.modules → quá muộn để patch,
    decorator @retry của các hàm đã bake giá trị RETRIES gốc."""
    import sys
    import types

    already = types.ModuleType("fake_vnstock_data_late.config")
    monkeypatch.setitem(sys.modules, "fake_vnstock_data_late.config", already)

    out = vnstock_license.patch_vnstock_data_retries(
        retries=1, package_name="fake_vnstock_data_late"
    )
    assert out is False


def test_patch_retries_khong_tim_thay_spec_tra_false(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        out = vnstock_license.patch_vnstock_data_retries(
            retries=1,
            package_name="fake_vnstock_data_khong_ton_tai",
            find_spec_fn=lambda name: None,
        )
    assert out is False
