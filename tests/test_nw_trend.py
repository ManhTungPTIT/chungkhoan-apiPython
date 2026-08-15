"""Lõi NW của BOT Dài hạn (BOT TREND 2) — bản Python phải khớp calcNwTrend (JS).

Các con số ở đây trùng từng chữ với nwTrend.test.js bên FE: nến biên độ cố định 2
→ WMA(range,10) = 2 → rev = 6 ở mọi nến, nên ngưỡng kiểm tra được bằng số học tay.
Xem spec 2026-08-15-bot-trend2-nw-design.md.
"""

import pytest

from app.services import signal_service


def _bars(closes):
    return [
        {"time": 1735689600 + i * 86400, "open": c, "high": c + 1, "low": c - 1, "close": c}
        for i, c in enumerate(closes)
    ]


# Nến biên độ THAY ĐỔI theo từng cây: ranges[i] là high-low riêng của nến i.
# `_bars` ở trên luôn cho high-low = 2 → _wma(range,10) trả CÙNG một giá trị ở
# MỌI chỉ số, nên nếu _nw_trend lỡ đọc lệch chỉ số vào wma (vd. `wma[i - seed
# - 1]` thay vì `wma[i - seed]` đúng), rev vẫn ra đúng số một cách tình cờ và
# test không phát hiện được gì. Fixture này cho high-low khác nhau ở từng nến
# để mỗi chỉ số của _wma trả một giá trị riêng biệt — đọc lệch chỉ số sẽ đổi
# hẳn con số nw. open=close=100 và high/low đối xứng quanh 100 để HAC luôn
# đúng bằng 100 (khử biến "giá đổi"), nhờ vậy chỉ còn _wma làm nw thay đổi
# giữa các nến. ĐỪNG "đơn giản hoá" hàm này về biên độ cố định — làm vậy là
# xoá luôn bẫy bắt lỗi lệch chỉ số _wma mà review Task 2 (JS) đã tìm thấy
# bằng mutation testing (đổi wma[i-seed] -> wma[i-seed-1] mà 12/12 test cũ
# vẫn xanh). Soi gương barsFromRanges trong nwTrend.test.js.
def _bars_from_ranges(ranges):
    return [{"open": 100, "high": 100 + r / 2, "low": 100 - r / 2, "close": 100} for r in ranges]


class TestWma:
    def test_weights_favour_most_recent(self):
        # (1×1 + 2×2 + 3×3) / (1+2+3) = 14/6
        assert signal_service._wma([1, 2, 3], 3) == pytest.approx([14 / 6])

    def test_ten_period_series(self):
        assert signal_service._wma(list(range(1, 11)), 10) == pytest.approx([7.0])

    def test_index_mapping_matches_js(self):
        out = signal_service._wma([1, 2, 3, 4], 3)
        assert len(out) == 2
        assert out[0] == pytest.approx(14 / 6)
        assert out[1] == pytest.approx(20 / 6)

    def test_returns_empty_when_too_short(self):
        assert signal_service._wma([1, 2], 3) == []
        assert signal_service._wma([], 10) == []


class TestNwTrend:
    def test_warmup_is_none_until_index_9(self):
        out = signal_service._nw_trend(_bars([100] * 12))
        assert len(out) == 12
        assert all(x is None for x in out[:9])
        assert out[9] is not None

    def test_seeds_in_downtrend(self):
        out = signal_service._nw_trend(_bars([100] * 10))
        assert out[9]["trend"] == "down"
        assert out[9]["nw"] == pytest.approx(106.0)

    def test_flips_up_when_price_clears_nw(self):
        out = signal_service._nw_trend(_bars([100] * 10 + [120]))
        assert out[10]["trend"] == "up"
        assert out[10]["nw"] == pytest.approx(114.0)

    def test_nw_never_retreats_in_uptrend(self):
        out = signal_service._nw_trend(_bars([100] * 10 + [120, 118]))
        assert out[11]["trend"] == "up"
        assert out[11]["nw"] == pytest.approx(114.0)

    def test_flips_down_when_price_breaks_nw(self):
        out = signal_service._nw_trend(_bars([100] * 10 + [120, 110]))
        assert out[11]["trend"] == "down"
        assert out[11]["nw"] == pytest.approx(116.0)

    def test_equality_does_not_flip(self):
        out = signal_service._nw_trend(_bars([100] * 10 + [106]))
        assert out[10]["trend"] == "down"

    def test_all_none_when_fewer_than_ten_candles(self):
        out = signal_service._nw_trend(_bars([100] * 9))
        assert len(out) == 9
        assert all(x is None for x in out)

    def test_downtrend_hold_pins_min_not_max(self):
        """Soi gương JS "giữ xu hướng giảm (không lật) — NW phải là MIN, chốt số
        cụ thể" trong nwTrend.test.js. Cùng chuỗi nến, cùng số kỳ vọng.

        Nối tiếp nến mồi (down, NW=106) bằng một nến giảm giá thật close=90.
        HAC[10] = (90+91+89+90)/4 = 90, không vượt NW nên KHÔNG lật trạng thái.
        Nhánh giữ giảm tính: NW_mới = min(NW_cũ, HAC + rev) = min(106, 90 + 6)
        = min(106, 96) = 96.

        Nếu code lỡ đổi min → max ở nhánh này (bug mà review Task 2 phía JS
        tìm thấy — các test cũ chỉ assert `trend`, không assert `nw`), kết quả
        sẽ là max(106, 96) = 106 — khác 96, test này bắt được ngay.
        """
        out = signal_service._nw_trend(_bars([100] * 10 + [90]))
        assert out[10]["trend"] == "down"
        assert out[10]["nw"] == pytest.approx(96.0)

    def test_varying_range_pins_wma_index_alignment(self):
        """Soi gương JS "biên độ nến thay đổi theo thời gian — chốt NW bằng số,
        bắt lỗi lệch chỉ số wmaOf" trong nwTrend.test.js. Cùng chuỗi range,
        cùng số kỳ vọng.

        10 nến mồi có range lần lượt 1..10 → _wma(range,10)[0]
          = (1×1 + 2×2 + ... + 10×10) / 55 = 385/55 = 7
          (đúng giá trị đã chốt ở test_ten_period_series).
        rev[9] = 3×7 = 21 → NW mồi (down) = HAC[9] + 21 = 100 + 21 = 121.
        (HAC luôn = 100 vì open=close=100 và high/low đối xứng quanh 100.)

        Nến thứ 11 có range=1 → cửa sổ wma dịch sang [2,3,4,5,6,7,8,9,10,1]:
          wma[1] = (2×1+3×2+4×3+5×4+6×5+7×6+8×7+9×8+10×9+1×10) / 55
                 = (2+6+12+20+30+42+56+72+90+10) / 55 = 340/55
        HAC[10] = 100, không vượt NW cũ 121 nên KHÔNG lật, nhánh giữ giảm:
          NW = min(121, 100 + 3×(340/55)) = 100 + 3×(340/55) ≈ 118.545
          (vì 100 + 3×340/55 ≈ 118.545 nhỏ hơn 121).

        Nếu _nw_trend đọc `wma[i - seed - 1]` thay vì `wma[i - seed]` đúng,
        nến 11 sẽ lấy nhầm wma[0] = 7 (rev = 21) thay vì wma[1], ra
        NW = min(121, 100 + 21) = 121 — khác hẳn ~118.545 mà test này chốt.
        """
        ranges = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1]
        out = signal_service._nw_trend(_bars_from_ranges(ranges))

        assert out[9]["trend"] == "down"
        assert out[9]["nw"] == pytest.approx(121.0)

        assert out[10]["trend"] == "down"
        assert out[10]["nw"] == pytest.approx(100 + 3 * (340 / 55))
