# Contract `daily_return` — Asset → SDI

> **Mục đích:** chốt CHÍNH XÁC cách tính `daily_return` mà Asset gửi SDI, đặc biệt cho **ngày không giao dịch (T7/CN/lễ)** và **ngày có nạp/rút**. SDI **không tự tính** hiệu suất (thin-layer) — SDI **lưu + compound on-read** (`∏(1+r)−1`). Nên nếu `daily_return` nhiễm dòng tiền, **mọi số hiệu suất, deviation và tracking error của SDI đều sai**, và SDI không có nguồn nào khác để đối chiếu.
>
> Chốt 2026-07-11. Thay QĐ4 trong `SDI-asset-handover.md` ("Asset chỉ gửi ngày GD") — **Asset gửi MỌI NGÀY LỊCH (365)**.

---

## 1. Nguyên lý: `daily_return` là TWR — dòng tiền PHẢI bị khử

`daily_return` = lợi suất **thời-gian-trọng-số (TWR)** = phần thay đổi tài sản **do THỊ TRƯỜNG**, KHÔNG phải do khách nạp/rút.

**Nạp tiền KHÔNG phải là lãi. Rút tiền KHÔNG phải là lỗ.**

Cách khử chuẩn (unit/NAV-per-unit, prior-day pricing):

```
ΔU_t  = CF_t / UP_(t−1)                  ← unit phát hành/hủy theo giá unit HÔM TRƯỚC
U_t   = U_(t−1) + ΔU_t
UP_t  = AUM_t / U_t
r_t   = UP_t / UP_(t−1) − 1
```

Rút gọn đại số (khỏi cần lưu unit):

```
                AUM_t
r_t  =  ─────────────────────  −  1
         AUM_(t−1) + CF_t
```

| Ký hiệu | Nghĩa |
|---|---|
| `AUM_t` | Tài sản cuối ngày `t` (số Asset gửi) |
| `CF_t` | **Dòng tiền NGOÀI ròng** ngày `t` = `nạp − rút` (tiền của KH) |
| `r_t` | `daily_return` ngày `t` |

---

## 2. ⚠️ Bất biến quan trọng nhất — NGÀY KHÔNG GIAO DỊCH

**Ngày T7/CN/lễ: `daily_return` PHẢI = 0** (hoặc `NULL`), **bất kể có nạp/rút hay không.**

Lý do: sở đóng cửa ⇒ **không có định giá lại** ⇒ thị trường không tạo ra đồng lãi/lỗ nào. Toàn bộ thay đổi AUM ngày đó **chỉ đến từ dòng tiền**, và dòng tiền phải bị khử.

Kiểm chứng bằng công thức: ngày nghỉ `AUM_t = AUM_(t−1) + CF_t` ⇒

```
r_t = (AUM_(t−1) + CF_t) / (AUM_(t−1) + CF_t) − 1 = 0     ✅
```

**Sai lầm cần tránh** (đây là thứ SDI lo nhất):

```
r_t = AUM_t / AUM_(t−1) − 1        ❌  KHÔNG trừ CF ở mẫu số
```

Công thức sai này biến **một khoản nạp thành lợi nhuận**. Ví dụ thật ở §3.

---

## 3. Ví dụ số — nạp 5 triệu vào **thứ Bảy**

Trạng thái đóng cửa **thứ Sáu**: `AUM = 10,000,000` · `UP = 10,000` · `U = 1,000`

| Ngày | Thị trường | CF (nạp) | AUM cuối ngày | ✅ ĐÚNG `r` | ❌ SAI (`AUM_t/AUM_(t−1)−1`) |
|---|---|---|---|---|---|
| **T7** | đóng cửa | +5,000,000 | 15,000,000 | `15,000,000 / (10,000,000 + 5,000,000) − 1` = **0** | `15/10 − 1` = **+50%** |
| **CN** | đóng cửa | 0 | 15,000,000 | **0** | 0 |
| **T2** | +2% | 0 | 15,300,000 | `15,300,000 / 15,000,000 − 1` = **+2,00%** | +2,00% |

Kiểm tra bằng unit: T7 `ΔU = 5,000,000 / 10,000 = 500` → `U = 1,500` → `UP = 15,000,000 / 1,500 = 10,000` → **UP không đổi ⇒ r = 0** ✅

**Hậu quả nếu Asset dùng công thức sai:** hiệu suất kỳ của KH đó bị SDI compound thành `(1+0.5)(1+0)(1+0.02) − 1 = +53%` trong khi thực tế chỉ **+2%**. Kéo theo deviation vs danh mục mẫu và tracking error đều vô nghĩa.

---

## 4. Phí quản lý KHÔNG phải dòng tiền ngoài

Phí QL là **CHI PHÍ**, phải **làm giảm** hiệu suất (SDI serve hiệu suất **NET of fee**). Nạp/rút của KH thì bị khử; **phí thì KHÔNG được khử**.

⇒ **`cash_in` / `cash_out` Asset gửi chỉ được chứa DÒNG TIỀN CỦA KHÁCH** (nạp/rút/SIP). **KHÔNG được gộp khoản BO cắt phí QL vào `cash_out`.**

Ví dụ (thị trường phẳng, BO cắt phí 12,000 vào thứ Tư, `AUM` trước đó 15,300,000):

| Cách xử lý phí | `r` thứ Tư | Đúng/Sai |
|---|---|---|
| Coi phí là `cash_out` (bị khử) | `15,288,000 / (15,300,000 − 12,000) − 1` = **0** | ❌ hiệu suất GROSS — phí tàng hình |
| Coi phí là chi phí (KHÔNG khử) | `15,288,000 / 15,300,000 − 1` = **−0,0784%** | ✅ hiệu suất NET |

Nếu hệ thống Asset **buộc** phải đưa khoản cắt phí vào `cash_out`, thì phải gửi kèm trường riêng (vd `fee_paid`) để hai bên loại nó ra khi tính `r`.

---

## 5. Quy ước thời điểm dòng tiền (ngày GD có nạp/rút)

Công thức §1 (`mẫu số = AUM_(t−1) + CF_t`) = **tiền được coi như có mặt từ ĐẦU phiên** (unit phát theo `UP_(t−1)`) ⇒ tiền nạp **chịu** biến động thị trường ngày đó.

Nếu thực tế tiền về **sau giờ khớp lệnh** (không kịp đầu tư), quy ước đúng hơn là **cuối phiên**:

```
r_t = (AUM_t − CF_t) / AUM_(t−1) − 1
```

**Hai bên PHẢI dùng CÙNG một quy ước.** Với ngày nghỉ thì **cả hai công thức đều ra `r = 0`**, nên bất biến ở §2 **không phụ thuộc** lựa chọn này. Đề nghị: chốt **đầu phiên** (unit prior-day) vì khớp với phương pháp unit đã thống nhất trước đây.

---

## 6. SDI sẽ tự động ĐỐI SOÁT — Asset cần biết

SDI có đủ đầu vào (`AUM_t`, `AUM_(t−1)`, `cash_in`, `cash_out`) để **tự tính lại** `r_t` và so với số Asset gửi. Reconcile chạy trong EOD, lệch → ghi `T_EOD_RECON_BREAK` → **chặn publish**:

| Check | Điều kiện | Kỳ vọng |
|---|---|---|
| `NONTRADING_RETURN` | ngày `UDF_IS_BUSINESS_DATE = 0` | `daily_return` = 0 hoặc NULL |
| `RETURN_MISMATCH` | mọi ngày | \|`daily_return`_Asset − `AUM_t/(AUM_(t−1)+CF_t) − 1`\| ≤ ε |
| `CASHFLOW_MISMATCH` | mọi ngày | `cash_in − cash_out` (Asset) = `Σ` cashflow SDI tự ghi (cùng value date) |

⇒ Nếu Asset lỡ dùng công thức sai, SDI **phát hiện ngay ngày đầu tiên**, không âm thầm sai hàng tháng.

---

## 7. Tóm tắt yêu cầu gửi Asset

1. Gửi **mọi ngày lịch (365)**: `aum`, `cash` (tổng), `cash_available` (khả dụng), `daily_return`, `cash_in`, `cash_out`.
2. **Ngày T7/CN/lễ: `daily_return` = 0** (hoặc NULL). **Tuyệt đối không** dùng `AUM_t/AUM_(t−1) − 1`.
3. `daily_return` = TWR đã **khử dòng tiền**: `AUM_t / (AUM_(t−1) + CF_t) − 1`.
4. `cash_in`/`cash_out` = **CHỈ dòng tiền của khách** (nạp/rút). **KHÔNG gộp phí QL bị cắt**.
5. `cash_in`/`cash_out` ghi theo **value date thật** (tiền về T7 thì ghi ngày T7, không dồn sang T2).
