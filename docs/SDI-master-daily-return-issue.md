# Cần chốt: trọng số trong công thức "Hiệu suất danh mục tổng khách hàng"

> **Gửi:** nhóm BRD / nghiệp vụ
> **Người nêu:** team kỹ thuật SDI · **Ngày:** 2026-07-13
> **Việc cần:** xác nhận cách hiểu một ký hiệu trong công thức đã ban hành. **Code hiện tại làm ĐÚNG spec** — vấn đề nằm ở chỗ spec chưa nói rõ, và cách hiểu đang dùng cho ra số **lệch có hệ thống**.

---

## 1. Vấn đề trong một câu

Spec định nghĩa lợi suất ngày của danh mục tổng KH là **bình quân gia quyền theo AUM**, nhưng **không nói AUM lấy ở thời điểm nào**. Engine đang lấy **AUM cuối ngày**. Về toán tài chính, trọng số phải là **vốn đầu ngày**. Hệ quả: đường hiệu suất mà **khách hàng và PM đang nhìn bị thổi phồng**, và méo mạnh nhất đúng vào những ngày có khách nạp tiền lớn.

---

## 2. Spec hiện đang viết

`SDI-spec.md` (dòng 142, 240, 263) và `SDI-thuat-ngu-cong-thuc.md` (dòng 156) đều ghi:

```
Master daily return  =  AUM-weighted   Σ(AUMᵢ · rᵢ) / Σ AUMᵢ
```

- `rᵢ` = lợi suất ngày của khách hàng i (do hệ Asset cung cấp, đã khử nạp/rút)
- `AUMᵢ` = ❓ **spec không nói ở thời điểm nào** — đầu ngày hay cuối ngày?

Engine chọn **AUM cuối ngày** (số có sẵn trong tay tại thời điểm chạy EOD). **Code không sai spec** — spec thiếu một chữ.

---

## 3. Vì sao "AUM cuối ngày" cho ra số sai

Lợi suất của một ngày được sinh ra trên **số vốn có mặt lúc bắt đầu ngày**. AUM cuối ngày = vốn đó **cộng thêm chính khoản lãi/lỗ vừa phát sinh**. Lấy nó làm trọng số là **lấy kết quả đi cân chính nó**.

### Cơ chế 1 — người lãi tự tăng trọng số cho mình

Danh mục có 2 khách, **vốn bằng nhau, không ai nạp rút**:

| | Vốn đầu ngày | Lợi suất ngày | AUM cuối ngày |
|---|---|---|---|
| Khách A | 1 tỷ | **+20%** | 1,2 tỷ |
| Khách B | 1 tỷ | **−20%** | 0,8 tỷ |
| **Tổng** | **2 tỷ** | ? | **2,0 tỷ** |

Tiền vào 2 tỷ, tiền ra 2 tỷ → **thực tế hoà vốn: 0%**.

| Cách tính | Trọng số | Kết quả |
|---|---|---|
| **Đang dùng** (AUM cuối ngày) | A = 1,2/2,0 = **60%** · B = **40%** | `0,6×20% + 0,4×(−20%)` = **+4%** ❌ |
| **Đề xuất** (vốn đầu ngày) | A = **50%** · B = **50%** | `0,5×20% + 0,5×(−20%)` = **0%** ✅ |

Khách A được **60% tiếng nói chỉ vì khách A lãi**. Khách lỗ bị bóp trọng số xuống.

⚠️ Đây là **thiên lệch một chiều** (luôn dương), không phải sai số ngẫu nhiên. Vì con số được nhân dồn qua các ngày nên nó **tích lũy dần** trên biểu đồ.

*Mức độ:* sai số mỗi ngày ≈ **độ phân tán lợi suất giữa các khách trong ngày đó**. Khách trong cùng danh mục đều bám một danh mục mẫu ⇒ phân tán nhỏ ⇒ sai số nhỏ (cỡ vài phần mười % một năm). **Đây là vấn đề về nguyên tắc hơn là về con số.**

### Cơ chế 2 — nạp/rút làm công thức vỡ hẳn ⚠️ *(nghiêm trọng)*

| | Vốn đầu ngày | Nạp trong ngày | Lợi suất | AUM cuối ngày |
|---|---|---|---|---|
| Khách A | 1 tỷ | 0 | **+10%** | 1,1 tỷ |
| Khách B | 1 tỷ | **+9 tỷ** *(tiền mới, chưa đầu tư)* | **0%** | 10 tỷ |
| **Tổng** | **2 tỷ** | +9 tỷ | ? | 11,1 tỷ |

Thực tế: vốn 2 tỷ, lãi 100 triệu → **+5%**.

| Cách tính | Trọng số | Kết quả |
|---|---|---|
| **Đang dùng** | A = 1,1/11,1 = **9,9%** · B = **90,1%** | `0,099×10% + 0,901×0%` = **+0,99%** ❌ |
| **Đề xuất** | A = **50%** · B = **50%** | **+5%** ✅ |

**Sai gấp 5 lần.** Vì 9 tỷ tiền mới toanh — chưa đầu tư một ngày nào — được gán **90% trọng số** cho lợi suất của ngày hôm đó.

Và sai **theo cả hai chiều**: khách đang lỗ mà nạp thêm tiền thì con số lại bị kéo **lên**. Không kiểm soát được.

> **Danh mục càng hút vốn mạnh thì con số càng méo.**

---

## 4. Con số này đang hiển thị ở đâu

| Màn hình | Cách dùng | Hệ quả |
|---|---|---|
| **Biểu đồ của khách hàng** | Nhân dồn cả chuỗi → đường *"danh mục tổng KH"* | Sai **tích lũy** theo thời gian |
| **Biểu đồ dashboard PM** | Nhân dồn cả chuỗi → đường *"Composite KH"* | Sai tích lũy |
| **Màn hình chi tiết tiểu khoản** | Lấy lát mới nhất | Sai của 1 ngày |

⇒ **Cả khách hàng lẫn PM đang nhìn một đường hiệu suất bị thổi phồng.**

---

## 5. Công thức đề xuất

Trọng số phải là **số vốn thực sự sinh ra lợi suất đó** — chính là mẫu số mà hệ Asset dùng khi tính `rᵢ`:

```
vốnᵢ,d   =  AUMᵢ(ngày trước)  +  nạpᵢ,d  −  rútᵢ,d

Master daily return  =  Σ( vốnᵢ · rᵢ ) / Σ vốnᵢ
```

**Điểm hay:** khi cân theo vốn, công thức **rút gọn hoàn hảo** thành lợi suất thật của cả túi tiền:

```
Σ(vốnᵢ · rᵢ) / Σ vốnᵢ   =   ( AUM_tổng cuối ngày − vốn_tổng ) / vốn_tổng   =   lãi / vốn
```

Kiểm lại cơ chế 2: `(11,1 − 2 − 9) / 2 = 0,1/2` = **+5%** ✓

⇒ Cho phép **đối soát chéo miễn phí**: tính từ dưới lên (gộp từng khách) phải bằng tính từ trên xuống (lấy tổng danh mục). Lệch nhau ⇒ có lỗi dữ liệu ⇒ hệ thống tự báo.

**Không cần đổi cấu trúc dữ liệu.** Cả 3 số (`AUM ngày trước`, `nạp`, `rút`) đã được lưu sẵn. Chỉ sửa bước tổng hợp cuối ngày.

---

## 6. Quyết định cần từ nghiệp vụ

| | Phương án | Hệ quả |
|---|---|---|
| **A** *(đề xuất)* | Chốt `AUMᵢ` trong spec = **vốn đầu ngày** (`AUM ngày trước + nạp − rút`) | Số đúng chuẩn ngành. Có thêm đối soát chéo tự động. **Đường hiệu suất lịch sử sẽ đổi số** (thấp xuống) — cần thông báo nếu đã công bố cho khách |
| **B** | Giữ nguyên **AUM cuối ngày** | Không phải tính lại lịch sử, nhưng số **lệch có hệ thống** và không đối soát được. Cần ghi rõ giới hạn này vào tài liệu để sau này không ai giật mình |

---

## 7. Phụ lục — hai chuyện liên quan, cũng cần biết

**(a) Hai con số cùng tên "Hiệu suất DM tổng KH".** Glossary ghi rõ KPI trên dashboard PM dùng **end-weight** (`Σ Wᵢ·Rᵢ` với `Wᵢ` = AUM **cuối kỳ**) — đây là **lựa chọn có chủ đích**, không phải lỗi. Nhưng nó **khác** với đường biểu đồ (nhân dồn chuỗi lợi suất ngày). ⇒ **Ô số KPI và điểm cuối của đường biểu đồ sẽ không khớp nhau.** Cần chốt: hai chỗ dùng chung một cách tính, hay giữ hai chỉ tiêu riêng với **hai tên gọi khác nhau**?

**(b) Phụ thuộc một quy ước chưa chốt với hệ Asset.** `vốnᵢ = AUM ngày trước **+ nạp − rút**` giả định tiền nạp trong ngày **được đầu tư ngay từ đầu phiên**. Nếu Asset quy ước tiền về **sau giờ khớp lệnh** (không kịp đầu tư) thì `vốnᵢ = AUM ngày trước` (không cộng dòng tiền). **Hai bên phải dùng chung một quy ước** — hiện đang chờ Asset xác nhận (xem `SDI-daily-return-contract.md`).
