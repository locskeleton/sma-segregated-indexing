# SDI — Từ điển khái niệm & Mối tương quan (Asset / Performance Engine)

> Tài liệu giải thích **toàn bộ khái niệm** liên quan đến tính tài sản & hiệu suất trong hệ thống SDI,
> mối quan hệ giữa chúng, và công thức chuẩn (kèm cảnh báo nơi BRD lệch chuẩn ngành).
>
> Phiên bản: **v0.2** — ngày 2026-06-15. Nguồn: BRD WS3.1_SDI_BUSINESS + file Excel mẫu (index DM mẫu, PnL danh mục KH) + đối chiếu chuẩn fund accounting / index methodology.
>
> ⚠️ Ký hiệu:
> - ✅ = khớp chuẩn / **đã chốt**
> - ⚠️ = cần chốt với BO/FO
> - ❌ = BRD lệch chuẩn, đã có phương án sửa
>
> **Changelog v0.1 → v0.2 (các quyết định đã chốt):**
> - **Mô hình = SMA**: segregated custody (tiền/CP nằm thật trong **tiểu khoản** KH) + thực thi gộp lệnh. KHÔNG phải pooled fund. (§1, §9)
> - **Tiểu khoản = (KH × SI)**; một KH có nhiều tiểu khoản (tham gia nhiều SI cùng lúc). (§1)
> - **Unit Price tính RIÊNG từng KH** từ NAV riêng + cashflow riêng — KHÔNG dùng SI Unit Price chung. (§4, §8) → đảo ngược plan §7.
> - **Historic pricing, chốt 1 lần cuối ngày (point B)**: ΔUnit quy đổi tại **Unit Price ngày t-1** — giữ theo BRD gốc + file mẫu; `net_cashflow = ΔUnit × Unit Price_(t-1)`. (§4)
> - **NAV do SDI engine TÍNH** (xử lý tiền pending + trừ phí), không đọc raw giá trị tiểu khoản. (§2)
> - **SI Index**: tính EOD-only, rebalance hiệu lực tại close; rebalance là execution → lệch SI vs index là tracking error hợp lệ. (§6)
> - **"Lãi Infy" = cash-in** (tiền KH bơm vào). (§3)
> - **D — CHỐT GIỮ NGUYÊN (PR, không đổi)**: danh mục mẫu (PR) vs VN-Index (PR) cùng cơ sở → đúng. Gap KH/SI (TR) vs benchmark (PR) **chấp nhận** vì user quen so với VN-Index. Known accepted artifact. (§7)
> - **E — CHỐT: implement CẢ TWR và MWR** (TWR = hiệu suất chiến lược + chart; MWR = "lợi suất của bạn", Modified Dietz). (§5.3, §5.4)
> - Còn treo: **F** (accrue phí daily), chi tiết lô lẻ/độ trễ giải ngân. (§12)

---

## 0. Bản đồ khái niệm (đọc trước)

Có **2 thế giới song song**, đừng trộn lẫn:

```
┌─────────────────────────────────────────────────────────────────────┐
│  THẾ GIỚI 1 — TIỀN THẬT (per Khách hàng × SI)                         │
│  Tài sản thật trong tiểu khoản → NAV (SDI tính) → Unit / Unit Price   │
│  → Hiệu suất TỪNG KH (NAV-per-share, historic pricing t-1)            │
└─────────────────────────────────────────────────────────────────────┘
                              ▲  so sánh trên cùng 1 chart (FR-03)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  THẾ GIỚI 2 — LÝ THUYẾT (chỉ số benchmark)                            │
│  Danh mục mẫu (weights) → SI Index → VN-Index                         │
│  (đo bằng phương pháp chỉ số, KHÔNG có tiền thật)                     │
└─────────────────────────────────────────────────────────────────────┘
```

- **TG1** trả lời: "Tiền của KH trong SI này đang lãi/lỗ bao nhiêu?" → tính **per (KH × SI)**.
- **TG2** trả lời: "Chiến lược mẫu lý thuyết hiệu suất bao nhiêu? Thị trường thế nào?"
- Chart FR-03 đặt 3 đường: **Hiệu suất SI** (tổng hợp TG1) vs **Danh mục mẫu** (TG2) vs **VN-Index** (TG2).

---

## 1. Khái niệm SẢN PHẨM & cấu trúc tài khoản

| Khái niệm | Định nghĩa |
|---|---|
| **SI / SDI / DMUT** | "Quỹ chỉ số" KH tham gia. BRD gọi lẫn lộn (SDI01, DMUT, SI) → ⚠️ nên chốt 1 thuật ngữ. |
| **Tiểu khoản (sub-account)** | Đơn vị nhỏ nhất, **= một (KH × SI)**. KH chuyển tiền đầu tư vào từng tiểu khoản theo từng SI. |
| **Một KH nhiều tiểu khoản** | KH có thể tham gia nhiều SI cùng lúc → nhiều tiểu khoản. Granularity tính toán = **(KH × SI)**. |
| **Danh mục mẫu (model portfolio)** | Bộ **mã + tỷ trọng mục tiêu w_i** định nghĩa chiến lược → cơ sở tính **SI Index**. KHÁC holdings thật. |
| **Holdings thực tế** | CP đang nắm thật trong tiểu khoản. Dùng cho FR-05. |
| **Rebalance** | Đổi tỷ trọng/cấu phần danh mục mẫu. Cho phép **1–2 lần/ngày**. Là **execution** (giữ target weights). |

### Mô hình thực thi: SMA (Separately Managed Account)

```
KH chuyển tiền vào tiểu khoản (theo SI)
        │
        ▼
SDI rebalance model (weights mới)  ──yêu cầu rebalance──►  FO
                                                            │ FO GOM lệnh toàn bộ KH trong SI
                                                            │ FO đẩy lệnh MP vào sàn
                                                            │ FO ALLOCATE khớp về từng tiểu khoản
        ┌──── execution feed (mã, KL, giá khớp MP) ◄────────┘
        ▼
Tiền + cổ phiếu nằm THẬT trong tiểu khoản KH (KH là chủ sở hữu)
        │
        ▼
SDI dựng holdings KH từ execution feed → tính NAV
```

- **Custody = segregated**: tài sản tách bạch theo tiểu khoản, KH sở hữu hợp pháp.
- **Execution = aggregated, do FO làm** (O4): SDI gửi yêu cầu rebalance; **FO tự gom lệnh + đẩy MP + allocate**. SDI **không sinh/khớp lệnh**, chỉ tiêu thụ execution feed.
- → KHÔNG phải pooled fund (không có quỹ gộp sở hữu chung). Xem §9.

---

## 2. Khái niệm TÀI SẢN (Asset) — TG1

Thứ tự: **Giá trị thành phần → Tổng tài sản → NAV**.

| Khái niệm | Công thức | Nguồn |
|---|---|---|
| **Chứng khoán** | `Σ (KL × market price)` | FO |
| **Tiền** | `Tiền mặt + tiền bán chờ về (T0+T1+T2) + cổ tức tiền` | FO |
| **Tổng tài sản** | `Tiền + Chứng khoán` | FO |
| **Phí phải trả** | `Phí lưu ký + phí quản lý AUM` (lũy kế) | FO + SDI |
| **NAV** | `Tổng tài sản − Phí phải trả` | **SDI tính** |
| **Tổng vốn đầu tư** | `Σ NAV vào − Σ NAV ra` | SDI |

> ✅ **Chốt:** NAV dùng cho tính hiệu suất **do SDI engine TÍNH**, KHÔNG đọc thẳng "giá trị thô của tiểu khoản".
> Lý do: giá trị thô trộn **tiền chờ giải ngân (chưa đầu tư), tiền mua chờ khớp, tiền bán chờ về (T+)**, và **phí quản lý AUM do SDI tính** (chưa có trong số FO). Engine phải chuẩn hóa rồi mới ra NAV.
> ✅ **Chốt methodology (O6):** tiền chờ giải ngân **vào NAV + phát unit NGAY tại ngày nộp** (giá t-1, historic); clock hiệu suất chạy từ ngày nộp. Mấy ngày chờ khớp → cash drag nằm trong tiểu khoản của **chính KH đó** (segregated → công bằng). **Trade-date accounting**: mua/bán ghi nhận tại ngày khớp (giá MP), tiền mua chờ khớp / bán chờ về tracked ở cash sub-ledger.
> ⚠️ Còn hỏi: **FO** — nộp T → khớp T+? & cadence gom batch (độ lớn cash drag); **BO** — clock từ ngày nộp hay ngày khớp (khuyến nghị: ngày nộp).

**FR-06 (báo cáo cấu phần tài sản)** — 3 nhóm: [A] Thông tin tài khoản (sức mua, tiền mặt/tài sản có thể rút, tiền mua CK, tiền bán chờ về, cổ tức tiền), [B] Tài sản thực tế (tiền, CK), [C] Khoản phải trả (phí phải trả). Vì tiểu khoản tách theo SI nên FR-06 per SI lấy từ FO hợp lý — nhưng cột NAV/phí cuối là **SDI tính**.

---

## 3. CASHFLOW vs INCOME (ranh giới quyết định tính đúng)

| Loại | Là gì | Vào đâu | Phát hành Unit? |
|---|---|---|---|
| **NAV vào (cash in)** | **Tiền KH bơm từ ngoài**: nộp lần đầu, nộp thêm, SIP, **lãi Infy** (tiền KH đem đi đầu tư) | External cashflow | ✅ Có |
| **NAV ra (cash out)** | **Tiền KH rút** về TK KH | External cashflow | ✅ Có (âm) |
| **Income** | **Cổ tức/lãi do CHÍNH tài sản sinh ra** (cổ tức CP trong rổ, lãi tiền gửi) | Là **PnL**, chảy vào NAV | ❌ Không |
| **Không cashflow** | Phí quản lý, phí phạt rút sớm | Là **chi phí**, giảm NAV | ❌ Không |

> ✅ **Chốt:** "Lãi Infy" = tiền KH bơm vào → đúng là **cash in**. FO **có nhãn nguồn đầy đủ** (DEPOSIT/SIP/WITHDRAW/DIVIDEND/INTEREST...) → tách được.
>
> ❌→✅ **Sửa công thức "Tiền" (lump):** BRD gộp `Tiền = tiền mặt + bán chờ về + cổ tức` thành 1 tổng. **TỔNG chỉ dùng để tính NAV.** TUYỆT ĐỐI KHÔNG lấy **Δ(tổng tiền)** làm cashflow — vì gộp lẫn KH nạp + cổ tức + tiền bán CP → phát unit sai, PnL sai.

| Dùng vào | Lấy từ |
|---|---|
| NAV / Tổng tài sản | **Tổng tiền (gộp OK)** |
| **CF (unit)** | **CHỈ event nhãn DEPOSIT/SIP/WITHDRAW** — không decompose từ tổng |
| Income (cổ tức/lãi) | event nhãn DIVIDEND/INTEREST → vào NAV, KHÔNG vào CF → tự vào PnL |
| FR-06 (sức mua, tài sản có thể rút) | **components typed** (chờ giải ngân, phong tỏa, mua chờ khớp, bán chờ về) |

> **Nguyên tắc:** giữ **cash sub-ledger theo nhãn**; tổng chỉ để cộng NAV. Reconcile mỗi ngày: `Δ tổng tiền = Σ(nạp/rút) + Σ(cổ tức/lãi) + (bán − mua khớp lệnh)`.

---

## 4. UNIT (NAV-per-share) — trái tim hiệu suất, tính PER KH

Mỗi tiểu khoản (KH × SI) có **chuỗi NAV riêng, cashflow riêng → Unit & Unit Price RIÊNG**.

### 4.1 Công thức chuẩn — Historic pricing (t-1), chốt 1 lần cuối ngày ✅

```
T0:  Unit Price_0 = 10.000
     Unit_0       = NAV_0 / 10.000

Tn (HISTORIC pricing, chốt EOD, CF = NAV vào − NAV ra gom trong ngày):
     ΔUnit_t      = CF_t / Unit Price_(t-1)               # quy đổi tại giá NGÀY HÔM TRƯỚC
     Unit_t       = Unit_(t-1) + ΔUnit_t
     Unit Price_t = NAV_cuối_t / Unit_t                   # giá EOD ngày t
```

> ✅ **Chốt point B = HISTORIC pricing** (giá `t-1`) — **giữ theo BRD gốc + file mẫu**.
> Hệ quả: `net_cashflow = ΔUnit × Unit Price_(t-1)` (nhất quán với cách phát unit).
> ⚠️ %PnL hơi nhạy với cashflow trên ngày có nạp/rút (noise nhỏ ~±1pp), **NAV/tiền không đổi** — chấp nhận. (Forward pricing là lựa chọn thay thế đã cân nhắc nhưng KHÔNG dùng.)

> ⚠️ **Implementation bắt buộc:** **Unit lưu full precision (thập phân)**, chỉ làm tròn khi hiển thị. Lưu integer → unit price drift sai. (Chứng minh: file mẫu hiển thị Unit=1,127 nhưng tính bằng 1,126.667 — xem §14.)

### 4.2 Tính chất per-KH (KHÔNG dùng giá chung)

- File Excel mẫu (PnL danh mục KH) tính Unit Price **riêng từng KH** từ NAV + cashflow của KH đó → **chốt per-KH**.
- ❌ Plan §7 ("mọi KH dùng SI Unit Price chung") **SAI** với mô hình SMA segregated → bỏ.
- Mỗi KH có tracking error riêng (cash drag, lô lẻ, thời điểm vào batch) → phải phản ánh, không san đều.

---

## 5. HIỆU SUẤT & PnL — TG1

### 5.0 Phân biệt thuật ngữ (ĐỌC KỸ — các từ này chồng lấn nhau)

7 thuật ngữ hay lẫn, thực ra chỉ chia **4 NHÓM bản chất**. Nắm nhóm là hết rối:

| Nhóm | Thuật ngữ | Đơn vị | Phạm vi | Trả lời câu hỏi |
|---|---|---|---|---|
| **① TIỀN** (tuyệt đối) | **PnL** | VND | ngày / kỳ | "Lãi/lỗ bao nhiêu **ĐỒNG**?" |
| **② % LỢI SUẤT** (tương đối) | **daily return** | % | **1 ngày** | "Hôm nay so hôm qua **±%**?" |
| | **%return** | % | **1 kỳ** | "Cả kỳ lãi **bao nhiêu %**?" |
| | **%PnL** | % | **1 kỳ** | (= %return — **CÙNG một thứ, tên khác**) |
| **③ PHƯƠNG PHÁP** tính % | **TWR** | (ra %) | kỳ | "% kiểu **bỏ qua nạp/rút** (đo quỹ)" → **chính là %PnL của hệ** |
| | **MWR** | (ra %) | kỳ | "% **tiền thật của KH**, có tính timing nạp/rút" → **KHÁC TWR** (đã chốt implement — §5.4) |
| **④ CÔNG CỤ** | **unit price** | VND/unit | mỗi ngày | "Cái **thước** để đo %" — KHÔNG phải con số lợi suất |

#### Trực giác từng cái (analogy)

- **PnL (tiền)** = "tao lãi **10 triệu đồng**" — số tiền thật, cộng được.
- **daily return** = "**hôm nay +2%**" — lợi suất của riêng 1 ngày.
- **%return = %PnL** = "**cả năm +15%**" — lợi suất gộp cả kỳ. Hai tên, một nghĩa.
- **TWR** = điểm chấm **phong độ của quỹ** — không quan tâm KH bỏ nhiều hay ít tiền, lúc nào. → **đây chính là %PnL/%return ta đang dùng.**
- **MWR** = lợi suất **cái ví của KH** — bỏ tiền đúng đáy thì cao, bỏ đúng đỉnh thì thấp. Phụ thuộc timing.
- **unit price** = "giá 1 cổ phần ảo" — chỉ là **thước đo**; bản thân nó không phải "lợi suất" để hiển thị.

#### Sơ đồ quan hệ

```
① PnL tiền/ngày ──(r_t = PnL_t/NAV đầu_t)──► ② daily return  (TWR 1 ngày)
                                                   │ nhân dồn Π(1+r)
                                                   ▼
                                          ② %return = %PnL    (TWR cả kỳ)
                                                   ▲
                                          đo bằng ④ unit price (UP cuối/UP đầu − 1)

   ③ %PnL của hệ = TWR.   TWR ≠ MWR (MWR = % tiền thật KH, phụ thuộc nạp/rút).
```

#### Quy tắc "muốn nói X → dùng từ Y"

| Mày muốn diễn đạt... | Dùng từ |
|---|---|
| Lãi/lỗ bao nhiêu **tiền** | **PnL** (VND) |
| Lãi bao nhiêu **%** trong 1 kỳ | **%PnL** = **%return** |
| % của **riêng 1 ngày** | **daily return** |
| Nhấn mạnh "% này **đã bỏ ảnh hưởng nạp/rút**, đo quỹ" | **TWR** (= %PnL của hệ) |
| "% **tiền thật KH** lãi, có tính lúc nạp" | **MWR** (đã chốt implement — §5.4) |
| Cái thước/chỉ số nội bộ để tính | **unit price** |

#### Một ví dụ — cùng 1 KH, cùng kỳ KT→ngày 7 (sample BRD historic) cho ra cả 4 con số

| Con số | Giá trị | Loại |
|---|---|---|
| **PnL** | **+10,250,000 đ** | ① tiền cả kỳ |
| **daily return** ngày 6 | **+25%** | ② % của 1 ngày |
| **%return = %PnL = TWR** | **+90.82%** | ② = ③ % cả kỳ (UP_7/UP_0 − 1 = 19,082/10,000−1) |
| **MWR** (nếu tính) | **< 90.82%** | ③ vì KH nạp phần lớn tiền MUỘN, không hưởng cú tăng đầu |

→ Cùng một KH một kỳ vẫn có nhiều "con số" hợp lệ khác nhau — **vì chúng trả lời câu hỏi khác nhau.**

#### 3 bẫy hay mắc

1. **PnL (tiền) ≠ %PnL.** Một cái là đồng, một cái là %. Đừng gọi lẫn.
2. **%PnL cả kỳ ≠ Σ daily return.** Phải **nhân dồn** `(1+r)`, không cộng. (`+10% rồi +10% = +21%`, không phải 20%.)
3. **%PnL (TWR) ≠ "tổng PnL tiền / vốn".** Cái sau bỏ compound + sai base, nó gần **MWR**, KHÔNG phải %PnL của hệ.

### 5.1 PnL (bằng TIỀN)

| Khái niệm | Công thức |
|---|---|
| **PnL 1 ngày** | `NAV cuối − NAV đầu + NAV ra − NAV vào` ✅ (cashflow-neutral) |
| **PnL cả kỳ** | `Σ PnL các ngày trong kỳ` |

### 5.2 Return (bằng %)

| Khái niệm | Công thức |
|---|---|
| **%PnL (TWR)** | `(Unit Price cuối kỳ / Unit Price đầu kỳ − 1) × 100%` |
| **Daily return** | `Unit Price_t / Unit Price_(t-1) − 1` |
| **YTD** | từ 01/01 năm hiện tại |

> ⚠️ **Nhất quán khung kỳ:** %PnL và PnL-tiền của cùng một nhãn kỳ phải dùng **cùng mốc bắt đầu**.
> (File mẫu BRD bị lệch: %PnL lấy gốc cuối-ngày-3 nhưng PnL-tiền cộng cả ngày 3 — xem §14. Phải chốt: kỳ bắt đầu từ **cuối ngày trước** rồi áp đồng nhất cho cả hai.)

### 5.3 TWR vs MWR — ✅ điểm E CHỐT: implement CẢ HAI

| | **TWR** (qua Unit Price) | **MWR** (money-weighted, dòng tiền KH) |
|---|---|---|
| Đo | Hiệu suất loại bỏ timing nạp/rút | Lợi suất **tiền thật** của KH |
| Phụ thuộc timing CF? | ❌ Không | ✅ Có |
| Dùng cho | "Hiệu suất chiến lược/SI" + **chart so benchmark FR-03** | **"Lợi suất của bạn"** (FR-01/02 headline KH) |
| Công thức | `UnitPrice cuối/đầu − 1` (§5.2) | Modified Dietz (mặc định) / XIRR (chính xác) — §5.4 |

> ✅ **Chốt E:** hiển thị **cả hai, gán nhãn rõ** — "Hiệu suất chiến lược" = TWR; "Lợi suất của bạn" = MWR. Tránh KH SIP hiểu nhầm.
> ⚠️ Chart FR-03 (so benchmark) **bắt buộc dùng TWR** (mới so sánh công bằng). MWR chỉ cho con số cá nhân KH.

### 5.4 MWR — công thức (per KH × SI, per range)

**Modified Dietz** (mặc định — closed-form, không cần lặp, chuẩn GIPS cho money-weighted):
```
MWR(range) = (NAV_cuối − NAV_đầu − CF_ròng) / (NAV_đầu + Σ_i w_i · CF_i)

  CF_ròng = Σ_i CF_i                  (tổng net flow trong kỳ)
  CF_i    = net flow ngày t_i         (nạp +, rút −)
  w_i     = (T − t_i) / T             (trọng số thời gian; t_i = số ngày từ đầu kỳ, T = độ dài kỳ)
```
- **Tử số** = `NAV_cuối − NAV_đầu − CF_ròng` = **PnL tiền cả kỳ** (lãi/lỗ thật).
- **Mẫu số** = **vốn bình quân gia quyền thời gian** (vốn đầu kỳ + các flow tính theo thời gian nằm trong quỹ).

**XIRR** (tùy chọn, chính xác — iterative): giải `r` sao cho
```
NAV_đầu·(1+r)^T + Σ_i CF_i·(1+r)^(T−t_i) = NAV_cuối
```
> Khuyến nghị: **Modified Dietz** cho hiển thị (robust, không lỗi hội tụ); XIRR cho sao kê chính xác nếu cần. Hiển thị **period return** (không annualize) để đồng bộ với TWR; annualize riêng nếu BO yêu cầu.

**Inputs đều derive-on-read:** NAV 2 đầu mút + cashflow events (có ngày) trong range. Không cần bảng daily mới.

---

## 6. SI INDEX (chỉ số danh mục mẫu) — TG2

> ⚠️ KHÔNG phải hiệu suất tiền thật của SI. Đây là **benchmark lý thuyết** của danh mục mẫu.
> "Hiệu suất SI" (đường 1 chart) đến từ **Unit Price** (§4), không phải Index này.

### 6.1 Công thức

```
Index_0 = 1000
Index_t = Index_(t-1) × Σ_i ( w_i × P_i,t / P_ref_i )
```
- `w_i` = tỷ trọng mục tiêu danh mục mẫu (đang hiệu lực); `P_i,t` = giá đóng cửa; `P_ref_i` per-mã.
- `P_ref_i` = giá đóng cửa hôm trước (bình thường) | giá tham chiếu điều chỉnh (khi có quyền).
- Daily factor `= 1 + Σ w_i·r_i` (bình quân gia quyền lợi suất từng mã).

### 6.2 Rebalance — ✅ chốt EOD-only

- **Không tính lại index khi rebalance intraday.** Tính **1 lần cuối ngày, close-to-close**.
- Quy ước: rebalance **hiệu lực tại close**. Lợi suất ngày t dùng weights tại close (t-1); rebalance trong ngày t set weights mới cho t+1.
- Rebalance **1 hay 2 lần/ngày** → chỉ **weights NET cuối ngày** vào công thức. KHÔNG cần giá intraday, không chain sub-period.
- Rebalance là **execution** → SI thật lệch index = **tracking error hợp lệ** (đúng cái FR-03 muốn show).
- Bảng weights lưu theo **`effective_date` mức NGÀY** (không cần timestamp).

### 6.3 Ví dụ số (khớp Excel BRD)

| Ngày | Tỷ trọng | P_ref | Giá đóng cửa | Index |
|---|---|---|---|---|
| 0 | A40 B35 C25 | — | 100/50/80 | **1000** |
| 1 | A40 B35 C25 | 100/50/80 | 102/49/82 | **1007** |
| 2 | A40 B35 C25 | 102/49/82 | 101/50.5/81 | **1011** |
| 3 (rebalance: −C +D) | A45 B35 D20 | 101/50.5/60 | 103/52/62 | **1037** |

### 6.4 Sự kiện quyền: dùng `P_ref` điều chỉnh để giá rớt do quyền không bị hiểu là lỗ; xử lý quyền trước, rồi áp weights.

---

## 7. BENCHMARK & so sánh (FR-03) — ⚠️ điểm D còn treo

| Đường | Nguồn | Phương pháp |
|---|---|---|
| Hiệu suất SI | Unit Price tổng hợp SI (§8) | NAV-per-share (TWR), **total return** |
| Danh mục mẫu | SI Index (§6) | Index, **price return** |
| VN-Index | Market data | Index, **price return** |

```
%index   = (Index cuối / Index đầu − 1) × 100%
%vnindex = (điểm cuối / điểm đầu − 1) × 100%
```

> ✅ **Point D — CHỐT GIỮ NGUYÊN (PR, không đổi).**
> - Danh mục mẫu (PR) vs VN-Index (PR) = **cùng cơ sở → đúng**, không cần sửa.
> - KH/SI (TR) vs benchmark (PR): **có gap nhưng CHẤP NHẬN** — user chỉ quen so danh mục của mình với VN-Index, không cần đổi sang VN30TRI/total-return.
> - **Known accepted artifact** (không phải bug): SI/KH luôn nhỉnh hơn VN-Index ~mức cổ tức (~1.5–2.5%/năm, compound 10 năm ~20–30%) dù FM không alpha; đường "danh mục mẫu" (PR) thấp hơn đường "SI" (TR) một cách hệ thống. Đây là hệ quả PR vs TR, không sửa.

---

## 8. KHÁCH HÀNG trong SI — per (KH × SI)

| Khái niệm | Công thức |
|---|---|
| **Customer NAV** | SDI tính từ holdings + tiền của tiểu khoản (đã chuẩn hóa §2) |
| **Customer Unit** | `Unit_(t-1) + CF_t / Customer Unit Price_(t-1)` (CF = nạp − rút của KH) |
| **Customer Unit Price** | `Customer NAV_cuối / Customer Unit_t` (historic t-1, §4.1) |
| **Customer %PnL (TWR)** | qua Customer Unit Price — "hiệu suất chiến lược" |
| **Customer MWR** | Modified Dietz / XIRR — "lợi suất của bạn" (§5.4) |

### Hiệu suất SI tổng hợp (đường chart)

```
SI NAV       = Σ Customer NAV          (cộng trực tiếp — khớp tự động)
SI Unit Price = SI NAV / Σ Customer Unit   (asset-weighted, đại diện sản phẩm)
```

> ✅ Reconciliation trong SMA là **FREE**: `SI NAV ≡ Σ Customer NAV` chỉ là phép cộng. KHÔNG cần trick "unit price chung" của plan §7 (trick đó còn gây lệch với FR-06).

---

## 9. KIẾN TRÚC: SMA (segregated custody + gộp lệnh) ✅

| Khía cạnh | Kết luận |
|---|---|
| Sở hữu | Segregated — tiền/CP thật trong tiểu khoản KH |
| Thực thi | Aggregated — gộp lệnh, phân bổ pro-rata |
| NAV | SDI tính per (KH × SI); SI NAV = Σ KH |
| Unit Price | **Per KH** (không chung) |
| FR-06 | Per SI từ FO + NAV/phí do SDI tính |
| Pháp lý | An toàn (tài sản KH tách bạch, đúng mô hình ủy thác CTCK) |

### 9b. Ranh giới SDI ↔ FO khi rebalance/giải ngân (O4 đã chốt)

Cầu nối TG2 (weights) → TG1 (tài khoản thật). **SDI KHÔNG sinh/khớp lệnh** — FO làm.

| Thực thể | Chủ sở hữu | Vai trò |
|---|---|---|
| Đăng ký tham gia | SDI | KH đăng ký vào SI |
| **Yêu cầu rebalance** | **SDI → FO** | SDI báo FO weights mới cần đạt |
| Gom lệnh + đẩy MP + allocate | **FO** | FO tự thực hiện cho toàn bộ KH trong SI |
| **Execution feed** | **FO → SDI** | Kết quả khớp MP per tiểu khoản (mã, KL, giá khớp) |
| Sổ cái holdings KH | SDI | Dựng từ execution feed + CA → mark-to-market NAV |

> **Execution feed từ FO là nguồn bắt buộc** để tính NAV per KH (giá MP = giá khớp thật, biết EOD).
> ⚠️ Còn chốt: **lô lẻ** (FO mua lô lẻ hay để dư tiền?), **độ trễ** (yêu cầu rebalance T → FO khớp T mấy?) — quyết định cash drag per KH (O5, O6).

---

## 10. DỮ LIỆU & QUY MÔ (10 năm)

Giả định: 100 SI, KH active TB 5 SI, ~200.000 KH, 2.500 ngày.

| Dữ liệu | Ước lượng | Số dòng 10 năm |
|---|---|---|
| SI Index / Performance / Asset snapshot SI | 100 × 2.500 | ~250K mỗi loại |
| Cashflow / allocation event | 200K × 5 × (SIP + rebalance) | ~100M+ |
| **NAV per (KH × SI) nếu lưu daily** | 200K × 5 × 2.500 | **~2,5 TỶ** 🔴 |

> ❌ **Đừng materialize NAV/holdings daily per KH.**
> Trong SMA, NAV KH = holdings thật MTM, holdings đổi khi cashflow **và** rebalance/allocation (1–2/ngày).
> → Lưu **sổ cái event** (holding-change từ allocation + corporate action; cash nạp/rút) + **vector giá EOD** → **derive** `NAV_t = Σ(lot_qty × price_t) + cash_t` (holdings piecewise-constant giữa các event).
> → Nếu cần bảng NAV daily để query nhanh: tính lazy/nightly, **partition `hash(customer_id)`** (không phải `YYYY`), index `(customer_id, si_id, business_date)`.

---

## 11. QUY TRÌNH EOD

```
B1  Sync FO / ASSET / Market data
B2  Tính NAV per (KH×SI)  (tài sản − phí; xử lý tiền pending; phí quản lý ACCRUE daily ⚠️ F)
B3  PnL ngày = NAV cuối − đầu + ra − vào
B4  Gom cashflow trong ngày (net CF)              ┐ historic pricing
B5  ΔUnit = net CF / Unit Price_(t-1)             │ chốt 1 lần EOD
B6  Unit = Unit_(t-1)+ΔUnit (full); UP_t = NAV/Unit ┘ (§4.1)
B7  SI Unit Price tổng hợp = ΣNAV / ΣUnit
B8  SI Index (EOD, weights net cuối ngày §6)
B9  Push sang Asset (snapshot, holding, performance, index, customer position)
```

---

## 12. TRẠNG THÁI các điểm BRD lệch chuẩn

| Mã | Vấn đề | Trạng thái |
|---|---|---|
| **A** | Index khi rebalance | ✅ Chốt: EOD-only, hiệu lực tại close (§6.2) |
| **B** | Định giá ΔUnit | ✅ Chốt: **historic pricing (giá t-1), 1 lần EOD** — giữ theo BRD gốc + file mẫu (§4.1) |
| **C** | "Lãi Infy" cash-in | ✅ Chốt: đúng là cash-in (§3) |
| — | Unit lưu integer | ✅ Chốt: lưu **full precision** (§4.1) |
| — | Pooled vs Segregated | ✅ Chốt: **SMA segregated + gộp lệnh** (§9) |
| — | Unit Price chung (plan §7) | ✅ Bỏ: dùng **per-KH** (§4.2, §8) |
| **D** | TR (SI) vs PR (mẫu, VN-Index) | ✅ Chốt **GIỮ NGUYÊN (PR)** — mẫu vs VN-Index cùng cơ sở; gap KH-vs-benchmark chấp nhận vì UX (§7) |
| **E** | TWR hiển thị như "% của KH" | ✅ Chốt: **implement CẢ TWR + MWR**, gán nhãn rõ (§5.3, §5.4) |
| **F** | Phí quản lý cadence | ⚠️ **Treo — chốt sau.** Thu phí theo THÁNG tại ngày cố định (đã rõ). Cần chốt: (F1) phí tháng tính trên AUM snapshot ngày thu hay AUM bình quân ngày? (F2) SDI có accrue daily vào "phí phải trả" không? Khuyến nghị: **accrue daily + thu tháng** (NAV mượt, công bằng, khớp `NAV=tài sản−phí phải trả`) |
| — | %PnL vs PnL-tiền lệch khung kỳ | ⚠️ Treo — thống nhất mốc kỳ (§5.2, §14) |

### Bảng dữ liệu còn THIẾU
1. **Danh mục mẫu** (weights theo `effective_date`) — input SI Index.
2. **Cấu hình đầu tư KH** (đầu tư ban đầu, kỳ/số tiền SIP, phí quản lý) — FR-04.
3. **VN-Index / benchmark market data** — đường 3 FR-03.
4. **Corporate Action** — recompute index khi có quyền.
5. **Yêu cầu rebalance (SDI→FO) + execution feed (FO→SDI)** (§9b) — cầu nối model → tài khoản; SDI không sinh lệnh.

---

## 13. THAM CHIẾU NHANH — tất cả công thức

```
# TÀI SẢN (SDI tính)
Tổng tài sản    = Tiền + Chứng khoán
Chứng khoán     = Σ (KL × market price)
Tiền            = Tiền mặt + tiền bán chờ về + cổ tức tiền
NAV             = Tổng tài sản − Phí phải trả
Tổng vốn đầu tư  = Σ NAV vào − Σ NAV ra

# UNIT per (KH × SI) — historic pricing (t-1), chốt EOD
Unit Price_0    = 10.000
Unit_0          = NAV_0 / 10.000
CF_t            = NAV vào − NAV ra            (net, gom trong ngày)
ΔUnit_t         = CF_t / Unit Price_(t-1)     (giá hôm trước)
Unit_t          = Unit_(t-1) + ΔUnit_t        (lưu full precision)
Unit Price_t    = NAV cuối_t / Unit_t

# HIỆU SUẤT (TG1)
PnL ngày        = NAV cuối − NAV đầu + NAV ra − NAV vào
PnL cả kỳ       = Σ PnL ngày
%PnL (TWR)      = (Unit Price cuối / Unit Price đầu − 1) × 100%   (cùng mốc kỳ với PnL tiền) — "hiệu suất chiến lược"
Daily return    = Unit Price_t / Unit Price_(t-1) − 1
MWR (Mod.Dietz) = (NAV cuối − NAV đầu − CF_ròng) / (NAV đầu + Σ w_i·CF_i)   — "lợi suất của bạn"; w_i=(T−t_i)/T

# SI TỔNG HỢP
SI NAV          = Σ Customer NAV
SI Unit Price   = SI NAV / Σ Customer Unit

# CHỈ SỐ (TG2) — EOD, weights net cuối ngày
Index_0         = 1000
Index_t         = Index_(t-1) × Σ_i ( w_i × P_i,t / P_ref_i )
%index          = (Index cuối / Index đầu − 1) × 100%
%vnindex        = (điểm cuối / điểm đầu − 1) × 100%
```

---

## 14. WORKED EXAMPLE — verify file Excel "PnL danh mục KH" (per-KH)

### 14.1 Verify file mẫu (historic pricing t-1 — SPEC đã chốt): mọi ô ĐÚNG

| Ngày | NAV | ra | vào | PnL | ΔUnit | Unit (hiển thị) | Unit Price |
|---|---|---|---|---|---|---|---|
| KT | 10,000,000 | | 10,000,000 | – | – | 1,000 | 10,000 |
| 2 | 15,000,000 | | | 5,000,000 | – | 1,000 | 15,000 |
| 3 | 18,000,000 | 100,000 | 2,000,000 | 1,100,000 | 127 | 1,127 | 15,976 |
| 4 | 20,000,000 | 150,000 | 3,500,000 | (1,350,000) | 210 | 1,336 | 14,966 |
| 5 | 20,000,000 | | | – | – | 1,336 | 14,966 |
| 6 | 25,000,000 | | | 5,000,000 | – | 1,336 | 18,708 |
| 7 | 25,500,000 | | | 500,000 | – | 1,336 | 19,082 |

- **ΔUnit ngày 3** = (2tr − 0.1tr) / UP₂(15,000) = **126.667**; Unit₃ = 1,126.667; UP₃ = 18M/1,126.667 = **15,976** ✓
- **net_cashflow ngày 3** = ΔUnit × UP₂ = 126.667 × 15,000 = **1,900,000** ✓ (khớp vào − ra)
- **Rounding:** Unit hiển thị 1,127 nhưng tính bằng **1,126.667** → bắt buộc **lưu full precision** (lưu integer → UP sai).
- **Đặc tính historic (chấp nhận):** ngày có cashflow lớn, %UP (15,976/15,000−1 = 6.51%) ≠ %tiền (1.1M/15M = 7.33%) chênh nhẹ; **NAV/tiền vẫn đúng**.

### 14.2 Lỗi khung kỳ trong file mẫu (cần thống nhất mốc — O7)

File mẫu: `%PnL ngày 3→7 = 19.44%` (gốc = UP cuối ngày 3 = 15,976) nhưng `PnL tiền ngày 3→7 = 5,250,000` (cộng cả PnL ngày 3) → **khác mốc**. Nhất quán phải là **một trong hai cặp**:
- Từ **cuối ngày 3**: %PnL = 19,082/15,976 − 1 = **19.44%**, PnL tiền (ngày 4–7) = **4,150,000**.
- Từ **cuối ngày 2** (gồm ngày 3): %PnL = 19,082/15,000 − 1 = **27.21%**, PnL tiền (ngày 3–7) = **5,250,000**.

---

## 15. QUY ƯỚC THUẬT NGỮ & ĐẶT TÊN CHUẨN (canonical — CODE PHẢI THEO)

> Đây là **nguồn DUY NHẤT** cho tên biến/cột/field khi implement. Cột **`code`** = snake_case dùng trong DB/API/code.
> Mục tiêu: một khái niệm = một tên, không lẫn VN/EN/viết tắt khác nhau giữa các module.

### 15.1 Entity & cấu trúc

| VN / BRD | English chuẩn | code | Nghĩa | Lưu ý chống lẫn |
|---|---|---|---|---|
| SI / SDI / DMUT / chỉ số | Strategy Investment | `si_id` | "Quỹ chỉ số" KH tham gia | **Entity dùng `si`**; `sdi_` chỉ là **prefix hệ thống**. KHÔNG dùng "strategy"/"dmut" trong code |
| (mã hiển thị SDI01) | SI code | `si_code` | mã hiển thị | khác `si_id` (khóa số) |
| Tiểu khoản | sub-account | `sub_account` (= `customer_id`+`si_id`) | đơn vị nhỏ nhất = (KH × SI) | 1 KH nhiều sub-account |
| Khách hàng | customer | `customer_id` | | |
| Danh mục mẫu | model portfolio | `model_weight` | rổ + tỷ trọng định nghĩa chiến lược | KHÁC holdings thật |
| Holdings thực tế | holdings | `holding` | CP nắm thật | |
| Rebalance | rebalance | `rebalance` | đổi tỷ trọng/cấu phần mẫu | |
| Ngày làm việc | business date | `business_date` | đơn vị tính EOD | |

### 15.2 Tài sản (asset)

| VN / BRD | English | code | Lưu ý |
|---|---|---|---|
| Tổng tài sản | total assets | `total_asset` | = Tiền + Chứng khoán |
| Giá trị chứng khoán | securities value | `stock_value` | Σ(KL × giá) |
| Tiền (tổng) | cash | `cash` | **chỉ để tính NAV**; KHÔNG lấy Δ làm cashflow |
| Tiền mặt | cash balance | `cash_balance` | |
| Tiền bán chờ về | pending sell proceeds (T+) | `pending_sell` | |
| Tiền mua chờ khớp | pending buy | `pending_buy` | |
| Tiền chờ giải ngân | undeployed cash | `undeployed_cash` | gây cash drag |
| Cổ tức tiền mặt | cash dividend | `cash_dividend` | **income, KHÔNG phải cash flow** |
| Phí lưu ký | custody fee | `custody_fee` | nguồn FO |
| Phí quản lý | management fee | `management_fee` | SDI accrue (điểm F) |
| Phí phải trả | payable | `payable_fee` | NAV trừ cái này |
| Phí phạt rút sớm | early redemption penalty | `early_withdrawal_penalty` | KHÔNG vào NAV in/out |
| NAV / Tài sản ròng | net asset value | `nav` | = total_asset − payable_fee |
| Sức mua | buying power | `buying_power` | FR-06 |
| Tài sản có thể rút | withdrawable assets | `withdrawable_asset` | FR-06 (công thức phức tạp) |
| Tổng vốn đầu tư | net invested capital | `net_invested_capital` | = cumulative net cash flow |

### 15.3 Cash flow (4 tầng — luôn rõ tầng)

| VN / BRD | English chuẩn | code | Tầng / Lưu ý |
|---|---|---|---|
| (loại) tiền KH bơm/rút | external cash flow | — | **category**; phân biệt income & trade |
| Giao dịch nạp/rút | cash flow event | `cashflow_event` | **gross**, per giao dịch (ledger) |
| NAV vào | external inflow / contribution | `cash_in` | nạp/SIP/lãi Infy |
| NAV ra | external outflow / withdrawal | `cash_out` | rút |
| (CF_t) | **net external cash flow** | `net_cashflow` | = `cash_in − cash_out`, **per (KH×SI), per ngày** — dùng trong công thức unit |
| Cổ tức/lãi từ tài sản | investment income | `income` | **KHÔNG phải cash flow** → vào PnL |

> Quy tắc: trong **công thức** luôn dùng `net_cashflow` (rõ "net"), KHÔNG viết trống "cashflow". `cash_in/cash_out` lấy từ **event có nhãn**, KHÔNG từ Δ`cash`.

### 15.4 Unit & hiệu suất (xem §5.0 để phân biệt nghĩa)

| VN / BRD | English | code | Lưu ý |
|---|---|---|---|
| Unit | unit (shares) | `unit` | **NUMERIC full precision**, không integer |
| Unit Price | unit price (NAV/share) | `unit_price` | thước đo TWR; T0=10000 |
| Delta Unit | unit change | `delta_unit` | = net_cashflow / unit_price_(t-1) (historic) |
| PnL (tiền) | profit & loss | `pnl` / `daily_pnl` | ① TIỀN (VND) |
| %PnL = %return | period return (TWR) | `return_pct` | ② % cả kỳ = `unit_price` cuối/đầu − 1 |
| daily return | daily return | `daily_return` | ② % 1 ngày = TWR 1 ngày |
| TWR | time-weighted return | `twr` | = `return_pct` của hệ |
| MWR | money-weighted return | `mwr` | ✅ implement; Modified Dietz mặc định, XIRR tùy chọn (§5.4) |

### 15.5 Index / benchmark (TG2)

| VN / BRD | English | code | Lưu ý |
|---|---|---|---|
| SI Index (danh mục mẫu) | model index | `si_index` / `index_value` | **price return** (D giữ PR) |
| Tỷ trọng mẫu | target weight | `target_weight` | Σ=100% (CP) |
| Tỷ trọng holdings thật | weight | `weight` | trên tổng CP (FR-05) |
| Giá tham chiếu | reference price | `ref_price` | close hôm trước |
| Giá ref điều chỉnh quyền | adjusted reference price | `adjusted_ref_price` | khi có CA |
| VN-Index | benchmark (price return) | `benchmark` / `index_value` | đường 3 FR-03 |
| Giá đóng cửa | close price | `close_price` | |

### 15.6 Phương pháp / khái niệm

| VN | English | code/term | Lưu ý |
|---|---|---|---|
| Định giá ΔUnit theo giá hôm trước | historic pricing | `historic_pricing` | ✅ chốt (BRD gốc) |
| Định giá cuối ngày ex-cash | forward pricing | `forward_pricing` | đã cân nhắc, KHÔNG dùng |
| Hạch toán ngày khớp | trade-date accounting | `trade_date_accounting` | ✅ |
| Trích phí dồn ngày | accrual | `accrual` | điểm F |
| Cản trở do tiền nhàn | cash drag | `cash_drag` | thật, per KH |
| Sai lệch so benchmark | tracking error | `tracking_error` | hợp lệ |
| Tài khoản riêng + gộp lệnh | separately managed account | `sma` | mô hình đã chốt |
| Tách bạch / gộp chung | segregated / pooled | `segregated` / `pooled` | hệ = segregated |
| Lợi suất giá / tổng | price / total return | `price_return` / `total_return` | hệ dùng PR cho benchmark |

### 15.7 Quy tắc đặt tên & kiểu dữ liệu

- **snake_case** theo cột `code` cho mọi DB column / API field / biến.
- **Tiền**: `BIGINT` (VND). **Tỷ lệ/%/giá**: `NUMERIC`. **Unit**: `NUMERIC(38,10)` (full precision).
- **Entity = `si`** (key `si_id`); `sdi_` chỉ là prefix bảng hệ thống.
- **Trong công thức**: dùng `net_cashflow` (rõ "net"); `income` tách khỏi cash flow; `cash` (tổng) chỉ cho NAV.
- **% lưu dạng thập phân** (`0.0733`), format `×100` ở tầng hiển thị.
- Một khái niệm = một `code`; cấm dùng synonym khác nhau giữa các module.
