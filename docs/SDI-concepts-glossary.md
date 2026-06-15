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
> - **Forward pricing, chốt 1 lần cuối ngày**: cashflow quy đổi unit tại giá đóng cửa của CHÍNH ngày đó (không phải hôm qua). (§4) → sửa point B.
> - **NAV do SDI engine TÍNH** (xử lý tiền pending + trừ phí), không đọc raw giá trị tiểu khoản. (§2)
> - **SI Index**: tính EOD-only, rebalance hiệu lực tại close; rebalance là execution → lệch SI vs index là tracking error hợp lệ. (§6)
> - **"Lãi Infy" = cash-in** (tiền KH bơm vào). (§3)
> - **D — CHỐT GIỮ NGUYÊN (PR, không đổi)**: danh mục mẫu (PR) vs VN-Index (PR) cùng cơ sở → đúng. Gap KH/SI (TR) vs benchmark (PR) **chấp nhận** vì user quen so với VN-Index. Known accepted artifact. (§7)
> - Còn treo: **E** (TWR vs MWR hiển thị), **F** (accrue phí daily), chi tiết lô lẻ/độ trễ giải ngân. (§12)

---

## 0. Bản đồ khái niệm (đọc trước)

Có **2 thế giới song song**, đừng trộn lẫn:

```
┌─────────────────────────────────────────────────────────────────────┐
│  THẾ GIỚI 1 — TIỀN THẬT (per Khách hàng × SI)                         │
│  Tài sản thật trong tiểu khoản → NAV (SDI tính) → Unit / Unit Price   │
│  → Hiệu suất TỪNG KH (NAV-per-share, forward pricing, EOD)            │
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
> ✅ **Chốt methodology (O6):** tiền chờ giải ngân **vào NAV + phát unit NGAY tại ngày nộp** (forward); clock hiệu suất chạy từ ngày nộp. Mấy ngày chờ khớp → cash drag nằm trong tiểu khoản của **chính KH đó** (segregated → công bằng). **Trade-date accounting**: mua/bán ghi nhận tại ngày khớp (giá MP), tiền mua chờ khớp / bán chờ về tracked ở cash sub-ledger.
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
| **CF (forward unit)** | **CHỈ event nhãn DEPOSIT/SIP/WITHDRAW** — không decompose từ tổng |
| Income (cổ tức/lãi) | event nhãn DIVIDEND/INTEREST → vào NAV, KHÔNG vào CF → tự vào PnL |
| FR-06 (sức mua, tài sản có thể rút) | **components typed** (chờ giải ngân, phong tỏa, mua chờ khớp, bán chờ về) |

> **Nguyên tắc:** giữ **cash sub-ledger theo nhãn**; tổng chỉ để cộng NAV. Reconcile mỗi ngày: `Δ tổng tiền = Σ(nạp/rút) + Σ(cổ tức/lãi) + (bán − mua khớp lệnh)`.

---

## 4. UNIT (NAV-per-share) — trái tim hiệu suất, tính PER KH

Mỗi tiểu khoản (KH × SI) có **chuỗi NAV riêng, cashflow riêng → Unit & Unit Price RIÊNG**.

### 4.1 Công thức chuẩn — Forward pricing, chốt 1 lần cuối ngày ✅

```
T0:  Unit Price_0 = 10.000
     Unit_0       = NAV_0 / 10.000

Tn (chốt EOD, CF = NAV vào − NAV ra gom trong ngày):
     Unit Price_t = (NAV_cuối_t − CF_t) / Unit_(t-1)      # giá EOD, ex-cash
     ΔUnit_t      = CF_t / Unit Price_t                    # quy đổi tại giá NGÀY t
     Unit_t       = Unit_(t-1) + ΔUnit_t
     (hệ quả: Unit Price_t = NAV_cuối_t / Unit_t — tự khớp)
```

> ❌→✅ **Sửa point B:** BRD gốc quy đổi ΔUnit tại `Unit Price hôm qua (t-1)` (historic pricing) → bias hệ thống.
> **Đã chốt forward pricing**: quy đổi tại giá đóng cửa ngày t. Khi đó `daily return = Unit Price_t/Unit Price_(t-1) − 1 = gain_t / NAV_đầu_t` = TWR sạch, **khớp với PnL tiền**.

> ⚠️ **Implementation bắt buộc:** **Unit lưu full precision (thập phân)**, chỉ làm tròn khi hiển thị. Lưu integer → unit price drift sai. (Chứng minh: file mẫu hiển thị Unit=1,127 nhưng tính bằng 1,126.667 — xem §14.)

### 4.2 Tính chất per-KH (KHÔNG dùng giá chung)

- File Excel mẫu (PnL danh mục KH) tính Unit Price **riêng từng KH** từ NAV + cashflow của KH đó → **chốt per-KH**.
- ❌ Plan §7 ("mọi KH dùng SI Unit Price chung") **SAI** với mô hình SMA segregated → bỏ.
- Mỗi KH có tracking error riêng (cash drag, lô lẻ, thời điểm vào batch) → phải phản ánh, không san đều.

---

## 5. HIỆU SUẤT & PnL — TG1

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

### 5.3 TWR vs MWR — ⚠️ điểm E còn treo

| | TWR (qua Unit Price) | MWR (IRR dòng tiền KH) |
|---|---|---|
| Đo | Hiệu suất loại bỏ timing nạp tiền | Lợi suất tiền thật của KH |
| Giống mọi KH? | Không (mỗi KH unit price riêng) | Không |
| Chuẩn cho | "Fund/SI performance" | "Lợi suất cá nhân của bạn" |

> ⚠️ BRD hiển thị một con số % (TWR). KH SIP có thể hiểu nhầm vì TWR ≠ lợi suất tiền thật (MWR). Cần quyết hiển thị (E).

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
| **Customer Unit** | `Unit_(t-1) + CF_t / Customer Unit Price_t` (CF = nạp − rút của KH) |
| **Customer Unit Price** | `(Customer NAV_cuối − CF) / Customer Unit_(t-1)` (forward, §4.1) |
| **Customer %PnL** | TWR qua Customer Unit Price (⚠️ E: TWR vs MWR) |

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
B4  Gom cashflow trong ngày (net CF)              ┐ forward pricing
B5  Unit Price_t = (NAV cuối − CF)/Unit_(t-1)     │ chốt 1 lần EOD
B6  ΔUnit, Unit (full precision)                  ┘ (§4.1)
B7  SI Unit Price tổng hợp = ΣNAV / ΣUnit
B8  SI Index (EOD, weights net cuối ngày §6)
B9  Push sang Asset (snapshot, holding, performance, index, customer position)
```

---

## 12. TRẠNG THÁI các điểm BRD lệch chuẩn

| Mã | Vấn đề | Trạng thái |
|---|---|---|
| **A** | Index khi rebalance | ✅ Chốt: EOD-only, hiệu lực tại close (§6.2) |
| **B** | Định giá cashflow tại giá hôm qua | ✅ Chốt: **forward pricing, 1 lần EOD** (§4.1) |
| **C** | "Lãi Infy" cash-in | ✅ Chốt: đúng là cash-in (§3) |
| — | Unit lưu integer | ✅ Chốt: lưu **full precision** (§4.1) |
| — | Pooled vs Segregated | ✅ Chốt: **SMA segregated + gộp lệnh** (§9) |
| — | Unit Price chung (plan §7) | ✅ Bỏ: dùng **per-KH** (§4.2, §8) |
| **D** | TR (SI) vs PR (mẫu, VN-Index) | ✅ Chốt **GIỮ NGUYÊN (PR)** — mẫu vs VN-Index cùng cơ sở; gap KH-vs-benchmark chấp nhận vì UX (§7) |
| **E** | TWR hiển thị như "% của KH" | ⚠️ Treo — tách TWR vs MWR (§5.3) |
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

# UNIT per (KH × SI) — forward pricing, chốt EOD
Unit Price_0    = 10.000
Unit_0          = NAV_0 / 10.000
CF_t            = NAV vào − NAV ra            (gom trong ngày)
Unit Price_t    = (NAV cuối_t − CF_t) / Unit_(t-1)
ΔUnit_t         = CF_t / Unit Price_t
Unit_t          = Unit_(t-1) + ΔUnit_t        (lưu full precision)

# HIỆU SUẤT (TG1)
PnL ngày        = NAV cuối − NAV đầu + NAV ra − NAV vào
PnL cả kỳ       = Σ PnL ngày
%PnL (TWR)      = (Unit Price cuối / Unit Price đầu − 1) × 100%   (cùng mốc kỳ với PnL tiền)
Daily return    = Unit Price_t / Unit Price_(t-1) − 1

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

### 14.1 Bản gốc BRD (historic pricing, t-1) — tao verify từng ô: ĐÚNG theo công thức BRD

| Ngày | NAV | ra | vào | PnL | ΔUnit | Unit (hiển thị) | Unit Price |
|---|---|---|---|---|---|---|---|
| KT | 10,000,000 | | 10,000,000 | – | – | 1,000 | 10,000 |
| 2 | 15,000,000 | | | 5,000,000 | – | 1,000 | 15,000 |
| 3 | 18,000,000 | 100,000 | 2,000,000 | 1,100,000 | 127 | 1,127 | 15,976 |
| 4 | 20,000,000 | 150,000 | 3,500,000 | (1,350,000) | 210 | 1,336 | 14,966 |
| 5 | 20,000,000 | | | – | – | 1,336 | 14,966 |
| 6 | 25,000,000 | | | 5,000,000 | – | 1,336 | 18,708 |
| 7 | 25,500,000 | | | 500,000 | – | 1,336 | 19,082 |

- **Rounding:** Unit ngày 3 thật = 1,126.667 (hiển thị 1,127); UP = 18M/1,126.667 = 15,976 ✓ → phải lưu full precision.
- **Bias historic (point B):** ngày 3 return tiền = 1.1M/15M = **7.33%**, nhưng UP return = 15,976/15,000−1 = **6.51%** → lệch 0.82đ% do quy đổi tại giá hôm qua khi có tiền vào lớn.

### 14.2 Sau khi chốt forward pricing (EOD) — chuỗi đúng

| Ngày | CF (net) | Unit Price | Unit (full) | Daily return | = PnL/NAV_đầu |
|---|---|---|---|---|---|
| KT | +10,000,000 | 10,000 | 1,000.000 | – | – |
| 2 | 0 | 15,000 | 1,000.000 | +50.00% | 5M/10M ✓ |
| 3 | +1,900,000 | **16,100** | 1,118.012 | **+7.33%** | 1.1M/15M ✓ |
| 4 | +3,350,000 | 14,892 | 1,342.969 | −7.50% | −1.35M/18M ✓ |
| 5 | 0 | 14,892 | 1,342.969 | 0% | ✓ |
| 6 | 0 | 18,615 | 1,342.969 | +25.00% | 5M/20M ✓ |
| 7 | 0 | 18,987 | 1,342.969 | +2.00% | 0.5M/25M ✓ |

→ Forward pricing: mọi daily return = đúng PnL/NAV_đầu (TWR sạch). Ngày 3 ra **16,100** thay vì 15,976.

### 14.3 Lỗi khung kỳ trong file mẫu (cần thống nhất)

File mẫu: `%PnL ngày 3→7 = 19.44%` (gốc = UP cuối ngày 3 = 15,976) nhưng `PnL tiền ngày 3→7 = 5,250,000` (cộng cả PnL ngày 3).
→ Hai ô khác mốc. Nhất quán phải là **một trong hai cặp**:
- Từ **cuối ngày 3**: %PnL (forward) = 18,987/16,100−1 = **17.93%**, PnL tiền = **4,150,000**.
- Từ **cuối ngày 2** (gồm ngày 3): %PnL = 18,987/15,000−1 = **26.58%**, PnL tiền = **5,250,000**.
