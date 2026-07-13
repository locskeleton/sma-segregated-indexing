# SDI — Cách tính & cuối ngày EOD lưu gì

> Tài liệu **đọc lại khi quên**. Giải thích bằng lời trước, công thức sau. Mọi số liệu đối chiếu code thật trong `db/`.

---

## 0. Nguyên tắc một câu

**SDI KHÔNG tự tính hiệu suất khách hàng.** Asset gửi sẵn `daily_return` (đã khử dòng tiền). SDI **LƯU** nguyên liệu mỗi ngày, rồi **LẮP RÁP lúc đọc**.

Chỉ có **2 thứ SDI tự tính**: **index danh mục mẫu** (từ giá + trọng số) và **phí quản lý** (từ AUM).

---

## 1. Hai cái đồng hồ — nhớ cái này là hết lẫn

| | Đơn vị thời gian | Vì sao |
|---|---|---|
| Hiệu suất · index · TE | **PHIÊN GIAO DỊCH** (~252/năm) | Không có phiên ⇒ không định giá lại ⇒ không có lãi/lỗ |
| **Phí quản lý** | **NGÀY DƯƠNG LỊCH** (365) | Tiền nằm trong TK ngày T7 thì vẫn chịu phí ngày T7 |

**Asset gửi dữ liệu MỌI ngày lịch (365)**, kể cả T7/CN/lễ — vì nạp/rút cuối tuần vẫn đổi AUM.
**Nhưng EOD chỉ chạy NGÀY GD.** Ngày nghỉ: chỉ nhận data + tính phí, không tính hiệu suất/index.

---

## 2. Mỗi ngày Asset gửi gì (per tiểu khoản)

| Trường | Là gì |
|---|---|
| `aum` | Tài sản cuối ngày (đã ròng phí). **AUM = NAV** |
| `cash` | Tổng tiền dư |
| `cash_available` | Tiền **khả dụng** (rút/cắt được thật) → **nguồn thu phí** |
| `daily_return` | **Lợi suất ngày (TWR)** — đã khử nạp/rút. **Ngày nghỉ = 0** |
| `cash_in` / `cash_out` | Nạp / rút trong ngày (để đối soát) |

> `daily_return` là con số **quan trọng nhất**. Nếu Asset tính sai (dùng `AUM_hôm_nay / AUM_hôm_qua − 1`) thì **khoản nạp biến thành lãi** và mọi số hiệu suất của SDI sai theo. Xem [SDI-daily-return-contract.md](./SDI-daily-return-contract.md).

---

## 3. Năm con số nghiệp vụ — giải thích bằng lời

### (a) Hiệu suất 1 khách hàng — `Rᵢ`

**Nói bằng lời:** 1 đồng của khách bỏ vào đầu kỳ, cuối kỳ thành bao nhiêu.

```
Rᵢ = (1+r₁)(1+r₂)...(1+rₙ) − 1
```

**Vì sao NHÂN chứ không CỘNG:** lãi ngày 2 sinh ra trên số vốn *đã có lãi* ngày 1. Cộng `+1% +1%` ra 2%, thực tế là 2,01%.

**Vì sao code viết `EXP(Σ ln(1+r)) − 1`:** đúng y hệt về toán, nhưng nhân hàng trăm số nhỏ dễ sai số/tràn; đổi sang cộng logarit thì SQL làm set-based được. **Không phải công thức khác.**

**Lưu gì:** chỉ lưu `daily_return` từng ngày. **Không lưu `Rᵢ`** — tính lúc đọc.

---

### (b) Index danh mục mẫu — SDI tự tính

**Nói bằng lời:** "Nếu bám **đúng y** danh mục mẫu thì 1.000 điểm ban đầu giờ thành bao nhiêu." Đây là **thước đo chuẩn** để so.

```
Index_t = Index_(t−1) × Σᵢ ( wᵢ × Pᵢ,t / Pᵢ,ref )        gốc 1.000
```

- `wᵢ` = trọng số mục tiêu mã i · `Pᵢ,t` = giá đóng cửa · `Pᵢ,ref` = giá tham chiếu đầu phiên
- Là **chuỗi NHÂN DỒN** → **tính nhầm 1 ngày nghỉ là sai theo cấp số nhân** (1 cuối tuần = +21%). Đó là lý do có `T_TRADING_HOLIDAY`.

**Lưu:** `T_MASTER_INDEX_DAILY` — `C_INDEX_VALUE` + `C_DAILY_RETURN`. **Chỉ ngày GD.**

---

### (c) Hiệu suất "danh mục tổng khách hàng" của **1 master** — AUM-weighted

**Nói bằng lời:** khách hàng thực tế lãi bao nhiêu. Khách nhiều tiền thì tiếng nói nặng hơn.

```
Wᵢ = AUMᵢ / Σ AUM        ← Σ chỉ trong CÙNG MỘT master
R_tổng = Σᵢ Wᵢ × Rᵢ
```

> ⚠️ **Tính RIÊNG cho từng master**, không gộp toàn hệ. Vì mỗi master có **index riêng** để so — gộp nhiều master lại thì không còn benchmark nào để đối chiếu.

**Lưu gì:** `Wᵢ` lấy từ `T_SI_CURRENT.C_LAST_AUM` (ảnh chụp hiện tại); `Rᵢ` tính từ (a). **Không lưu kết quả.**

---

### (d) Deviation (BPS)

**Nói bằng lời:** khách bám sát danh mục mẫu tới đâu. Dương = khách ăn hơn mẫu.

```
Deviation = ( R_tổng_KH − R_index ) × 10.000        (1 BPS = 0,01%)
```

---

### (e) Tracking Error (TE) — và **lý do EOD phải lưu 3 cột lạ**

**Nói bằng lời:**
- **Deviation** trả lời: *"lệch bao nhiêu?"*
- **TE** trả lời: *"lệch có ỔN ĐỊNH không?"*

TE thấp = ngày nào cũng bám đều. TE cao = lúc hơn nhiều lúc kém nhiều (dù trung bình có thể vẫn bằng 0). Nó là **độ lệch chuẩn của chênh lệch hằng ngày**.

```
active return ngày d:   a_d = r_KH(d) − r_index(d)
TE = STDEV(a) × √(số phiên)
```

**Vấn đề:** người dùng bấm xem TE cho **khoảng bất kỳ** (1 tháng / 1 năm / từ đầu). Muốn tính stdev thì phải có `Σa`, `Σa²`, `n` **của đúng khoảng đó** → nếu không lưu sẵn thì mỗi lần bấm phải **quét lại toàn bộ lịch sử của 50.000 khách**. Chết máy.

**Mẹo (prefix-sum):** mỗi ngày lưu **cộng dồn TỪ ĐẦU ĐỜI** (không phải giá trị riêng của ngày đó!):

```
C_ACCUM_ACTIVE_RET     = Σ a    (từ inception tới HẾT ngày đó)
C_ACCUM_ACTIVE_RET_SQ  = Σ a²
C_RET_DAY_COUNT        = n
```

#### Cách ghi (job `J12B_TE_ACCUM`, chỉ ngày GD)

Đọc **dòng của phiên GD hôm trước** rồi cộng thêm phần hôm nay:

```
a_d    = daily_return(KH, d) − daily_return(index, d)     ← "active return" ngày d

Σa(d)  = Σa(d−1)  + a_d
Σa²(d) = Σa²(d−1) + a_d²
n(d)   = n(d−1)   + 1
```

Ngày thiếu return (KH hoặc index NULL) → đóng góp **0**, `n` **không tăng**.

#### Ví dụ số

| Ngày | r_KH | r_index | `a` (riêng ngày) | **Σa (LƯU)** | **Σa² (LƯU)** | **n (LƯU)** |
|---|---|---|---|---|---|---|
| d1 | +1,0% | +0,8% | **+0,002** | 0,002 | 0,000004 | 1 |
| d2 | −0,5% | −0,3% | **−0,002** | 0,000 | 0,000008 | 2 |
| d3 | +0,7% | +0,5% | **+0,002** | 0,002 | 0,000012 | 3 |
| d4 | +0,2% | +0,4% | **−0,002** | 0,000 | 0,000016 | 4 |

> Cột `a` (riêng ngày) **KHÔNG được lưu** — chỉ lưu 3 cột cộng dồn bên phải.

#### Lấy ra cho khoảng bất kỳ = **TRỪ 2 LÁT**

TE của khoảng **d1 → d4** (tức 3 ngày d2, d3, d4):

```
Σa  = Σa(d4)  − Σa(d1)  = 0,000    − 0,002    = −0,002
Σa² = Σa²(d4) − Σa²(d1) = 0,000016 − 0,000004 =  0,000012
n   = n(d4)   − n(d1)   = 4 − 1 = 3

Var = ( Σa² − (Σa)²/n ) / (n−1) = (0,000012 − 0,0000013) / 2 = 0,0000053
TE  = √Var × √n = 0,00231 × √3 = 0,4%
```

Đọc **đúng 2 dòng** (d1 và d4) thay vì quét 250 dòng × 50.000 khách. Đó là toàn bộ lý do 3 cột đó tồn tại.

#### ⚠️ Hai chỗ dễ vấp

**1. Khoảng luôn là `(base, end]` — KHÔNG tính ngày base.** Vì `Σa(base)` đã bao gồm cả ngày base; trừ nó ra thì ngày base biến mất. Đó là lý do khắp code viết `C_BUSINESS_DATE > @base AND <= @end`, **không phải `>=`**.

**2. Vì sao lưu `Σa²` chứ không lưu thẳng phương sai?** Vì **phương sai KHÔNG cộng/trừ được** giữa các khoảng, còn **tổng bình phương thì cộng/trừ được**. Lưu `Σa²` rồi ráp phương sai lúc đọc là cách **duy nhất** làm được trò "trừ 2 lát".

**3. BẪY từ khi Asset gửi 365 ngày:** `T_SI_BALANCE` giờ có cả dòng T7/CN, nhưng **J12B chỉ update dòng NGÀY GD** ⇒ **dòng ngày nghỉ có `Σa = Σa² = n = 0`** (giá trị default), *không phải* số cộng dồn.
> **TUYỆT ĐỐI không lấy lát của ngày nghỉ làm mốc base/end** → ra TE rác.
> Code hiện tại an toàn vì PM API lấy mốc ngày từ `T_MASTER_BALANCE` (chỉ EOD ghi ⇒ chỉ có ngày GD). Ai viết API mới mà lấy `MAX(C_BUSINESS_DATE)` từ `T_SI_BALANCE` làm mốc là **dính ngay**.

---

## 4. CUỐI NGÀY EOD LƯU GÌ — bảng tra nhanh

| Bảng | Mỗi dòng = | Ai ghi | Cột chính | Để làm gì |
|---|---|---|---|---|
| **`T_SI_BALANCE`** | 1 tiểu khoản × **1 NGÀY LỊCH** (365) | Asset ingest | `C_AUM`, `C_DAILY_RETURN`, `C_CASH`, `C_CASH_AVAILABLE`, `C_CASH_IN/OUT` | Nguyên liệu gốc của mọi thứ |
| ↳ *3 cột TE* | (cùng dòng trên) | **J12B**, chỉ ngày GD | `C_ACCUM_ACTIVE_RET`, `_SQ`, `C_RET_DAY_COUNT` | Prefix-sum → TE khoảng bất kỳ |
| **`T_SI_CURRENT`** | 1 tiểu khoản (ảnh chụp **hiện tại**) | Asset ingest | `C_LAST_AUM`, `C_CASH`, `C_CASH_AVAILABLE` | **Trọng số W** + **thu phí** |
| **`T_MASTER_BALANCE`** | 1 master × **1 ngày GD** | **J11**, EOD | `C_AUM` (=Σ SI), `C_DAILY_RETURN` (AUM-weighted), `C_CASH_IN/OUT`, `C_TOTAL_ACCOUNT` | Số cấp master + **mốc ngày** cho PM API |
| **`T_MASTER_INDEX_DAILY`** | 1 master × **1 ngày GD** | **J12** (luồng riêng, chỉ cần giá) | `C_INDEX_VALUE`, `C_DAILY_RETURN` | Benchmark để so |
| **`T_MASTER_HOLDING_BALANCE`** | 1 master × mã × ngày GD | **J14** | qty, giá, tỷ trọng | Composition, drift |
| **`T_SI_FEE_BALANCE`** | 1 tiểu khoản × **1 NGÀY LỊCH** | **`SP_FEE_RUN_DAILY`** (ngoài EOD, đồng hồ 365) | `C_AUM` (của **chính ngày đó**), `C_FEE_AMOUNT` | Phí từng ngày |
| **`T_SI_FEE_CHARGE`** | 1 tiểu khoản × **1 tháng** | **`SP_FEE_RUN_DAILY`** (cộng dồn + chốt cuối tháng) | `C_FEE_TOTAL`, `C_FEE_DUE`, `C_CLOSED_AT` | Nợ phí; `C_CLOSED_AT` NULL = **chưa chốt ⇒ chưa thu** |

### Thứ tự job trong EOD (chỉ chạy ngày GD)

```
[luồng riêng, chỉ cần giá BO]   J12  SI_INDEX        → T_MASTER_INDEX_DAILY

[pipeline chính]
  J0   GATE          chờ đủ nguồn (Asset NAV / FO / giá)
  J07  COMPUTE       nạp T_EOD_WORK từ T_SI_BALANCE (Asset đã gửi)  ← KHÔNG tính gì
  J11  SI_AGG        Σ lên master                → T_MASTER_BALANCE
  J12B TE_ACCUM      cộng dồn active return      → 3 cột trên T_SI_BALANCE
  J13  RECONCILE     đối soát → có lệch thì CHẶN publish
  J14  SNAPSHOT      composition                 → T_MASTER_HOLDING_BALANCE

[luồng PHÍ — TÁCH RIÊNG, chạy MỌI NGÀY LỊCH kể cả T7/CN/lễ]
  SP_FEE_RUN_DAILY @d
     ├─ SP_EOD_FEE_ACCRUE    phí ĐÚNG ngày đó (AUM ngày đó × rate/365) → T_SI_FEE_BALANCE
     │                        + upsert charge tháng (cộng dồn, C_CLOSED_AT = NULL)
     └─ SP_FEE_CLOSE_PERIOD  đóng sổ nếu là NGÀY CUỐI THÁNG (+ bắt-kịp kỳ cũ) → C_CLOSED_AT
```

---

## 5. Cái gì **KHÔNG** lưu (tính lúc đọc)

`Rᵢ` · `R_tổng_KH` · `Deviation` · `TE` · `%PnL kỳ`

**Vì sao:** tất cả đều phụ thuộc **khoảng thời gian người dùng chọn** (1M / 3M / 1Y / từ đầu). Lưu sẵn thì phải lưu cho **mọi khoảng** — vô hạn. Nên: **lưu nguyên liệu hằng ngày** (`daily_return` + 3 cột prefix-sum), **lắp lúc đọc**.

---

## 6. Phí quản lý — nhắc lại vì nó chạy đồng hồ khác

```
phí ngày d = AUM(CỦA CHÍNH NGÀY d) × rate / 365        ← ngày dương lịch, kể cả T7/CN
```

- **Tiền nằm trong tài khoản ngày nào thì chịu phí ngày đó.** Nạp 1 tỷ vào T7 ⇒ phí T7/CN tính trên số tiền mới.
- **Phí KHÔNG nằm trong EOD.** EOD bị gate lịch (chỉ ngày GD, vì index/TE không được tính ngày nghỉ) — để phí trong đó thì **mất phí ~115 ngày nghỉ/năm**.
  ⇒ App gọi **`SP_FEE_RUN_DAILY @d`** **mỗi ngày lịch**, ngay sau khi ingest Asset xong ngày đó. **1 ngày = 1 lần = 1 dòng/SI.**
- Bản ghi phí tháng **sinh ngay từ đầu kỳ**, cộng dồn theo ngày. **Chưa chốt tháng ⇒ chưa thu.**
- **Chốt kỳ ở NGÀY CUỐI THÁNG dương lịch** — kể cả khi ngày đó là T7/CN/lễ (vì phí đã accrue tới tận ngày đó). Có cơ chế **bắt-kịp**: lỡ một ngày chạy thì lần chạy sau tự đóng nốt kỳ cũ.
- Thu tiền (`SP_FEE_COLLECT`, **chỉ ngày GD** vì phải qua BO): cắt theo **`cash_available`**, không phải tổng tiền.

```
Ngày GD        :  ingest Asset  →  SP_FEE_RUN_DAILY  →  SP_EOD_RUN (index/TE/reconcile)  →  SP_FEE_COLLECT
Ngày nghỉ (T7/CN/lễ) :  ingest Asset  →  SP_FEE_RUN_DAILY                    (KHÔNG EOD, KHÔNG thu tiền)
```

> ⚠️ Ngày nghỉ **không có reconcile** (J13 nằm trong EOD). Nên phí T7/CN tính trên số Asset gửi **chưa qua đối soát**. Chấp nhận được vì phí chỉ là `AUM × rate/365` (không dính index/TE), và phiên GD kế tiếp reconcile **theo dải** (gồm ngày nghỉ) → nếu lệch thì **chạy lại accrue** là số tự đúng (proc idempotent).
