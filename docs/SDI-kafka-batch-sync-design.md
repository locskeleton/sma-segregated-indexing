# Thiết kế đồng bộ Asset qua Kafka — nhiều pod, Redis điều phối

> **Ràng buộc:** không thêm bảng DB · nhiều pod chia tải · Redis quản batch + trạng thái job · **không** so danh sách tài khoản giữa Asset và SDI ở tầng Kafka (post-check lo) · **ngắt** consumer khỏi EOD/chain, chạy từng step tường minh.
>
> **Thay cho:** `COUNTER` / `TOTAL_PROCESSED` / `counter==1` / `INITIAL_LOCK` / `END_LOCK` / retry-loop 100s / `JobDone` trong RAM.

---

## 0. Ba nguyên tắc — mọi thứ dưới đây dựa vào chúng

> ### ① Redis lo TỐC ĐỘ. DB lo TÍNH ĐÚNG.
> Redis mất sạch key chỉ được phép làm **chậm/treo**, **không bao giờ** được làm **sai số**.
>
> ### ② Đừng để hệ mình chết vì hệ người khác lỗi.
> `totalRow` là số **Asset khai** — không kiểm soát được. Nó chỉ là **gợi ý**, không phải **cổng chặn**.
>
> ### ③ Đã xây trên nền at-least-once thì phép cộng phải có DEDUP.
> Kafka chỉ hứa *"ít nhất một lần"*. Nó **không bao giờ** hứa *"đúng một lần"*.

---

## 1. Hai câu hỏi — hai tầng — đừng trộn

| Tầng | Câu hỏi | Trả lời bằng | Sai thì sao |
|---|---|---|---|
| **Kafka + Redis** | *"Asset gửi đủ cái nó **KHAI** chưa?"* | `rows >= totalRow` (có dedup) **hoặc** timeout | Chỉ tốn một lần thử — **vô hại** |
| **DB (post-check)** | *"SDI đủ dữ liệu cho **tài khoản CỦA MÌNH** chưa?"* | `SP_EOD_RUN` → **err=12** | **Không thể sai** — đọc dữ liệu thật, tính từ registry thật |

⇒ Tầng Kafka **không đụng registry SDI**. Cờ `READY` bật sớm/muộn đều **vô hại**, vì cửa khoá thật nằm ở DB.

---

## 2. Hai khoá định danh — hai vai trò khác nhau

| Khoá | Nguồn | Trả lời | Dùng để |
|---|---|---|---|
| **`requestId`** | **Asset gửi sẵn** — chung cho MỌI batch của job | *"Đây là JOB nào?"* | `jobId` · scope Redis · MERGE job log |
| **`Sha256(data)`** | Consumer tự tính từ payload | *"Batch này xử lý chưa?"* | Dedup · cộng dồn an toàn |

### Vì sao dedup theo **nội dung**, không theo `(partition, offset)`

`offset` là danh tính của **cái phong bì**. `enable.idempotence=true` chỉ bảo vệ được **một phần**:

| Tình huống | `enable.idempotence` chặn được? |
|---|---|
| Retry vì timeout / ack rớt | ✅ |
| **Leader partition đổi broker** (failover) | ✅ — producer state nằm trong log của partition |
| **Producer process RESTART** (redeploy, OOM, evict) | ❌ **KHÔNG** — PID mới, broker không nhận ra sequence cũ |

⇒ Producer restart → cùng nội dung nằm ở **offset khác** → dedup theo offset **mù**.
`Sha256(data)` là danh tính của **nội dung bên trong** → **miễn nhiễm**.

> Vẫn bật `enable.idempotence = true` (rẻ, bịt các lỗ khác). Nhưng **đừng xây tính đúng dựa trên nó**.

---

## 3. Bản đồ Redis key

`P = SDI:{bizType}:{requestId}` — TTL **48h** toàn bộ.

| Key | Kiểu | Dùng để |
|---|---|---|
| `{P}:MSG` | **SET** | `Sha256(data)` của các batch đã xử lý → dedup. ~500 phần tử ≈ **35 KB** |
| `{P}:ROWS` | STRING | Tổng số dòng đã nhận (cộng dồn **có dedup**) |
| `{P}:TOTAL` | STRING | `totalRow` Asset khai (audit + so sánh) |
| `{P}:READY` | STRING | `"1"` — chống gọi `SP_EOD_SET_SOURCE_READY` lặp |
| `{P}:LAST_AT` | STRING | Timestamp message cuối → **timeout fallback + watchdog** |

**Đã xoá:** `COUNTER`, `COUNTER_SUCCESS`, `COUNTER_FAIL`, `TOTAL_PROCESSED`, `JOB_ID`, `INITIAL_LOCK`, `END_LOCK`, `LAST_TRY_DEQUEUE`, `EOD_DATE`.

---

## 4. Lua — cộng dồn NGUYÊN TỬ, một round-trip

Vấn đề nếu tách 2 lệnh:

```
SADD {P}:MSG <hash>      → đánh dấu đã xử lý
⚡ pod chết ĐÚNG ở đây
INCRBY {P}:ROWS 100      → KHÔNG BAO GIỜ CHẠY
⇒ message coi như xong nhưng 100 dòng không được cộng ⇒ TREO VĨNH VIỄN
```

Redis chạy Lua **nguyên tử** → không có cửa sổ ghi-dở:

```lua
-- KEYS[1] = {P}:MSG    KEYS[2] = {P}:ROWS
-- ARGV[1] = batchKey (sha256 của data)
-- ARGV[2] = số dòng trong batch
-- ARGV[3] = TTL giây
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
    local n = redis.call('INCRBY', KEYS[2], ARGV[2])
    redis.call('EXPIRE', KEYS[1], ARGV[3])
    redis.call('EXPIRE', KEYS[2], ARGV[3])
    return n                                              -- batch MỚI → đã cộng
else
    return tonumber(redis.call('GET', KEYS[2]) or '0')    -- batch TRÙNG → KHÔNG cộng
end
```

Giao lại 10 lần → `SADD` trả 0 → **không cộng lần nào nữa**.

---

## 5. Consumer — chỉ làm ĐÚNG MỘT VIỆC

**Không chain. Không EOD. Không phí. Không lock. Không chờ.**

```csharp
private async Task ProcessDataSyncAssetSdiCore(string rawJson, ConsumeResult<string,string> r)
{
    var model    = JsonConvert.DeserializeObject<AssetSdiCoreKafkaModel>(rawJson);
    var tranDate = model.data.Select(m => m.date).FirstOrDefault();
    var jobId    = model.requestId;                      // ★ JOB_ID có sẵn — không cần tạo, không cần chờ
    var P        = $"SDI:{bizType}:{jobId}";

    if (!Utils.IsValidDate(tranDate)) { Log.Error(...); return; }

    // ── 1) GHI DB TRƯỚC — idempotent (DELETE+INSERT theo (date,si)), all-or-nothing
    var (err, _) = await _sp.IngestAssetNavAsync(rawJson, tranDate);
    if (err != 0)
        throw new SyncException($"ingest err={err}");     // KHÔNG commit offset → Kafka giao lại

    // ── 2) CỘNG DỒN AN TOÀN — dedup theo NỘI DUNG, nguyên tử (Lua)
    var batchKey = Sha256(JsonConvert.SerializeObject(model.data));   // ⚠️ chỉ data, BỎ requestId/timestamp
    long rows = (long)await _redis.ScriptEvaluateAsync(LuaSumOnce,
        new RedisKey[]   { $"{P}:MSG", $"{P}:ROWS" },
        new RedisValue[] { batchKey, model.data.Count, 172800 });

    await _redis.StringSetAsync($"{P}:TOTAL",   model.totalRow, _ttl48h, When.NotExists);
    await _redis.StringSetAsync($"{P}:LAST_AT", DateTime.UtcNow.Ticks, _ttl48h);

    // ── 3) ĐỦ CHƯA → BẬT CỜ. HẾT.
    if (rows >= model.totalRow)
        await TrySetReadyAsync(P, tranDate, model.totalRow, rows, byTimeout: false);

    // ❌ KHÔNG gọi ProcessJobAumFeeAcccrue
    // ❌ KHÔNG gọi ProcessJobAumFeeCharge
    // ❌ KHÔNG gọi SP_EOD_RUN
    // → commit offset. Vài chục ms. KHÔNG BAO GIỜ bị Kafka đá.
}

private async Task TrySetReadyAsync(string P, string d, long total, long rows, bool byTimeout)
{
    // SET NX ⇒ chỉ 1 pod gọi SP, dù nhiều pod cùng thấy đủ
    if (!await _redis.StringSetAsync($"{P}:READY", "1", _ttl48h, When.NotExists)) return;

    if (byTimeout)
        Log.Warning("[{Biz}] {Date} chốt theo TIMEOUT: {Rows}/{Total} dòng. Asset có thể gửi thiếu " +
                    "hoặc totalRow sai. Đủ/thiếu THẬT do SP_EOD_RUN quyết (err=12).", bizType, d, rows, total);

    await _sp.SetSourceReadyAsync(d, "ASSET_NAV", total);   // ghi cờ C_ASSET_NAV_STATUS='READY'
    Log.Information("[{Biz}] {Date} ASSET_NAV READY: {Rows}/{Total}", bizType, d, rows, total);
}
```

### `totalRow` là GỢI Ý — timeout là lưới an toàn

Chạy trong `BackgroundService` (quét 15s), **không** phụ thuộc message mới tới — vì lúc treo thì **đúng là không còn message nào tới nữa**:

```csharp
foreach (var P in await ScanActiveJobsAsync())          // các job chưa READY
{
    long rows  = await _redis.StringGetAsync($"{P}:ROWS");
    long total = await _redis.StringGetAsync($"{P}:TOTAL");
    var  last  = await _redis.StringGetAsync($"{P}:LAST_AT");

    if (rows > 0 && DateTime.UtcNow - last > TimeSpan.FromMinutes(5))
        await TrySetReadyAsync(P, date, total, rows, byTimeout: true);
}
```

### Ném lỗi của Asset vào — không có ô nào chết

| Asset lỗi kiểu gì | Điều gì xảy ra |
|---|---|
| `totalRow` khai **thừa** | `rows` không bao giờ đạt → **timeout 5 phút → READY** → `SP_EOD_RUN` kiểm tra thật → chạy hoặc err=12. **Không treo** |
| `totalRow` khai **thiếu** | `READY` sớm → `SP_EOD_RUN` → **err=12** nếu thiếu SI của SDI → chặn → batch còn lại tới → chạy lại. **Không sai** |
| Gửi **trùng** `si_account` | Dòng trùng vẫn được cộng (đúng theo định nghĩa `totalRow` = số dòng gửi) → không ảnh hưởng |
| Gửi **dư** acc không thuộc SDI | `SP_INGEST_ASSET_NAV` lọc. `rows` vẫn cộng đủ theo payload → READY đúng |
| **Thiếu hẳn** tài khoản của SDI | → **err=12** → **CHẶN**. Đúng, vì đây mới là lỗi nguy hiểm |
| Producer restart → gửi lại batch | `Sha256(data)` trùng → **không cộng lại** |
| Kafka rebalance → zombie pod ghi song song | DB `DELETE+INSERT` theo `(date,si)` + `SADD` hash → **vô hại** |

---

## 6. NGẮT khỏi EOD — chạy từng step tường minh

Consumer **chỉ** ingest + bật cờ. Các bước sau gọi **riêng**, nhìn `err` là biết kẹt ở đâu:

| Step | Gọi | Chưa đủ tiền đề → | Chạy lại |
|---|---|---|---|
| 1. Asset ingest | Kafka consumer | — | idempotent |
| 2. **Phí** | `SP_FEE_RUN_DAILY @d` | — (chỉ cần AUM ngày đó). **Chạy MỌI ngày lịch**, độc lập | MERGE → an toàn |
| 3. **Index** | `SP_EOD_RUN_INDEX @d` | `10` MKT chưa READY · `11` thiếu giá mã rổ · `13` ngày nghỉ | idempotent |
| 4. **EOD** | `SP_EOD_RUN @d` | `10` precondition · **`12` thiếu SI của SDI** · `13` ngày nghỉ | step-gate → an toàn |

Sau khi từng step chạy ổn định và tin được rồi, muốn tự động hoá thì cho **scheduler quét `T_EOD_PIPELINE`** — **không bao giờ** nối lại vào trong Kafka handler.

> **Vì sao không bao giờ:** chain chạy trong handler → block consumer → vượt `max.poll.interval` → Kafka **đá pod** (nhưng **không giết thread** — nó vẫn ghi DB!) → **zombie ghi song song** → rebalance → giao lại → duplicate → vòng xoáy tự khuếch đại.

---

## 7. Redis mất key thì sao — bảng trung thực

| Mất | Hậu quả | **Sai số liệu?** |
|---|---|---|
| `{P}:MSG` | Batch giao lại **được cộng lại** → `rows` phình → READY sớm | ❌ **Không** — `SP_EOD_RUN` err=12 vẫn chặn nếu thiếu |
| `{P}:ROWS` | `rows` tụt → không đạt `totalRow` → **timeout 5 phút → READY** | ❌ Không |
| `{P}:READY` | Gọi lại `SP_EOD_SET_SOURCE_READY` | ❌ Không (idempotent) |
| **Toàn bộ Redis** | Chậm/thử lại, có thể trùng công | ❌ **Không bao giờ sai số** |

**Không có ô nào ✅.** Đúng nguyên tắc ①.

---

## 8. Chia tải nhiều pod — tự do

Vì mọi thao tác đã idempotent:

- Partition **tuỳ ý** (round-robin cũng được) — **không cần** ép `key = job_key`.
- 2 pod (hay 10) cùng ăn batch của **cùng một job** → an toàn.
- Zombie sau rebalance ghi song song → vô hại.

---

## 9. Phía DB — **đã xong**, không thêm bảng/cột

`SP_EOD_SET_SOURCE_READY` nhánh `ASSET_NAV` **đã đổi thành CỜ** (commit `65cf008`):

- **Trước:** so `RECEIVED = COUNT(DISTINCT si)` *(chỉ SI SDI nhận — acc lạ bị `INNER JOIN` lọc)* với `@p_total_record` *(số Asset khai — GỒM acc lạ)*. **Hai tập khác nhau** ⇒ chỉ cần 1 acc lạ là `PENDING` **vĩnh viễn** ⇒ EOD + chain **không bao giờ chạy**.
- **Nay:** chỉ ghi cờ `READY`. `TOTAL`/`RECEIVED` lưu để **audit** (`RECEIVED < TOTAL` khi có acc lạ là **BÌNH THƯỜNG**).
- **Cửa khoá thật giữ nguyên:** `SP_EOD_RUN` → `err=12` nếu SI ACTIVE nào thiếu dòng `T_SI_BALANCE @d`.

---

## 10. Tóm tắt — 4 câu

1. **`requestId` = JOB_ID** → xoá sổ `counter==1`, `INITIAL_LOCK`, retry-loop 100s. Không còn "message đầu tiên đặc biệt", không còn điểm chết đơn.
2. **Dedup theo NỘI DUNG** (`Sha256(data)`), không theo `offset` → bịt được lỗ producer restart mà `enable.idempotence` không bịt nổi.
3. **Cộng dồn bằng Lua** (SADD + INCRBY nguyên tử) → không có cửa sổ ghi-dở.
4. **`totalRow` là gợi ý, `err=12` là cửa khoá.** Asset lỗi → chậm 5 phút, **không chết**. SDI thiếu dữ liệu → **chặn**, báo rõ.
