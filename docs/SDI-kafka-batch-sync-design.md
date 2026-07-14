# Thiết kế đồng bộ Asset qua Kafka — nhiều pod, Redis điều phối

> **Ràng buộc:** không thêm bảng DB · nhiều pod để chia tải · Redis quản batch + trạng thái job · không so danh sách tài khoản giữa Asset và SDI ở tầng Kafka (post-check lo).
> **Thay cho:** cơ chế `COUNTER` / `TOTAL_PROCESSED` / `counter==1` / retry-loop hiện tại.

---

## 0. Nguyên tắc nền — đọc kỹ, mọi thứ dưới đây dựa vào nó

> ### Redis lo TỐC ĐỘ và ĐIỀU PHỐI. DB lo TÍNH ĐÚNG.
> **Redis mất sạch key vẫn không được làm SAI một đồng nào** — chỉ được phép làm *chậm* hoặc *treo*.

Đạt được bằng cách: **tính đúng nằm ở tính idempotent của DB**, Redis chỉ để **khỏi làm lại việc thừa** và **để biết khi nào đủ**.

Hệ quả trực tiếp: **không bao giờ dùng Redis lock để bảo vệ tính đúng.** Lock có TTL, TTL sẽ hết vào lúc tệ nhất. Lock chỉ để tiết kiệm CPU.

---

## 1. Hai câu hỏi khác nhau — đừng trộn

| Tầng | Câu hỏi | Trả lời bằng | Nếu thiếu |
|---|---|---|---|
| **Kafka + Redis** | *"Asset đã gửi đủ những gì nó **KHAI** chưa?"* | `SCARD(si đã nhận)` ≥ `totalRow` | Job chưa READY → chờ |
| **DB (post-check)** | *"SDI có đủ dữ liệu cho **tài khoản của mình** chưa?"* | `SP_EOD_RUN` → **err=12** | Chặn EOD, báo rõ số SI thiếu |

⇒ **Tầng Kafka KHÔNG cần biết registry SDI.** Asset gửi dư tài khoản lạ → `SP_INGEST_ASSET_NAV` tự lọc. Asset gửi thiếu tài khoản của SDI → `err=12` bắt được ở tầng sau.

---

## 2. Danh tính để chống trùng — **có sẵn, không cần producer sửa**

Producer hiện **chỉ gửi `totalRow`** (giống nhau mọi batch), **không có `batch_index`**. Vậy lấy gì làm danh tính?

**`si_account`.** Nó nằm sẵn trong payload, và `totalRow` chính là **tổng số bản ghi (= số tài khoản) cả ngày**.

```
Đếm theo si_account (SADD)  ⇒  trùng bao nhiêu lần cũng chỉ tính 1
So với totalRow             ⇒  biết đã đủ chưa
KHÔNG cần biết có bao nhiêu batch
```

> ⚠️ **Giả định:** 1 bản ghi = 1 `si_account` cho một ngày (khớp `UQ (C_BUSINESS_DATE, C_SI_ACCOUNT)` của `T_SI_BALANCE`). Nếu Asset gửi trùng `si_account` trong cùng ngày thì `totalRow` (đếm dòng) sẽ **lớn hơn** số tài khoản phân biệt → `SCARD` không bao giờ đạt → treo. **Cần Asset xác nhận `totalRow` = số tài khoản phân biệt.** Có guard cảnh báo ở §6.

---

## 3. Bản đồ Redis key

`P = SDI:{bizType}:{yyyyMMdd}` — TTL toàn bộ **48h**.

| Key | Kiểu | Dùng để |
|---|---|---|
| `{P}:SI` | **SET** | `si_account` đã nhận (từ **payload**, kể cả acc SDI không nhận). `SADD` → idempotent |
| `{P}:TOTAL` | STRING | `totalRow` Asset khai (ghi lần đầu bằng `SET NX`) |
| `{P}:READY` | STRING | `"1"` khi `SCARD ≥ TOTAL` — chống gọi `SP_EOD_SET_SOURCE_READY` lặp |
| `{P}:LAST_AT` | STRING | Timestamp message cuối → **watchdog** |
| `{P}:JOB_ID` | STRING | Id log job (`SET NX`) — **chỉ để log, KHÔNG chặn xử lý** |
| `SDI:CHAIN:{yyyyMMdd}` | STRING | Quyền chạy chain (`SET NX EX 60`, có gia hạn) |

**Không còn:** `COUNTER`, `COUNTER_SUCCESS`, `COUNTER_FAIL`, `TOTAL_PROCESSED`, `INITIAL_LOCK`, `END_LOCK`, `LAST_TRY_DEQUEUE`.

---

## 4. Consumer — mọi pod giống hệt nhau

**Không có "message đầu tiên đặc biệt". Không lock. Không chờ. Không chain.**

```csharp
async Task HandleAsync(ConsumeResult<string,string> r)
{
    var model  = JsonConvert.DeserializeObject<AssetKafkaModel>(r.Message.Value);
    var d      = model.Date;                 // tranDate
    var total  = model.TotalRow;             // Asset khai, giống nhau mọi batch
    var siList = model.Data.Select(x => (RedisValue)x.si_account).ToArray();
    var P      = $"SDI:{bizType}:{d:yyyyMMdd}";

    // ── 1) GHI DB TRƯỚC ────────────────────────────────────────────────
    //    SP_INGEST_ASSET_NAV: all-or-nothing + idempotent (DELETE+INSERT theo (date,si))
    //    Xử lý lại bao nhiêu lần cũng ra cùng kết quả.
    var (err, _) = await _sp.IngestAssetNavAsync(r.Message.Value, d);
    if (err != 0)
        throw new SyncException($"ingest fail err={err}");   // KHÔNG commit offset → Kafka giao lại

    // ── 2) ĐÁNH DẤU SAU KHI DB ĐÃ COMMIT ──────────────────────────────
    //    Đánh dấu TRƯỚC mà pod chết giữa chừng ⇒ message coi như xong dù chưa ghi. KHÔNG BAO GIỜ.
    var batch = _redis.CreateBatch();
    var tAdd  = batch.SetAddAsync($"{P}:SI", siList);                                  // SADD nhiều phần tử, 1 lệnh
    _         = batch.StringSetAsync($"{P}:TOTAL",   total, _ttl48h, When.NotExists);
    _         = batch.StringSetAsync($"{P}:LAST_AT", DateTime.UtcNow.Ticks, _ttl48h);
    _         = batch.KeyExpireAsync($"{P}:SI", _ttl48h);
    batch.Execute();
    await tAdd;

    // ── 3) "ĐỦ CHƯA?" LÀ MỘT PHÉP HỎI, KHÔNG PHẢI MỘT SỰ KIỆN ─────────
    long got = await _redis.SetLengthAsync($"{P}:SI");        // SCARD — O(1)
    if (got >= total)
    {
        // SET NX ⇒ chỉ đúng 1 pod gọi SP, dù cả hai cùng thấy đủ
        if (await _redis.StringSetAsync($"{P}:READY", "1", _ttl48h, When.NotExists))
        {
            await _sp.SetSourceReadyAsync(d, "ASSET_NAV", total);   // ghi cờ vào T_EOD_PIPELINE
            _log.Information("[{Biz}] ASSET_NAV READY {Date}: {Got}/{Total}", bizType, d, got, total);
        }
    }

    // XONG → commit offset. Vài chục ms. Không bao giờ bị Kafka đá.
}
```

### Điều gì đã bị xoá bỏ, và vì sao

| Bỏ | Vì |
|---|---|
| `counter == 1` để tạo `JOB_ID` | **Điểm chết đơn**: pod chết sau `INCR` → không ai còn thấy `counter==1` → job chết vĩnh viễn |
| Vòng retry `1000 × 100ms` chờ `JOB_ID` | Block consumer 100s → vượt `max.poll.interval` → **bị đá → zombie → duplicate** |
| Bắt buộc có `JOB_ID` mới được xử lý | `JOB_ID` **chỉ để log**. Không được để nó chặn dữ liệu |
| `TOTAL_PROCESSED >= totalRow` (cộng số dòng) | `numProcessed` = số dòng **ghi được** (đã lọc acc lạ) ≠ `totalRow` = số dòng **gửi đi** ⇒ **không bao giờ đạt → treo** |
| `COUNTER` làm điều kiện chốt | `INCR` **trước** khi xử lý ⇒ batch **FAIL vẫn được tính là xong** ⇒ chốt job trên data thiếu |
| `INITIAL_LOCK` / `END_LOCK` | Không cần khoá khi mọi thao tác đã idempotent theo danh tính |

---

## 5. Chain job — chạy ở mọi pod, chỉ một pod thắng

**Không chạy trong handler Kafka.** Chạy trong `BackgroundService` của **cùng pod đó** (không cần deployment riêng).

```csharp
protected override async Task ExecuteAsync(CancellationToken ct)
{
    while (!ct.IsCancellationRequested)
    {
        await Task.Delay(15_000, ct);

        // T_EOD_PIPELINE: C_ASSET_NAV_STATUS='READY' AND C_EOD_STATUS <> 'DONE'
        foreach (var d in await _sp.GetDatesNeedingChainAsync())
        {
            var key = $"SDI:CHAIN:{d:yyyyMMdd}";

            // Giành quyền: SET NX EX 60. Thua thì bỏ qua, không chờ.
            if (!await _redis.StringSetAsync(key, _podId, TimeSpan.FromSeconds(60), When.NotExists))
                continue;

            using var keepAlive = RenewEvery(TimeSpan.FromSeconds(20), key, _podId);  // gia hạn TTL
            try
            {
                await _sp.FeeRunDailyAsync(d);   // SP_FEE_RUN_DAILY  — MERGE, idempotent
                await _sp.EodRunAsync(d);        // SP_EOD_RUN        — step-gate, idempotent
                                                 //   err=12 nếu Asset thiếu SI ⇒ POST-CHECK ở đây
            }
            catch (Exception ex) { _log.Error(ex, "chain {Date}", d); }
            finally { await ReleaseIfMineAsync(key, _podId); }   // Lua: GET==podId thì DEL
        }
    }
}
```

### Vì sao TTL lock hết giữa chừng **không** gây sai

Vì công việc bên dưới **idempotent sẵn**:

- `SP_EOD_RUN` → `SP_EOD_STEP` có **resume-gate**: job đã `DONE` thì **bỏ qua**, không chạy lại.
- `SP_FEE_RUN_DAILY` → **MERGE upsert**, khoá kỳ đã/đang thu.

Hai pod cùng chạy = **lãng phí CPU, không sai số**. Lock chỉ để **tiết kiệm**, không để **bảo vệ**.

> Đây là điểm quan trọng nhất của cả thiết kế: **không có đường nào mà "Redis hỏng ⇒ tiền sai"**.

### Chain không bao giờ bị bỏ nữa

Hiện tại: `JobDone` là **bool trong RAM** của **đúng một message may mắn**. Message đó lỗi/trùng/pod chết → **sự kiện mất luôn**, không ai dựng lại → chain bị bỏ. *(Đúng triệu chứng đang gặp.)*

Bây giờ: chain được kích bởi **trạng thái trong `T_EOD_PIPELINE`**. Pod chết? Vòng quét sau 15 giây của **pod bất kỳ** vẫn thấy `READY` và chạy tiếp. **Không còn khái niệm "bỏ lỡ".**

---

## 6. Watchdog — hết treo im lặng

Cùng vòng lặp 15s, mỗi pod:

```csharp
// Chưa READY mà đã lâu không có message mới → kêu
if (!ready && (now - lastAt) > TimeSpan.FromMinutes(10))
    _log.Warning("[{Biz}] {Date} TREO: nhận {Got}/{Total} tài khoản, {Min} phút không có message mới",
                 bizType, d, scard, total, minutes);

// Nhận VƯỢT số Asset khai → Asset gửi trùng si_account, hoặc totalRow sai
if (scard > total)
    _log.Error("[{Biz}] {Date} nhận {Got} > khai {Total} — kiểm tra định nghĩa totalRow của Asset", ...);
```

---

## 7. Redis mất key thì sao — bảng trung thực

| Mất | Hậu quả | **Sai số liệu?** |
|---|---|---|
| `{P}:SI` | `SCARD` tụt → không bao giờ đạt `TOTAL` → job **treo** | ❌ **Không.** Dữ liệu trong DB **vẫn đủ**, chỉ là cờ không bật. Watchdog kêu → ops bật `READY` thủ công (hoặc Asset gửi lại — SP idempotent) |
| `{P}:READY` | Gọi lại `SP_EOD_SET_SOURCE_READY` | ❌ Không (idempotent) |
| `SDI:CHAIN:*` | 2 pod cùng chạy chain | ❌ Không (chain idempotent) |
| `{P}:JOB_ID` | Sinh thêm 1 dòng log job | ❌ Không (chỉ trùng log) |
| **Toàn bộ Redis** | Job treo, cần can thiệp tay | ❌ **Không bao giờ sai số** |

**Không có ô nào là ✅.** Đó chính là tiêu chí thiết kế ở §0.

---

## 8. Chia tải nhiều pod — giờ đã tự do

Vì mọi thao tác đều **idempotent theo danh tính**, không còn ràng buộc gì về partition:

- Partition tuỳ ý (round-robin, hoặc key = `si_account` để trải đều).
- **Không cần** ép `key = job_key` để dồn một job về một pod.
- 2 pod (hay 10 pod) cùng ăn batch của **cùng một job** → **an toàn tuyệt đối**.
- Zombie pod sau rebalance ghi song song → `DELETE+INSERT` theo `(date, si)` + `SADD` → **vô hại**.

> Chỉ khi hệ thống **không** idempotent thì mới phải hy sinh khả năng scale để đổi lấy an toàn. Ở đây thì không phải đánh đổi gì.

---

## 9. Thay đổi phía DB — **không thêm bảng nào**

Chỉ sửa **một chỗ** trong `SP_EOD_SET_SOURCE_READY`, nhánh `ASSET_NAV`:

**Hiện tại** — SP **tự quyết** đủ/thiếu bằng cách so với số Asset khai:
```sql
DECLARE @anavRecv INT = (SELECT COUNT(DISTINCT C_SI_ACCOUNT) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@p_business_date);
C_ASSET_NAV_STATUS = CASE WHEN @anavRecv >= @p_total_record THEN 'READY' ELSE 'PENDING' END
IF @anavOk=0 → err=4
```
⚠️ Sai vì `@anavRecv` chỉ đếm SI **SDI nhận** (acc lạ bị `INNER JOIN` lọc), còn `@p_total_record` là số Asset **khai** (gồm acc lạ) ⇒ **không bao giờ bằng nhau** ⇒ luôn `PENDING`.

**Đổi thành** — tầng Kafka đã quyết (Redis `SCARD ≥ totalRow`), SP chỉ **ghi cờ** (giống `MKT_DATA`):
```sql
-- Consumer chỉ gọi khi Redis xác nhận đã nhận đủ si_account so với totalRow Asset khai.
-- Đủ/thiếu so với REGISTRY SDI ⇒ post-check ở SP_EOD_RUN (err=12), KHÔNG check ở đây.
UPDATE T_EOD_PIPELINE
   SET C_ASSET_NAV_STATUS = 'READY',
       C_ASSET_NAV_TOTAL  = @p_total_record,                        -- lưu để audit
       C_ASSET_NAV_RECEIVED = (SELECT COUNT(DISTINCT C_SI_ACCOUNT)  -- lưu để audit (có thể < total: acc lạ)
                               FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@p_business_date),
       C_ASSET_NAV_AT     = GETDATE(),
       C_UPDATED_AT = GETDATE(), C_UPDATED_BY = @p_user
 WHERE C_BUSINESS_DATE = @p_business_date;
```

**Giữ nguyên** chốt chặn thật ở `SP_EOD_RUN` — đây chính là *"post check phía sau"*:
```sql
-- MỌI tiểu khoản ACTIVE phải có dòng T_SI_BALANCE @d
IF @missSI > 0 → err=12  "Thiếu Asset NAV cho N tiểu khoản ACTIVE @d — chặn EOD"
```

---

## 10. Tóm tắt — 3 câu

1. **Đừng ĐẾM. Hãy ĐÁNH DẤU** — `SADD si_account`, không `INCR`. Trùng bao nhiêu lần cũng vô hại.
2. **"Đủ chưa" là một PHÉP HỎI, không phải một SỰ KIỆN** — ai hỏi lúc nào cũng ra đúng ⇒ chain không bao giờ bị bỏ lỡ.
3. **Redis hỏng chỉ được phép làm CHẬM, không được phép làm SAI** — vì tính đúng nằm ở idempotency của DB, không nằm ở lock.
