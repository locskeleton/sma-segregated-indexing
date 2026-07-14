# Bản DROP-IN — sửa thẳng vào hàm generic cũ

> Dùng **cái này** nếu muốn ghép nhanh vào luồng hiện tại.
> Bản đầy đủ (`../`) tách thành 7 file — đúng hơn về kiến trúc nhưng phải sửa nhiều.

## Ghép vào: 1 file + 2 chỗ ở call-site

### ① Thay method `SyncEodDataGeneric` trong `BaseService<T>`
Copy `SyncEodDataGeneric.cs`. **Giữ nguyên chữ ký, nguyên `JobResult`.**

### ② `BulkInsertAssetToSdiCore` — đổi giá trị thứ 3 trả về

```diff
- return (true, totalRow, rowsWritten);        // ❌ @p_rows = số dòng GHI ĐƯỢC
+ return (true, totalRow, model.data.Count);   // ✅ số dòng TRONG PAYLOAD
```

> **Đây là sửa quan trọng nhất.** `SP_INGEST_ASSET_NAV` có `INNER JOIN` registry ⇒ acc **không thuộc SDI bị lọc bỏ** ⇒ `@p_rows < số item Asset gửi`. Còn `totalRow` là số Asset **gửi đi**.
> ⇒ `Σ(@p_rows)` **không bao giờ đạt** `totalRow` ⇒ `JobDone` **luôn false** ⇒ **chain luôn bị bỏ**.
> Chỉ cần Asset có **một** tài khoản lạ là job treo vĩnh viễn. Đây chính là bug đang gặp.

### ③ `ProcessDataSyncAssetSdiCore` — bật cờ, bỏ chain

```diff
  var job01 = await SyncEodDataGeneric(rawJson, tranDate, RedisKeyDefine.EOD_ASSET,
                                       BizTypeDefine.JOB_EOD_ASSET, BulkInsertAssetToSdiCore);
  if (!job01.JobDone) return;

+ await _bo.SetSourceReady(tranDate, "ASSET_NAV", totalRow);   // SP_EOD_SET_SOURCE_READY → cờ READY

- var job02 = await SyncEodDataGeneric(... ProcessJobAumFeeAcccrue ...);
- var job03 = await SyncEodDataGeneric(... ProcessJobAumFeeCharge ...);
```

Chain chuyển sang **cron/scheduler** gọi thẳng SP:
```
SP_FEE_RUN_DAILY @d     — mọi ngày lịch
SP_EOD_RUN_INDEX @d     — ngày GD
SP_EOD_RUN       @d     — ngày GD (err=12 nếu thiếu SI của SDI ⇐ CỬA KHOÁ THẬT)
```

> Chain chạy **trong** Kafka handler ⇒ block consumer ⇒ vượt `max.poll.interval` ⇒ **Kafka đá pod** (nhưng **không giết thread** — pod cũ thành **zombie**, vẫn ghi DB) ⇒ pod mới xử lý **lại** ⇒ duplicate ⇒ vòng xoáy.

### ④ Consumer: **không commit offset khi batch fail**
`BatchDone = false` ⇒ đừng commit ⇒ Kafka giao lại (SP idempotent nên xử lý lại vô hại). Nuốt lỗi = **mất dữ liệu**.

---

## Job 02/03 (fee) — **KHÔNG SỬA GÌ**

`executeFunc` trả `(true, 1, 1)` → `totalRow=1`, `rows=1` → `1 >= 1` → `JobDone=true`. Chạy y như cũ.

---

## Đã xoá gì trong hàm generic

| Xoá | Vì sao |
|---|---|
| `COUNTER` + `if (counter == 1)` tạo `JOB_ID` | **Điểm chết đơn**: pod `INCR` lên 1 rồi **chết** trước khi ghi `JOB_ID` ⇒ không ai còn thấy `counter==1` ⇒ **job chết vĩnh viễn**. Nay: `SET NX` — ai đặt được thì tạo, thua thì đọc |
| `LockTake(INITIAL_LOCK, 5s)` **không có `else`** | Lấy hụt lock (lock sót từ lần trước) ⇒ **im lặng bỏ qua** ⇒ `JOB_ID` không bao giờ được tạo |
| Vòng retry `1000 × 100ms` chờ `JOB_ID` | Block **100 giây** ⇒ Kafka đá pod ⇒ zombie ⇒ duplicate. Nay `jobId` **chỉ để log**, không bao giờ chặn việc ghi dữ liệu |
| `TOTAL_PROCESSED += numProcessed` | Phép cộng **không idempotent**. Kafka hứa *"ít nhất một lần"* ⇒ giao lại ⇒ **cộng 2 lần** ⇒ đủ **sớm** ⇒ chốt job khi batch cuối **chưa hề tới** |
| `COUNTER` làm điều kiện chốt | `INCR` chạy **trước** `executeFunc` ⇒ batch **INSERT LỖI vẫn được tính là xong** ⇒ chốt job trên **data thiếu**. Tệ hơn treo |
| `END_LOCK` + double-check | Không cần khoá khi mọi thao tác đã idempotent theo danh tính |
| **`KeyDeleteAsync(...)` khi job xong** | Batch **tới muộn** không thấy `JOB_ID` → spin → `executeFunc` **không được gọi** ⇒ **dữ liệu batch đó không bao giờ vào DB**, job vẫn báo DONE. **Việc "dọn dẹp" đã giết dữ liệu.** Nay: **TTL tự hết hạn** |

## Thay bằng

```lua
-- CỘNG DỒN NGUYÊN TỬ (1 round-trip). Tách 2 lệnh thì pod chết giữa SADD và INCRBY ⇒ TREO VĨNH VIỄN.
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then      -- batch MỚI?
    return redis.call('INCRBY', KEYS[2], ARGV[2])      -- → cộng
else
    return tonumber(redis.call('GET', KEYS[2]) or '0') -- → TRÙNG, KHÔNG cộng
end
```

`batchKey = Sha256(msg)` — danh tính **nội dung**. Cùng batch gửi lại (offset khác, PID khác, broker khác) → **cùng khoá** → bắt được. Không phụ thuộc `enable.idempotence` bên Asset.

---

## ⚠️ Còn thiếu (chấp nhận được, nhưng phải biết)

Bản tối giản này **không có watchdog**. Nếu Asset khai `totalRow` **sai/thừa**, hoặc **thiếu hẳn một batch**:

```
rows < totalRow → cờ ASSET_NAV không bao giờ bật → EOD treo, IM LẶNG
```

**Tối thiểu phải có alert**: cron gọi `SP_EOD_RUN` sẽ trả `err=10` lặp mãi → **alert trên chuỗi đó**. Rồi ops gọi tay `SP_EOD_SET_SOURCE_READY` để gỡ.

Muốn tự lành thì lấy `EodJobWatchdogService.cs` ở thư mục cha.
