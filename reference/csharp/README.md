# Refactor consumer Kafka — bản tham chiếu

> Code C# **không nằm trong repo này** (nó ở `sdi-core-messaging-process`). Đây là **bản tham chiếu** viết lại theo thiết kế ở [`docs/SDI-kafka-batch-sync-design.md`](../../docs/SDI-kafka-batch-sync-design.md), giữ nguyên tên class/method/interface để **drop vào là chạy**.

## File

| File | Thay cho |
|---|---|
| `KafkaSyncKeys.cs` | (mới) khoá Redis + **script Lua cộng dồn nguyên tử** |
| `BatchSyncService.cs` | **`BaseService.SyncEodDataGeneric`** |
| `SyncAssetDataService.cs` | **`SyncAssetDataService.ProcessDataSyncAssetSdiCore`** |
| `EodJobWatchdogService.cs` | (mới) timeout fallback + hết treo im lặng |
| `EodStepRunner.cs` | (mới) chạy từng step tường minh — **thay chain nối trong handler** |

## Cần bổ sung (tự viết theo repo sẵn có)

```csharp
public interface ISdiDbGateway
{
    Task<(int Err, string? Msg)> SetSourceReadyAsync(string tranDate, string source, long totalRecord);
    Task<(int Err, string? Msg)> FeeRunDailyAsync(DateTime d);      // SP_FEE_RUN_DAILY
    Task<(int Err, string? Msg)> EodRunIndexAsync(DateTime d);      // SP_EOD_RUN_INDEX
    Task<(int Err, string? Msg)> EodRunAsync(DateTime d);           // SP_EOD_RUN
}
```

## Đăng ký DI

```csharp
services.AddSingleton<BatchSyncService>();
services.AddSingleton<EodStepRunner>();
services.AddHostedService<EodJobWatchdogService>();   // chạy trên MỌI pod — an toàn, idempotent
```

## Cấu hình Kafka

### ✅ Của SDI — bắt buộc, và mày kiểm soát 100%

```properties
enable.auto.commit = false       # Consumer: commit TAY, SAU KHI xử lý xong
```

Commit offset **sau khi** `ProcessDataSyncAssetSdiCore` trả về không ném. Ném ⇒ **không commit** ⇒ Kafka giao lại ⇒ đúng ý đồ (SP idempotent nên xử lý lại là vô hại).

### ⚠️ Của Asset — **KHÔNG phụ thuộc, không cần xin**

```properties
enable.idempotence = true        # Producer của ASSET. Có thì tốt. KHÔNG có cũng KHÔNG SAO.
```

**Thiết kế này không dựa vào nó.** Dedup theo **danh tính nghiệp vụ** (`Sha256(tập si_account đã sắp xếp)`) — Asset gửi lại từ PID nào, offset nào, broker nào, serialize kiểu gì, **nội dung vẫn thế** → cùng khoá → bắt được.

> **Đừng bao giờ đặt tính đúng của hệ mình vào một cấu hình nằm trong tay hệ khác.**

---

## Đã xoá gì, và vì sao

| Xoá | Lý do |
|---|---|
| `COUNTER` + `if (counter == 1)` tạo `JOB_ID` | **Điểm chết đơn**: pod `INCR` lên 1 rồi chết trước khi ghi `JOB_ID` ⇒ không ai còn thấy `counter==1` ⇒ **job chết vĩnh viễn**. Nay `JOB_ID = requestId` (Asset gửi sẵn trong MỌI message) |
| `INITIAL_LOCK` (`LockTake` 5s, **không có `else`**) | Lấy hụt lock (lock sót từ lần chạy trước) ⇒ **im lặng bỏ qua** ⇒ `JOB_ID` không bao giờ được tạo |
| Vòng retry `1000 × 100ms` chờ `JOB_ID` | Block consumer **100 giây** ⇒ vượt `max.poll.interval` ⇒ **Kafka đá pod**. Kafka **không giết thread** — pod cũ thành **zombie**, vẫn ghi DB, trong khi pod mới xử lý **lại** cùng message ⇒ duplicate ⇒ vòng xoáy |
| `TOTAL_PROCESSED >= totalRow` | `numProcessed` = `@p_rows` = số dòng **GHI ĐƯỢC** (đã lọc acc không thuộc SDI), còn `totalRow` = số dòng **GỬI ĐI** ⇒ **hai tập khác nhau** ⇒ chỉ cần Asset có **1** tài khoản lạ là tổng **không bao giờ đạt** ⇒ `JobDone` luôn `false` ⇒ **chain luôn bị bỏ** |
| `COUNTER` làm điều kiện chốt (bản sau) | `INCR` chạy **trước** `executeFunc` ⇒ batch **INSERT LỖI vẫn được tính là xong** ⇒ chốt job trên **dữ liệu thiếu**. Tệ hơn treo |
| `END_LOCK` + double-check + `JobDone` (bool RAM) | Chain gắn vào biến RAM của **đúng một message may mắn** ⇒ message đó lỗi/trùng/pod chết là **mất luôn**, không ai dựng lại ⇒ **"thường xuyên bỏ qua chain"** |
| Chain (`fee accrue` / `fee charge`) trong handler | Việc nặng trong consumer = **tự sát** (xem dòng retry-loop ở trên) |

## Thay bằng gì

| Vấn đề | Cách mới |
|---|---|
| Tạo `JOB_ID` | `requestId` — Asset gửi sẵn, **chung cho mọi batch**. Không tạo, không lock, không chờ |
| Đếm hoàn thành | **Lua nguyên tử**: `SADD Sha256(data)` → nếu mới thì `INCRBY rows`. Trùng ⇒ **không cộng** |
| Dedup | Theo **NỘI DUNG** (`Sha256(data)`), **không** theo `(partition, offset)` — vì `enable.idempotence` **không sống sót qua producer restart** |
| `totalRow` sai / Asset lỗi | **Timeout 5 phút** trong watchdog → vẫn bật cờ + log WARNING. **Không treo** |
| Cửa khoá thật | **`SP_EOD_RUN` err=12** — mọi SI ACTIVE của SDI phải có dòng `T_SI_BALANCE @d`. Đọc registry của **chính SDI**, không phụ thuộc con số nào của Asset |
| Chain | `EodStepRunner` — gọi tường minh từng step, **ngoài** Kafka handler |

---

---

## ★ Tính đúng KHÔNG phụ thuộc Asset, Kafka, hay Redis

Đây là điều quan trọng nhất. **Toàn bộ tính đúng nằm trong 2 thứ SDI kiểm soát 100%:**

| # | Cơ chế | Nằm ở đâu |
|---|---|---|
| **①** | **Ghi idempotent** — `SP_INGEST_ASSET_NAV` `DELETE+INSERT` theo `(date, si)` | DB của SDI |
| **②** | **Cửa khoá** — `SP_EOD_RUN` **err=12**: mọi SI ACTIVE của SDI phải có dòng `T_SI_BALANCE @d`, đọc từ registry của **chính SDI** | DB của SDI |

Ném mọi kiểu lỗi của Asset/Kafka/Redis vào — **không có ô nào làm sai số liệu**:

| Hỏng gì | Hậu quả | Sai số? |
|---|---|---|
| Asset **không bật** `enable.idempotence` | Dedup theo nội dung vẫn bắt được | ❌ |
| Asset gửi **trùng** batch (PID mới, offset mới) | Cùng tập `si_account` → cùng khoá → **không cộng lại** | ❌ |
| Asset khai `totalRow` **sai** | Cờ READY bật sớm/muộn → EOD thử → err=12 chặn nếu thiếu | ❌ |
| Asset gửi **thiếu** tài khoản của SDI | **err=12 → CHẶN**, báo rõ thiếu bao nhiêu | ❌ (chặn đúng) |
| Asset gửi **dư** acc không thuộc SDI | `INNER JOIN` registry lọc bỏ | ❌ |
| Kafka rebalance → **zombie pod** ghi song song | `DELETE+INSERT` theo `(date, si)` → **vô hại** | ❌ |
| **Redis mất sạch key** | Job treo → watchdog kêu → bật cờ tay | ❌ (chậm, không sai) |
| Dedup **thủng hoàn toàn** | Cộng dồn 2 lần → READY sớm → EOD → **err=12 chặn** | ❌ |

⇒ **Dedup và bộ đếm chỉ là TỐI ƯU TỐC ĐỘ.** Chúng quyết định *"khi nào thử chạy EOD"*, **không** quyết định *"dữ liệu có đúng không"*.

---

## Ba nguyên tắc

1. **Đừng ĐẾM LƯỢT GHÉ — hãy ĐÁNH DẤU DANH TÍNH rồi mới cộng.** Kafka chỉ hứa *"ít nhất một lần"*, không bao giờ hứa *"đúng một lần"*.
2. **"Đủ chưa" là một PHÉP HỎI, không phải một SỰ KIỆN.** Ai hỏi lúc nào cũng ra đúng ⇒ chain không bao giờ bị bỏ lỡ.
3. **Đừng để tính đúng của hệ mình phụ thuộc vào hệ khác.** Nhận gợi ý từ họ (nhanh), nhưng **quyết định bằng dữ liệu của mình** (đúng).
