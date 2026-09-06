# AnyStore v1 API 接入指南

本文档面向 AnyStore 的业务接入方，描述当前已实现的 v1 API。部署地址和访问凭据由服务管理方另行提供。

## 1. 接入信息

| 项目 | 值 |
|---|---|
| 服务状态 | 由服务管理方确认 |
| API 版本 | v1 |
| API Base URL | `https://your-api.example.com/api/v1`（替换为自己的部署地址） |
| 数据格式 | JSON |
| 字符编码 | UTF-8 |
| 文件存储 | 腾讯云 COS，客户端通过临时签名 URL 直传、直下 |
| 元数据存储 | CloudBase PostgreSQL |

以下示例使用：

```bash
export ANYSTORE_BASE_URL='https://your-api.example.com/api/v1'
export ANYSTORE_TOKEN='<由 AnyStore 管理方分配的 API Token>'
```

> `ANYSTORE_TOKEN` 指 AnyStore 的业务访问 Token。接入方不应获取或使用
> CloudBase PG API Key、腾讯云 SecretId 或 SecretKey。

## 2. 鉴权

除健康检查和指标接口外，所有 `/api/v1` 请求都必须携带：

```http
Authorization: Bearer <ANYSTORE_TOKEN>
```

示例：

```bash
curl "$ANYSTORE_BASE_URL/objects/root" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN"
```

Token 缺失或错误时返回：

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json
```

```json
{
  "error": "unauthorized",
  "message": "Authentication is required.",
  "request_id": "req_xxx"
}
```

### 安全要求

- 只通过 HTTPS 调用 API。
- Token 只能保存在服务端、密钥管理系统或受保护的运行时配置中。
- 不要将 Token 提交到 Git、前端源码、日志或错误上报内容。
- AnyStore 返回的 COS 签名 URL 已自带临时授权；调用该 URL 时**不要附加**
  AnyStore Bearer Token。

## 3. 通用请求规则

### 3.1 Content-Type

带 JSON 请求体的请求应发送：

```http
Content-Type: application/json
```

单个请求体最大为 **1 MiB**。超出时返回 `413` 和标准错误体。

### 3.2 请求追踪

所有响应均包含：

```http
X-Request-ID: req_xxx
```

错误响应体也包含同一个 `request_id`。提交问题时应同时提供：

- 请求时间；
- HTTP 方法和路径；
- HTTP 状态码；
- `X-Request-ID`；
- 脱敏后的请求参数。

### 3.3 时间格式

时间字段使用 UTC RFC 3339，精确到微秒：

```text
2026-09-02T01:23:45.123456Z
```

### 3.4 幂等

以下写操作支持 `Idempotency-Key`：

- `POST /objects`
- `PATCH /objects/{id}`
- `DELETE /objects/{id}`
- `POST /uploads`
- `POST /uploads/{id}/parts`
- `POST /uploads/{id}/complete`
- `DELETE /uploads/{id}`

推荐所有业务写请求都发送：

```http
Idempotency-Key: <业务唯一键>
```

推荐格式：

```text
<系统>-<业务类型>-<业务ID>-<操作>-<版本>
```

例如：

```text
order-service-report-20260902-create-v1
```

行为：

1. 相同 Key、相同请求会返回第一次请求的原始状态码和响应体。
2. 重试不会重复创建对象、增加 revision 或产生重复 Change。
3. 相同 Key 用于不同请求时返回 `409 idempotency_key_reused`。
4. 已完成记录至少保留 24 小时。

网络超时后，接入方应使用**相同 Key 和相同请求内容**重试，不要生成新 Key。

### 3.5 Revision 与并发更新

对象具有递增的 `revision`：

- 新对象从 `1` 开始；
- 每次可见修改只增加一次；
- 无变化的 PATCH 不增加 revision；
- 上传会话创建、分块 URL 分配、上传中止不增加 revision。

更新、删除或完成上传时，建议发送：

```http
If-Match: <当前 revision>
```

revision 不匹配时返回：

```http
HTTP/1.1 412 Precondition Failed
```

```json
{
  "error": "revision_conflict",
  "message": "Object has been modified.",
  "current_revision": 8,
  "request_id": "req_xxx"
}
```

接入方应重新读取对象、合并业务修改，再使用新的 revision 重试。

## 4. Object 数据结构

文件示例：

```json
{
  "id": "obj_xxx",
  "revision": 2,
  "kind": "file",
  "name": "example.pdf",
  "parent_id": "root",
  "path": "/example.pdf",
  "content_type": "application/pdf",
  "size": 123456,
  "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "content_state": "ready",
  "metadata": {
    "business_id": "report-2026-09",
    "source": "report-service"
  },
  "created_at": "2026-09-02T01:00:00.000000Z",
  "updated_at": "2026-09-02T01:05:00.000000Z"
}
```

文件夹示例：

```json
{
  "id": "obj_xxx",
  "revision": 1,
  "kind": "folder",
  "name": "reports",
  "parent_id": "root",
  "path": "/reports",
  "metadata": {},
  "created_at": "2026-09-02T01:00:00.000000Z",
  "updated_at": "2026-09-02T01:00:00.000000Z"
}
```

字段说明：

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 不可变对象 ID |
| `revision` | integer | 并发控制版本 |
| `kind` | string | `file` 或 `folder` |
| `name` | string | 当前父目录内的名称 |
| `parent_id` | string/null | 父对象 ID；根对象为 `null` |
| `path` | string | 根据目录树实时推导的完整路径 |
| `content_type` | string/null | 文件 MIME 类型 |
| `size` | integer/null | 已完成内容的字节数 |
| `sha256` | string/null | 64 位十六进制 SHA-256 |
| `content_state` | string | 文件为 `none` 或 `ready` |
| `metadata` | object | 业务自定义 JSON 元数据 |
| `created_at` | string | 创建时间 |
| `updated_at` | string | 最近修改时间 |

文件夹不会返回 `content_type`、`size`、`sha256` 和 `content_state`。

固定根对象：

```text
id        = root
name      = ""
parent_id = null
path      = /
```

根对象不能重命名、移动或删除。

### 名称约束

`name`：

- 非根对象不能为空；
- 最大 255 UTF-8 字节；
- 不能包含 `/` 或 NUL；
- 不能为 `.` 或 `..`；
- 同一父目录下必须唯一。

### Metadata 约束

- `metadata` 必须是 JSON object。
- 值可为任意 JSON 类型。
- Key 不能为空，且不能包含 NUL。
- AnyStore 只保存和查询 metadata，不解释业务含义。

建议接入方在 metadata 中保留自己的业务标识，例如：

```json
{
  "source_system": "report-service",
  "business_id": "report-20260902-001",
  "tenant_id": "tenant-01"
}
```

## 5. 接口总览

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/objects` | 创建文件或文件夹 |
| `GET` | `/objects/{id}` | 按 ID 读取对象 |
| `PATCH` | `/objects/{id}` | 重命名、移动或修改 metadata |
| `DELETE` | `/objects/{id}` | 删除对象 |
| `GET` | `/objects/{id}/children` | 列出直接子对象 |
| `GET` | `/resolve` | 按路径解析对象 |
| `POST` | `/query` | 查询系统字段和 metadata |
| `POST` | `/uploads` | 创建上传会话 |
| `POST` | `/uploads/{id}/parts` | 获取分块上传 URL |
| `POST` | `/uploads/{id}/complete` | 完成上传并提交内容 |
| `DELETE` | `/uploads/{id}` | 中止上传 |
| `GET` | `/objects/{id}/content` | 获取临时下载地址 |
| `HEAD` | `/objects/{id}/content` | 获取文件内容元信息 |
| `GET` | `/changes` | 消费有序变更流 |

## 6. Object API

### 6.1 创建文件夹

```http
POST /objects
```

```bash
curl -X POST "$ANYSTORE_BASE_URL/objects" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: reports-folder-create-v1" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "folder",
    "name": "reports",
    "parent_id": "root",
    "metadata": {
      "source_system": "report-service"
    }
  }'
```

成功返回 `201 Created` 和完整 Object。

`parent_id` 省略时默认为 `root`。

### 6.2 创建空文件对象

创建文件对象不会上传文件字节：

```bash
curl -X POST "$ANYSTORE_BASE_URL/objects" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-object-create-v1" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "file",
    "name": "report.pdf",
    "parent_id": "obj_folder",
    "content_type": "application/pdf",
    "metadata": {
      "business_id": "report-001"
    }
  }'
```

新文件：

```json
{
  "revision": 1,
  "content_state": "none",
  "size": null,
  "sha256": null
}
```

文件内容需通过第 8 节上传流程提交。

### 6.3 读取对象

```http
GET /objects/{id}
```

```bash
curl "$ANYSTORE_BASE_URL/objects/obj_xxx" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN"
```

成功返回 `200 OK`。已删除或不存在的对象返回
`404 object_not_found`。

### 6.4 修改对象

```http
PATCH /objects/{id}
```

可修改：

- `name`
- `parent_id`
- `metadata`

组合修改示例：

```bash
curl -X PATCH "$ANYSTORE_BASE_URL/objects/obj_xxx" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-update-v2" \
  -H "If-Match: 3" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "report-final.pdf",
    "parent_id": "obj_archive",
    "metadata": {
      "set": {
        "status": "final",
        "approved": true
      },
      "remove": ["draft_owner"]
    }
  }'
```

Metadata 的执行顺序为：

1. 应用 `set`；
2. 应用 `remove`。

如果同一个 Key 同时出现在 `set` 和 `remove` 中，最终会被删除。

一个 PATCH 即使修改多个类别，也只增加一次 revision；Changes 中可能产生多个
相同 revision 的记录。

空 PATCH 或实际没有变化时返回当前对象，不增加 revision。

### 6.5 删除对象

文件或空文件夹：

```bash
curl -X DELETE "$ANYSTORE_BASE_URL/objects/obj_xxx" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-delete-v1" \
  -H "If-Match: 4"
```

成功返回：

```http
204 No Content
```

非空文件夹默认返回 `409 folder_not_empty`。

递归删除：

```bash
curl -X DELETE \
  "$ANYSTORE_BASE_URL/objects/obj_folder?recursive=true" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: folder-recursive-delete-v1" \
  -H "If-Match: 1"
```

递归删除会为目录和每个后代产生独立的 `deleted` tombstone Change。

### 6.6 列出子对象

```http
GET /objects/{id}/children
```

参数：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `limit` | 100 | 1～1000，超出会被限制到最大值 |
| `cursor` | 空 | 上一页返回的不透明游标 |
| `order_by` | `name` | `name`、`created_at`、`updated_at`、`size` |
| `order` | `asc` | `asc` 或 `desc` |

```bash
curl "$ANYSTORE_BASE_URL/objects/obj_folder/children?limit=100&order_by=name&order=asc" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN"
```

响应：

```json
{
  "items": [],
  "next_cursor": null,
  "has_more": false
}
```

仅返回直接子对象，不递归返回后代。

### 6.7 按路径解析

```http
GET /resolve?path=<完整路径>
```

路径必须进行 URL 编码：

```bash
curl --get "$ANYSTORE_BASE_URL/resolve" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  --data-urlencode 'path=/reports/2026 年/report.pdf'
```

成功返回对应 Object。路径不存在时返回 `404 object_not_found`。

## 7. Metadata Query API

```http
POST /query
```

可查询：

- `kind`
- `name`
- `parent_id`
- 顶层 metadata Key

当前 metadata 操作符：

| 操作符 | 含义 |
|---|---|
| `eq` | JSON 类型和值完全相等 |
| `exists` | Key 是否存在 |

所有条件按 AND 组合：

```bash
curl -X POST "$ANYSTORE_BASE_URL/query" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "file",
    "parent_id": "obj_folder",
    "metadata": {
      "source_system": {
        "eq": "report-service"
      },
      "approved": {
        "exists": true
      }
    },
    "limit": 100,
    "cursor": null
  }'
```

响应：

```json
{
  "items": [],
  "next_cursor": null,
  "has_more": false
}
```

查询分页默认 100，最大 1000。已删除对象不会返回。

`eq` 保留 JSON 类型差异，例如数字 `2026` 不等于字符串 `"2026"`。

## 8. 文件上传

文件字节不会经过 AnyStore API 服务。标准流程为：

```text
创建 file Object
    ↓
创建 Upload Session
    ↓
客户端直接 PUT 到 COS 签名 URL
    ↓
调用 Complete
    ↓
Object content_state 变为 ready
```

当前部署参数：

| 参数 | 值 |
|---|---:|
| 单次上传阈值 | 64 MiB |
| 分块大小 | 16 MiB |
| 签名上传 URL 有效期 | 约 6 小时 |
| 上传会话内部有效期 | 24 小时 |

### 8.1 计算 SHA-256

推荐上传前计算 SHA-256。

Linux/macOS：

```bash
sha256sum report.pdf
```

Node.js：

```js
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

const bytes = await readFile("report.pdf");
const sha256 = createHash("sha256").update(bytes).digest("hex");
```

SHA-256 必须是 64 位十六进制字符串。

### 8.2 创建上传会话

```bash
curl -X POST "$ANYSTORE_BASE_URL/uploads" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-upload-create-v1" \
  -H "Content-Type: application/json" \
  -d '{
    "object_id": "obj_xxx",
    "size": 123456,
    "content_type": "application/pdf",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  }'
```

创建会话不会增加 Object revision。

### 8.3 单次上传

小于等于 64 MiB 时，响应类似：

```json
{
  "id": "upload_xxx",
  "object_id": "obj_xxx",
  "mode": "single",
  "upload": {
    "method": "PUT",
    "url": "https://temporary-signed-cos-url",
    "headers": {}
  },
  "expires_at": "2026-09-02T07:00:00.000000Z"
}
```

必须使用响应中的方法、URL 和 headers 上传原始文件字节：

```bash
curl -X PUT '<upload.url>' \
  --data-binary '@report.pdf'
```

注意：

- 不要将文件包装为 JSON、Base64 或 multipart/form-data。
- 不要向 COS URL 发送 AnyStore Bearer Token。
- 如果 `upload.headers` 非空，必须逐项原样发送。
- 相同 `Idempotency-Key` 会重放原始响应，包括原签名 URL。签名 URL 已过期时，
  应使用**新的上传会话 Idempotency-Key** 创建新会话。

### 8.4 完成单次上传

```bash
curl -X POST "$ANYSTORE_BASE_URL/uploads/upload_xxx/complete" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-upload-complete-v1" \
  -H "If-Match: 1" \
  -H "Content-Type: application/json" \
  -d '{}'
```

成功返回：

```json
{
  "object_id": "obj_xxx",
  "revision": 2,
  "content_state": "ready",
  "size": 123456,
  "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

第一次完成内容产生 `content_ready` Change；替换已有内容产生
`content_replaced` Change。

### 8.5 分块上传

大于 64 MiB 时，创建会话返回：

```json
{
  "id": "upload_xxx",
  "object_id": "obj_xxx",
  "mode": "multipart",
  "part_size": 16777216,
  "expires_at": "2026-09-02T07:00:00.000000Z"
}
```

客户端应按 `part_size` 切分文件。最后一块可以小于 `part_size`。

申请分块 URL：

```bash
curl -X POST "$ANYSTORE_BASE_URL/uploads/upload_xxx/parts" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-parts-1-4-v1" \
  -H "Content-Type: application/json" \
  -d '{
    "part_numbers": [1, 2, 3, 4]
  }'
```

响应：

```json
{
  "parts": [
    {
      "part_number": 1,
      "method": "PUT",
      "url": "https://temporary-signed-cos-url"
    }
  ]
}
```

每块上传成功后，记录 COS 返回的 `ETag`：

```bash
curl -i -X PUT '<part.url>' \
  --data-binary '@part-0001'
```

完成分块上传：

```bash
curl -X POST "$ANYSTORE_BASE_URL/uploads/upload_xxx/complete" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-multipart-complete-v1" \
  -H "If-Match: 1" \
  -H "Content-Type: application/json" \
  -d '{
    "parts": [
      {
        "part_number": 1,
        "etag": "<第 1 块返回的 ETag>"
      },
      {
        "part_number": 2,
        "etag": "<第 2 块返回的 ETag>"
      }
    ]
  }'
```

建议：

- part number 从 1 开始，保持唯一；
- 按 part number 升序提交；
- 在签名 URL 有效期内，用相同 `Idempotency-Key` 安全重试同一次 URL 分配；
- 签名 URL 过期后，用新的 `Idempotency-Key` 重新分配 URL；
- 浏览器直传时，COS CORS 需要允许 `PUT/GET/HEAD` 并暴露 `ETag` 响应头。

### 8.6 中止上传

```bash
curl -X DELETE "$ANYSTORE_BASE_URL/uploads/upload_xxx" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: report-001-upload-abort-v1"
```

成功返回 `204 No Content`。

中止上传不修改 Object revision，也不产生 Object Change。

### 8.7 替换已有内容

替换文件内容不需要创建新 Object：

1. 读取当前 Object，保存 revision；
2. 为同一个 `object_id` 创建新上传会话；
3. 上传新字节；
4. 使用步骤 1 的 revision 调用 complete。

只有 complete 成功后，内容指针和 revision 才会原子切换。失败时旧内容仍可读取。

## 9. 文件下载

### 9.1 获取内容

```http
GET /objects/{id}/content
```

AnyStore 返回临时重定向：

```http
HTTP/1.1 307 Temporary Redirect
Location: https://temporary-signed-cos-url
Cache-Control: no-store
```

curl 自动跟随：

```bash
curl -L "$ANYSTORE_BASE_URL/objects/obj_xxx/content" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -o report.pdf
```

SDK 接入时推荐显式处理：

1. 携带 Bearer Token 请求 AnyStore；
2. 读取 `307 Location`；
3. 不携带 Bearer Token 请求 COS Location。

签名 URL 短期有效，不应持久化。需要下载时重新向 AnyStore 获取。

下载文件名使用 Object 当前的 `name`。对象重命名后无需移动 COS 数据。

### 9.2 获取内容元信息

```bash
curl -I "$ANYSTORE_BASE_URL/objects/obj_xxx/content" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN"
```

响应头：

```http
Content-Type: application/pdf
Content-Length: 123456
X-AnyStore-SHA256: 0123456789abcdef...
```

文件夹返回 `409 not_a_file`。尚未完成上传的文件返回
`409 content_not_ready`。

## 10. Changes 增量同步

Changes 用于下游系统可靠地同步对象变化。

### 10.1 首次读取

```bash
curl "$ANYSTORE_BASE_URL/changes?limit=500" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN"
```

响应：

```json
{
  "items": [
    {
      "change_id": "chg_00000000000000000001",
      "object_id": "obj_xxx",
      "revision": 1,
      "action": "created",
      "changed_at": "2026-09-02T01:00:00.000000Z",
      "request_id": "req_xxx",
      "idempotency_key": "report-001-object-create-v1"
    }
  ],
  "next_cursor": "chgcur_xxx",
  "has_more": false
}
```

`limit` 默认 100，最大 1000。

### 10.2 Change action

| Action | 含义 |
|---|---|
| `created` | 创建对象 |
| `metadata_updated` | metadata 变化 |
| `renamed` | 名称变化 |
| `moved` | 父目录变化 |
| `content_ready` | 首次提交文件内容 |
| `content_replaced` | 替换已有内容 |
| `deleted` | 删除；同时有 `tombstone: true` |

一个 PATCH 同时修改名称、父目录和 metadata 时，会产生多个 Change，但使用同一个
新 revision。

### 10.3 持续轮询

接入方应持久化 `next_cursor`：

```text
读取 cursor A
    ↓
处理并持久化 items
    ↓
只有处理成功后，保存 next_cursor B
    ↓
使用 cursor B 读取下一页
```

即使页面为空，服务仍返回新的 `next_cursor`，因此轮询方必须更新游标。

示例：

```bash
curl --get "$ANYSTORE_BASE_URL/changes" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  --data-urlencode 'cursor=chgcur_xxx' \
  --data-urlencode 'limit=500'
```

同一 cursor 使用相同 limit 重读时，返回稳定的同一页和同一
`next_cursor`，可用于消费失败后的安全重试。

同一 cursor 不应改用另一个 limit。

Change 至少保留 30 天。游标超出保留窗口时返回：

```http
410 Gone
```

```json
{
  "error": "changes_cursor_expired",
  "message": "The requested Changes cursor is outside the retention window.",
  "request_id": "req_xxx"
}
```

此时接入方应执行全量同步，再从新的 cursor 开始。

## 11. 错误响应

标准错误体：

```json
{
  "error": "error_code",
  "message": "Human-readable message.",
  "request_id": "req_xxx"
}
```

错误码：

| HTTP | error | 说明 |
|---:|---|---|
| 400 | `invalid_request` | JSON、参数、Header 或请求格式错误 |
| 400 | `invalid_name` | 对象名称不合法 |
| 400 | `invalid_metadata` | metadata 不合法 |
| 401 | `unauthorized` | Bearer Token 缺失或错误 |
| 404 | `object_not_found` | 对象不存在或已删除 |
| 404 | `upload_not_found` | 上传会话不存在、已中止或已过期 |
| 409 | `name_conflict` | 同目录存在同名对象 |
| 409 | `folder_not_empty` | 删除非空目录但未指定 recursive |
| 409 | `invalid_move` | 非法移动，例如移入自身后代 |
| 409 | `not_a_folder` | 目标需要是文件夹 |
| 409 | `not_a_file` | 目标需要是文件 |
| 409 | `content_not_ready` | 文件字节尚未上传或不可读取 |
| 409 | `idempotency_key_reused` | 幂等 Key 被用于不同请求 |
| 410 | `changes_cursor_expired` | Changes 游标过期 |
| 412 | `revision_conflict` | `If-Match` 与当前 revision 不同 |
| 413 | `invalid_request` | 请求体超过 1 MiB |
| 422 | `checksum_mismatch` | 上传大小或 SHA-256 不匹配 |
| 429 | `rate_limited` | 请求或幂等争用被限流 |
| 500 | `internal_error` | 服务内部错误 |
| 502 | `storage_error` | COS 等存储后端错误 |

建议重试策略：

| 类型 | 策略 |
|---|---|
| 网络超时 | 使用相同 Idempotency-Key 重试 |
| `409 idempotency_key_reused` | 修正业务 Key 生成逻辑，不要直接重试 |
| `412 revision_conflict` | 重读对象并处理并发冲突 |
| `429 rate_limited` | 指数退避并加入随机抖动 |
| `500/502` | 使用相同 Idempotency-Key 指数退避重试，并记录 request_id |
| 其他 4xx | 修正请求，不应无条件重试 |

## 12. 完整最小接入流程

下面是一条最小可用流程：

### 第一步：创建文件对象

```bash
FILE=$(
  curl -sS -X POST "$ANYSTORE_BASE_URL/objects" \
    -H "Authorization: Bearer $ANYSTORE_TOKEN" \
    -H "Idempotency-Key: demo-file-create-v1" \
    -H "Content-Type: application/json" \
    -d '{
      "kind": "file",
      "name": "hello.txt",
      "parent_id": "root",
      "content_type": "text/plain",
      "metadata": {
        "business_id": "demo-001"
      }
    }'
)

FILE_ID=$(echo "$FILE" | jq -r .id)
REVISION=$(echo "$FILE" | jq -r .revision)
```

### 第二步：创建上传会话

```bash
printf 'hello AnyStore\n' > /tmp/hello.txt
SIZE=$(wc -c < /tmp/hello.txt)
SHA256=$(sha256sum /tmp/hello.txt | awk '{print $1}')

UPLOAD=$(
  curl -sS -X POST "$ANYSTORE_BASE_URL/uploads" \
    -H "Authorization: Bearer $ANYSTORE_TOKEN" \
    -H "Idempotency-Key: demo-file-upload-v1" \
    -H "Content-Type: application/json" \
    -d "{
      \"object_id\": \"$FILE_ID\",
      \"size\": $SIZE,
      \"content_type\": \"text/plain\",
      \"sha256\": \"$SHA256\"
    }"
)

UPLOAD_ID=$(echo "$UPLOAD" | jq -r .id)
UPLOAD_URL=$(echo "$UPLOAD" | jq -r .upload.url)
```

### 第三步：直传 COS

```bash
curl -X PUT "$UPLOAD_URL" \
  --data-binary '@/tmp/hello.txt'
```

### 第四步：提交内容

```bash
curl -X POST "$ANYSTORE_BASE_URL/uploads/$UPLOAD_ID/complete" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN" \
  -H "Idempotency-Key: demo-file-complete-v1" \
  -H "If-Match: $REVISION" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### 第五步：下载验证

```bash
curl -L "$ANYSTORE_BASE_URL/objects/$FILE_ID/content" \
  -H "Authorization: Bearer $ANYSTORE_TOKEN"
```

## 13. 运维接口

以下接口不属于业务数据 API：

| 方法 | 路径 | 鉴权 | 说明 |
|---|---|---|---|
| `GET` | `/healthz` | 无 | 返回文本 `ok` |
| `GET` | `/metrics` | 无 | Prometheus 文本指标 |

健康检查地址：

```text
https://your-api.example.com/healthz
```

## 14. 当前范围与限制

- 当前提供 v1 Object、Metadata Query、Upload、Content 和 Changes API。
- 全文内容搜索 `POST /api/v1/search` 属于 v2，**当前未实现**。
- AnyStore 不提供终端用户注册或登录接口；业务调用 Token 由服务管理方分配。
- 文件字节必须通过 COS 临时签名 URL 传输，不能直接 POST 到 AnyStore API。
- `path` 是派生字段，不能直接写入。
- `id`、`revision`、`size`、`sha256`、`content_state` 不能通过 PATCH 直接修改。
- 接入方不得依赖 COS 签名 URL 的长期有效性。

## 15. 自动化验收

仓库内提供线上自动化测试：

```bash
cd AnyStore3
export ANYSTORE_TEST_BASE_URL='https://your-api.example.com/api/v1'
tests/run_remote.sh
```

默认测试覆盖：

- 健康检查和 Bearer 鉴权；
- 对象创建和幂等重放；
- CloudBase PostgreSQL 读写；
- COS 签名上传和下载；
- 上传完成和 SHA-256 校验；
- Changes；
- Newman/Postman 核心接口；
- 测试对象和上传会话清理。

完整测试：

```bash
tests/run_remote.sh --full
```

完整模式会上传超过 64 MiB 的分块对象，建议在独立测试环境运行。
