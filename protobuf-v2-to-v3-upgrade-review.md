# Protobuf v2 → v3.7.2 升级复核报告

> 复核日期：2026-05-31
> 复核范围：raft-rs/proto/protobuf-build/compiler/、raft-rs/proto/protobuf-build/、raft-rs/proto/、raft-rs/、./
> 升级提交：`0b2499a`

---

## 复核结论：没有发现问题 ✅

经过对所有相关目录和文件的完整审查，升级没有引入功能回归或破坏性变更。

---

## 1. 依赖版本变更（Cargo.toml 文件 — 全部正确 ✅）

| 文件 | 行号 | 旧版本 | 新版本 | 状态 |
|---|---|---|---|---|
| `protobuf-build/Cargo.toml` | 20-22 | `"2"` | `"3.7.2"` | ✅ |
| `protobuf-build/compiler/Cargo.toml` | 20 | `"2"` | `"3.7.2"` | ✅ |
| `protobuf-build/tests/Cargo.toml` | 14 | `"2"` | `"3.7.2"` | ✅ |
| `raft-rs/proto/Cargo.toml` | 27 | `"2"` | `"3.7.2"` | ✅ |
| `raft-rs/Cargo.toml` | 32 | `"2"` | `"3.7.2"` | ✅ |

所有依赖统一为 `"3.7.2"`，版本一致性良好。

---

## 2. `protobuf-build/Cargo.toml` — feature 兼容性

```toml
protobuf-codec = ["protobuf-codegen", "protobuf-parse", "protobuf/with-bytes"]
```

- `with-bytes` feature 在 protobuf v3.7.2 中仍然存在，用于启用 `bytes::Bytes` 支持 ✅
- `protobuf-codegen` 和 `protobuf-parse` 在 v3.7.2 中仍然作为独立包提供 ✅

---

## 3. `protobuf-build/src/protobuf_impl.rs` — 代码生成实现

| API 使用 | 行号 | v3.7.2 兼容性 | 状态 |
|---|---|---|---|
| `protobuf::Message` trait | 8 | 仍然存在 | ✅ |
| `protobuf_codegen::CustomizeCallback` | 9 | 在 v3 中有默认 trait 方法实现 | ✅ |
| `protobuf_parse::ProtoPathBuf::new()` | 61 | 接受 `Into<String>` | ✅ |
| `FileDescriptorSet::new() + merge_from_bytes()` | 40-41 | v3 API 兼容 | ✅ |
| `check_initialized()` | 42 | 是 `Message` trait 方法 | ✅ |
| `gen_and_write::gen_and_write()` | 64-71 | 函数签名与调用匹配 | ✅ |
| `Customize::default().generate_accessors(true)` | 69 | builder 模式在 v3 中支持 | ✅ |
| 正则替换 `read_proto3_enum_with_unknown_fields_into` | 82 | 该函数在 `protobuf::rt` 中仍存在 | ✅ |

---

## 4. `protobuf-build/src/wrapper.rs` — Message trait 包装器（关键文件）

这是升级中最关键的改动。wrapper.rs 为 protobuf v3 的 `Message` trait 生成了正确的默认实现：

| 方法 | v2 API | v3.7.2 API | 生成代码是否正确 |
|---|---|---|---|
| `const NAME` | ❌ 不存在 | ✅ 必需 | `const NAME: &'static str = "";` ✅ |
| `compute_size()` | 返回 `u32` | 返回 `u64` | `-> u64` ✅ |
| `special_fields()` | ❌ 不存在 | ✅ 必需 | `fn special_fields(&self) -> &::protobuf::SpecialFields` ✅ |
| `mut_special_fields()` | ❌ 不存在 | ✅ 必需 | `fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields` ✅ |
| 错误类型 | `ProtobufError` | `protobuf::Error` | 使用 `io::Error::new(...).into()` ✅ |
| `write_to_bytes` | - | - | 正确使用 `prost::Message::encode` + 错误转换 ✅ |
| `merge_from_bytes` | - | - | 正确使用 `prost::Message::merge` + 错误转换 ✅ |

所有必需的 v3 API 方法都已实现，弃用方法已被移除 ✅

---

## 5. `protobuf-build/compiler/src/codegen.rs` — gRPC 代码生成器

| API | 行号 | v3.7.2 兼容性 | 状态 |
|---|---|---|---|
| `protobuf::descriptor::*` | 27 | 模块结构未变 | ✅ |
| `protobuf::plugin::*` | 28 | `CodeGeneratorRequest` / `CodeGeneratorResponse` 仍存在 | ✅ |
| `Message::parse_from_reader()` | 46 | 仍然是 Message trait 方法 | ✅ |
| `CodeGeneratorResponse::new()` | 48 | 仍然存在 | ✅ |
| `set_supported_features(FEATURE_PROTO3_OPTIONAL as u64)` | 49 | v3 API 中仍兼容 | ✅ |
| `write_to_writer()` | 59 | Message trait 方法 | ✅ |

`fd.package()`, `fd.message_type`, `fd.service` 等字段访问器仍使用 `protobuf::descriptor` 的 getter API，与 v3 兼容 ✅

---

## 6. `protobuf-build/compiler/src/prost_codegen.rs` — Prost 代码生成器

该文件使用 `prost::Message`（不是 `protobuf::Message`），不依赖于 protobuf v2 API，因此不受升级影响 ✅

---

## 7. `raft-rs/src/errors.rs` — 关键错误类型修复

```
-    CodecError(#[from] protobuf::ProtobufError),
+    CodecError(#[from] protobuf::Error),
```

- 在 protobuf v2 中，错误类型是 `protobuf::ProtobufError`
- 在 protobuf v3.7.2 中，错误类型已重命名为 `protobuf::Error`
- **这个更改是正确的** ✅

---

## 8. `raft-rs/src/lib.rs` — lint 修复

```
#![allow(unexpected_cfgs)]
```

- 这是 Rust 1.80+ 中引入的 lint，用于抑制 prost 生成代码中出现的非预期 cfg 警告 ✅
- 与 protobuf 升级无直接关联，但不影响功能 ✅

---

## 9. `protobuf-build/src/lib.rs` — protoc 版本检查

```rust
if (major, minor) >= (3, 1) {
    return Ok(protoc.to_owned());
}
```

- 要求 protoc >= 3.1.0，与 protobuf v3.7.2 兼容 ✅
- 非 Windows 平台使用 `protobuf-src = "1.1.0"` 获取 protoc 二进制 ✅
- Windows 使用捆绑的 `protoc-win32.exe` ✅

**小问题（非功能性）**：版本检查正则 `([0-9]+)\.([0-9]+)(\.[0-9])?` 最多只捕获一位 patch 版本号，例如 `3.21.12` 会被识别为 `3.21.1`。但由于只检查 major >= 3, minor >= 1，这不影响实际运行。

---

## 10. 外部 crate 之间的关系

```
rmqtt-raft (build.rs uses tonic-build 0.9)   ← 不受影响，使用独立的 prost/tonic 生态
  └─ tikv-raft (= rmqtt-raft-core 0.8.0)
       └─ raft-proto (= rmqtt-raft-proto 0.8.0)
            ├─ protobuf = "3.7.2"
            └─ protobuf-build (= rmqtt-protobuf-build 0.16.0)
                 ├─ protobuf = "3.7.2"
                 ├─ protobuf-codegen = "3.7.2"
                 └─ protobuf-parse = "3.7.2"
```

依赖链清晰，版本统一为 `3.7.2` ✅

---

## 复核总表

| 检查项 | 结果 |
|---|---|
| 所有依赖版本统一为 3.7.2 | ✅ |
| 错误类型正确迁移（`ProtobufError` → `Error`） | ✅ |
| `Message` trait impl 方法完整覆盖 v3 API | ✅ |
| gRPC 代码生成器兼容 v3 descriptor API | ✅ |
| protoc 版本检查兼容 v3 | ✅ |
| protobuf 功能 flag（`with-bytes`）兼容 | ✅ |
| prost 代码路径不受影响 | ✅ |
| 根 crate（tonic 生态）不受影响 | ✅ |

**升级完全正确，没有遗漏，也没有改变原有功能。** 所有 API 调用都符合 protobuf v3.7.2 的要求，`wrapper.rs` 中的 `Message` trait 实现对 v3 新增的 `special_fields()/mut_special_fields()` 和 `const NAME` 都提供了正确的默认实现。
