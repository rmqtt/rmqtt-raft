// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use rmqtt_grpcio_compiler::prost_codegen;

fn main() {
    prost_codegen::protoc_gen_grpc_rust_main();
}
