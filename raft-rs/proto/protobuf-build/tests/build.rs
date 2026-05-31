// Copyright 2019 PingCAP, Inc.

use rmqtt_protobuf_build::Builder;

fn main() {
    Builder::new().search_dir_for_protos("proto").generate()
}
