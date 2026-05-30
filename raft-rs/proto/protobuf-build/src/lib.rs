// Copyright 2019 PingCAP, Inc.

//! Utility functions for generating Rust code from protobuf specifications.
//!
//! These functions panic liberally, they are designed to be used from build
//! scripts, not in production.

#[cfg(feature = "prost-codec")]
mod wrapper;

#[cfg(feature = "protobuf-codec")]
mod protobuf_impl;

#[cfg(feature = "prost-codec")]
mod prost_impl;

use bitflags::bitflags;
use regex::Regex;
use std::env;
use std::env::var;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::from_utf8;

// We use system protoc when its version matches,
// otherwise use the protoc from bin which we bundle with the crate.
fn get_protoc() -> String {
    // $PROTOC overrides everything; if it isn't a useful version then fail.
    if let Ok(s) = var("PROTOC") {
        check_protoc_version(&s).expect("PROTOC version not usable");
        return s;
    }

    if let Ok(s) = check_protoc_version("protoc") {
        return s;
    }

    // The bundled protoc should always match the version
    #[cfg(windows)]
    {
        let bin_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("bin")
            .join("protoc-win32.exe");
        bin_path.display().to_string()
    }

    #[cfg(not(windows))]
    protobuf_src::protoc().display().to_string()
}

fn check_protoc_version(protoc: &str) -> Result<String, ()> {
    let ver_re = Regex::new(r"([0-9]+)\.([0-9]+)(\.[0-9])?").unwrap();
    let output = Command::new(protoc).arg("--version").output();
    match output {
        Ok(o) => {
            let caps = ver_re.captures(from_utf8(&o.stdout).unwrap()).unwrap();
            let major = caps.get(1).unwrap().as_str().parse::<i16>().unwrap();
            let minor = caps.get(2).unwrap().as_str().parse::<i16>().unwrap();
            if (major, minor) >= (3, 1) {
                return Ok(protoc.to_owned());
            }
            println!("The system `protoc` version mismatch, require >= 3.1.0, got {}.{}.x, fallback to the bundled `protoc`", major, minor);
        }
        Err(_) => println!("`protoc` not in PATH, try using the bundled protoc"),
    };

    Err(())
}

pub struct Builder {
    files: Vec<String>,
    includes: Vec<String>,
    black_list: Vec<String>,
    out_dir: String,
    #[cfg(feature = "prost-codec")]
    wrapper_opts: GenOpt,
    package_name: Option<String>,
    #[cfg(feature = "grpcio-protobuf-codec")]
    re_export_services: bool,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            files: Vec::new(),
            includes: vec!["include".to_owned(), "proto".to_owned()],
            black_list: vec![
                "protobuf".to_owned(),
                "google".to_owned(),
                "gogoproto".to_owned(),
            ],
            out_dir: format!("{}/protos", var("OUT_DIR").expect("No OUT_DIR defined")),
            #[cfg(feature = "prost-codec")]
            wrapper_opts: GenOpt::all(),
            package_name: None,
            #[cfg(feature = "grpcio-protobuf-codec")]
            re_export_services: true,
        }
    }

    pub fn include_google_protos(&mut self) -> &mut Self {
        let path = format!("{}/include", env!("CARGO_MANIFEST_DIR"));
        self.includes.push(path);
        self
    }

    pub fn generate(&self) {
        assert!(!self.files.is_empty(), "No files specified for generation");
        self.prep_out_dir();
        self.generate_files();
        self.generate_mod_file();
    }

    /// This option is only used when generating Prost code. Otherwise, it is
    /// silently ignored.
    #[cfg(feature = "prost-codec")]
    pub fn wrapper_options(&mut self, wrapper_opts: GenOpt) -> &mut Self {
        self.wrapper_opts = wrapper_opts;
        self
    }

    /// Finds proto files to operate on in the `proto_dir` directory.
    pub fn search_dir_for_protos(&mut self, proto_dir: &str) -> &mut Self {
        self.files = fs::read_dir(proto_dir)
            .expect("Couldn't read proto directory")
            .filter_map(|e| {
                let e = e.expect("Couldn't list file");
                if e.file_type().expect("File broken").is_dir() {
                    None
                } else {
                    Some(format!("{}/{}", proto_dir, e.file_name().to_string_lossy()))
                }
            })
            .collect();
        self
    }

    pub fn files<T: ToString>(&mut self, files: &[T]) -> &mut Self {
        self.files = files.iter().map(|t| t.to_string()).collect();
        self
    }

    pub fn includes<T: ToString>(&mut self, includes: &[T]) -> &mut Self {
        self.includes = includes.iter().map(|t| t.to_string()).collect();
        self
    }

    pub fn append_include(&mut self, include: impl Into<String>) -> &mut Self {
        self.includes.push(include.into());
        self
    }

    pub fn black_list<T: ToString>(&mut self, black_list: &[T]) -> &mut Self {
        self.black_list = black_list.iter().map(|t| t.to_string()).collect();
        self
    }

    /// Add the name of an include file to the builder's black list.
    ///
    /// Files named on the black list are not made modules of the generated
    /// program.
    pub fn append_to_black_list(&mut self, include: impl Into<String>) -> &mut Self {
        self.black_list.push(include.into());
        self
    }

    pub fn out_dir(&mut self, out_dir: impl Into<String>) -> &mut Self {
        self.out_dir = out_dir.into();
        self
    }

    /// If specified, a module with the given name will be generated which re-exports
    /// all generated items.
    ///
    /// This is ignored by Prost, since Prost uses the package names of protocols
    /// in any case.
    pub fn package_name(&mut self, package_name: impl Into<String>) -> &mut Self {
        self.package_name = Some(package_name.into());
        self
    }

    /// Whether services defined in separate modules should be re-exported from
    /// their corresponding module. Default is `true`.
    #[cfg(feature = "grpcio-protobuf-codec")]
    pub fn re_export_services(&mut self, re_export_services: bool) -> &mut Self {
        self.re_export_services = re_export_services;
        self
    }

    fn generate_mod_file(&self) {
        let mut f = File::create(format!("{}/mod.rs", self.out_dir)).unwrap();

        let modules = self.list_rs_files().filter_map(|path| {
            let name = path.file_stem().unwrap().to_str().unwrap();
            if name.starts_with("wrapper_")
                || name == "mod"
                || self.black_list.iter().any(|i| name.contains(i))
            {
                return None;
            }
            Some((name.replace('-', "_"), name.to_owned()))
        });

        let mut exports = String::new();
        for (module, file_name) in modules {
            if cfg!(feature = "protobuf-codec") {
                if self.package_name.is_some() {
                    writeln!(exports, "pub use super::{}::*;", module).unwrap();
                } else {
                    writeln!(f, "pub ").unwrap();
                }
                writeln!(f, "mod {};", module).unwrap();
                continue;
            }

            let mut level = 0;
            for part in module.split('.') {
                writeln!(f, "pub mod {} {{", part).unwrap();
                level += 1;
            }
            writeln!(f, "include!(\"{}.rs\");", file_name,).unwrap();
            if Path::new(&format!("{}/wrapper_{}.rs", self.out_dir, file_name)).exists() {
                writeln!(f, "include!(\"wrapper_{}.rs\");", file_name,).unwrap();
            }
            writeln!(f, "{}", "}\n".repeat(level)).unwrap();
        }

        if !exports.is_empty() {
            writeln!(
                f,
                "pub mod {} {{ {} }}",
                self.package_name.as_ref().unwrap(),
                exports
            )
            .unwrap();
        }
    }

    fn prep_out_dir(&self) {
        if Path::new(&self.out_dir).exists() {
            fs::remove_dir_all(&self.out_dir).unwrap();
        }
        fs::create_dir_all(&self.out_dir).unwrap();
    }

    // List all `.rs` files in `self.out_dir`.
    fn list_rs_files(&self) -> impl Iterator<Item = PathBuf> {
        fs::read_dir(&self.out_dir)
            .expect("Couldn't read directory")
            .filter_map(|e| {
                let path = e.expect("Couldn't list file").path();
                if path.extension() == Some(std::ffi::OsStr::new("rs")) {
                    Some(path)
                } else {
                    None
                }
            })
    }
}

impl Default for Builder {
    fn default() -> Builder {
        Builder::new()
    }
}

bitflags! {
    pub struct GenOpt: u32 {
        /// Generate implementation for trait `::protobuf::Message`.
        const MESSAGE = 0b0000_0001;
        /// Generate getters.
        const TRIVIAL_GET = 0b0000_0010;
        /// Generate setters.
        const TRIVIAL_SET = 0b0000_0100;
        /// Generate the `new_` constructors.
        const NEW = 0b0000_1000;
        /// Generate `clear_*` functions.
        const CLEAR = 0b0001_0000;
        /// Generate `has_*` functions.
        const HAS = 0b0010_0000;
        /// Generate mutable getters.
        const MUT = 0b0100_0000;
        /// Generate `take_*` functions.
        const TAKE = 0b1000_0000;
        /// Except `impl protobuf::Message`.
        const NO_MSG = Self::TRIVIAL_GET.bits
         | Self::TRIVIAL_SET.bits
         | Self::CLEAR.bits
         | Self::HAS.bits
         | Self::MUT.bits
         | Self::TAKE.bits;
        /// Except `new_` and `impl protobuf::Message`.
        const ACCESSOR = Self::TRIVIAL_GET.bits
         | Self::TRIVIAL_SET.bits
         | Self::MUT.bits
         | Self::TAKE.bits;
    }
}
