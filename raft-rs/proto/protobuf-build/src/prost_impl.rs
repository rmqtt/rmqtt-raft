use crate::wrapper::WrapperGen;
use crate::{get_protoc, Builder};

impl Builder {
    pub fn generate_files(&self) {
        std::env::set_var("PROTOC", get_protoc());

        #[cfg(feature = "grpcio-prost-codec")]
        {
            grpcio_compiler::prost_codegen::compile_protos(
                &self.files,
                &self.includes,
                &self.out_dir,
            )
            .unwrap();
        }
        #[cfg(not(feature = "grpcio-prost-codec"))]
        {
            prost_build::Config::new()
                .out_dir(&self.out_dir)
                .compile_protos(&self.files, &self.includes)
                .unwrap();
        }

        self.list_rs_files()
            .for_each(|path| WrapperGen::new(path, self.wrapper_opts).write());
    }
}
