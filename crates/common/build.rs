fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/log.proto");
    tonic_build::compile_protos("../../proto/log.proto")?;
    tonic_build::compile_protos("../../proto/pubsub.proto")?;
    Ok(())
}