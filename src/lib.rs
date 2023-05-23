#[macro_use]
extern crate log;

use sophia::graph::inmem::FastGraph;
use sophia_api::serializer::TripleSerializer;
use sophia_api::triple::stream::TripleSource;
use std::error;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path;

pub fn serialize_graph(output_path: &path::PathBuf, graph: &FastGraph) -> Result<(), Box<dyn error::Error>> {
    let output = fs::File::create(&output_path).expect(format!("can't create {}", output_path.to_string_lossy()).as_str());
    info!("writing: {}", output_path.to_string_lossy());
    let writer = io::BufWriter::new(output);
    let mut serializer = sophia::serializer::nt::NtSerializer::new(writer);
    serializer.serialize_graph(graph)?;
    Ok(())
}

pub fn deserialize_graph(input_path: &path::PathBuf) -> Result<FastGraph, Box<dyn error::Error>> {
    let input = fs::File::open(&input_path).expect(format!("can't open {}", input_path.to_string_lossy()).as_str());
    info!("reading: {}", input_path.to_string_lossy());
    let reader = io::BufReader::new(input);
    let graph: FastGraph = match input_path.extension().and_then(OsStr::to_str) {
        Some("ttl") => sophia::parser::turtle::parse_bufread(reader).collect_triples().unwrap(),
        Some("nt") => sophia::parser::nt::parse_bufread(reader).collect_triples().unwrap(),
        Some("xml") | Some("rdf") | Some("owl") => sophia::parser::xml::parse_bufread(reader).collect_triples().unwrap(),
        _ => panic!("invalid extension"),
    };
    Ok(graph)
}
