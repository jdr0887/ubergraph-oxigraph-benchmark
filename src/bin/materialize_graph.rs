#[macro_use]
extern crate log;

use clap::Parser;
use futures::future::join_all;
use reqwest::redirect::Policy;
use reqwest::Client;
use sophia::graph::inmem::FastGraph;
use sophia::graph::Graph;
use sophia::graph::MutableGraph;
use sophia::ns;
use sophia::term;
use sophia_api::term::TTerm;
use sophia_api::triple::stream::TripleSource;
use sophia_api::triple::Triple;
use std::collections::HashMap;
use std::error;
use std::fs;
use std::io;
use std::io::Write;
use std::path;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long = "input", long_help = "input")]
    input: path::PathBuf,

    #[clap(short = 'o', long = "output", long_help = "output")]
    output: path::PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let output_graph = base_ontology(&options.input).await?;
    ubergraph_oxigraph_benchmark::serialize_graph(&options.output, &output_graph)?;

    Ok(())
}

async fn base_ontology(ontologies_path: &path::PathBuf) -> Result<FastGraph, Box<dyn error::Error>> {
    let mut output_graph = FastGraph::new();

    let graph = ubergraph_oxigraph_benchmark::deserialize_graph(&ontologies_path)?;

    let owl_ns = ns::Namespace::new("http://www.w3.org/2002/07/owl#")?;
    let owl_ontology = owl_ns.get("Ontology")?;
    let owl_imports = owl_ns.get("imports")?;

    let potential_base_ontology_triples: Vec<String> =
        graph.triples_with_po(&ns::rdf::type_, &owl_ontology).map_triples(|t| t.s().value().to_string()).into_iter().collect::<Result<Vec<String>, _>>()?;

    let base_ontology = term::SimpleIri::new(potential_base_ontology_triples.iter().next().unwrap(), None).unwrap();
    debug!("base_ontology: {:?}", base_ontology);

    if let Some(home_dir) = dirs::home_dir() {
        let owl_import_dir = home_dir.join(".owl");
        if !owl_import_dir.exists() {
            fs::create_dir_all(owl_import_dir.as_path())?;
        }

        let import_url_and_dest_on_fs_map: HashMap<String, path::PathBuf> = graph
            .triples_with_p(&owl_imports)
            .map_triples(|t| {
                let import: String = t.o().value().to_string();
                let owl_url = import.replace("http://", "").replace("<", "").replace(">", "");
                (import.clone(), owl_import_dir.clone().join(owl_url))
            })
            .into_iter()
            .collect::<Result<HashMap<String, path::PathBuf>, _>>()?;

        let client = reqwest::ClientBuilder::new().redirect(Policy::default()).build()?;
        let future_responses: Vec<_> = import_url_and_dest_on_fs_map.iter().map(|(import_url, path_on_fs)| fetch_import(&client, import_url.as_str(), &path_on_fs)).collect();
        join_all(future_responses).await;

        graph.triples_with_p(&owl_imports).for_each_triple(|t| {
            let import: String = t.o().value().to_string();
            let owl_on_fs = import_url_and_dest_on_fs_map.get(import.as_str()).expect(format!("Could not get PathBuf for: {:?}", import).as_str());
            let tmp_graph = ubergraph_oxigraph_benchmark::deserialize_graph(&owl_on_fs).unwrap();
            tmp_graph
                .triples()
                .for_each_triple(|inner_triple| {
                    if inner_triple.s().kind() == term::TermKind::BlankNode && inner_triple.o().kind() == term::TermKind::BlankNode {
                        let sub = inner_triple.s().value().to_string().replace("riog", owl_on_fs.file_stem().unwrap().to_str().unwrap());
                        let sub_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(sub.as_str()).unwrap();

                        let obj = inner_triple.o().value().to_string().replace("riog", owl_on_fs.file_stem().unwrap().to_str().unwrap());
                        let obj_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(obj.as_str()).unwrap();

                        output_graph.insert(&sub_term, inner_triple.p(), &obj_term).unwrap();
                        return;
                    }

                    if inner_triple.s().kind() == term::TermKind::BlankNode && inner_triple.o().kind() != term::TermKind::BlankNode {
                        let sub = inner_triple.s().value().to_string().replace("riog", owl_on_fs.file_stem().unwrap().to_str().unwrap());
                        let sub_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(sub.as_str()).unwrap();

                        output_graph.insert(&sub_term, inner_triple.p(), inner_triple.o()).unwrap();
                        return;
                    }

                    if inner_triple.s().kind() != term::TermKind::BlankNode && inner_triple.o().kind() == term::TermKind::BlankNode {
                        let obj = inner_triple.o().value().to_string().replace("riog", owl_on_fs.file_stem().unwrap().to_str().unwrap());
                        let obj_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(obj.as_str()).unwrap();

                        output_graph.insert(inner_triple.s(), inner_triple.p(), &obj_term).unwrap();
                        return;
                    }

                    output_graph.insert(inner_triple.s(), inner_triple.p(), inner_triple.o()).unwrap();
                })
                .unwrap();
            debug!("tmp_graph.triples().count(): {}, output_graph.triples().count(): {}", tmp_graph.triples().count(), output_graph.triples().count());
        })?;
    }

    graph.triples().filter_triples(|t| t.p() != &owl_imports).for_each_triple(|triple| {
        if triple.s().kind() == term::TermKind::BlankNode && triple.o().kind() == term::TermKind::BlankNode {
            let sub = triple.s().value().to_string().replace("riog", "cam");
            let sub_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(sub.as_str()).unwrap();

            let obj = triple.o().value().to_string().replace("riog", "cam");
            let obj_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(obj.as_str()).unwrap();

            output_graph.insert(&sub_term, triple.p(), &obj_term).unwrap();
            return;
        }

        if triple.s().kind() == term::TermKind::BlankNode && triple.o().kind() != term::TermKind::BlankNode {
            let sub = triple.s().value().to_string().replace("riog", "cam");
            let sub_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(sub.as_str()).unwrap();

            output_graph.insert(&sub_term, triple.p(), triple.o()).unwrap();
            return;
        }

        if triple.s().kind() != term::TermKind::BlankNode && triple.o().kind() == term::TermKind::BlankNode {
            let obj = triple.o().value().to_string().replace("riog", "cam");
            let obj_term: term::blank_node::BlankNode<&str> = term::blank_node::BlankNode::new(obj.as_str()).unwrap();

            output_graph.insert(triple.s(), triple.p(), &obj_term).unwrap();
            return;
        }

        output_graph.insert(triple.s(), triple.p(), triple.o()).unwrap();
    })?;

    Ok(output_graph)
}

async fn fetch_import(client: &Client, import_url: &str, owl_on_fs: &path::Path) -> Result<(), Box<dyn error::Error + Send + Sync>> {
    fs::create_dir_all(&owl_on_fs.parent().expect("could not get parent directory")).unwrap();
    debug!("attempting get on: {:?} and writing to {:?}", import_url, owl_on_fs);
    let mut response = client.get(import_url).send().await?;
    let output = fs::File::create(&owl_on_fs)?;
    let mut tmp_writer = io::BufWriter::new(output);
    while let Some(chunk) = response.chunk().await? {
        tmp_writer.write_all(&chunk)?;
    }
    Ok(())
}
