# Welcome to Benchmarking Oxigraph with Ubergraph

## to compile

```shell
cargo build --release
```

## to build/materialize ubergraph

```shell
RUST_LOG=debug ./target/release/ubergraph-oxigraph-benchmark -i <ubergraph.ttl> -o /tmp/ubergraph.nt
```

