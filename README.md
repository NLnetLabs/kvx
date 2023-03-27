## Key value store X

Abstraction layer over various key-value store backends in Rust. Taylored to fit the use-cases for [krill](https://github.com/NLnetLabs/krill).

Switching between backends should be as simple as changing a configuration value.

For now an in-memory, filesystem and postgres implementations are provided by default.

## Development

Startup postgres:

```
docker compose up
```

Run tests:

```
cargo test
```
