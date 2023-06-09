# Key-value store X

Abstraction layer over various key-value store backends in Rust. Tailored to fit the use-cases for [Krill](https://github.com/NLnetLabs/krill).

For user documentation see README.md in the kvx directory.

## Development

In order to make development easy, a `docker-compose.yml` that starts a Postgres container is included. One can start it with:
```
docker compose up
```

When the container is running, one can run tests with:
```
cargo test
```

To run test without including Postgres, run:
```
cargo test --no-default-features
```

## Releasing

Use:
```
cargo release
```