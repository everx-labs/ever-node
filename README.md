# ever-node

Everscale/Venom node and validator

## Table of Contents

- [About](#about)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## About

Implementation of Everscal/Venom node and validator in safe Rust. 

## Getting Started

### Prerequisites

Rust complier v1.65+.

```
apt-get update
apt-get install pkg-config make clang libssl-dev libzstd-dev libgoogle-perftools-dev
```

### Installing

```
git clone --recurse-submodules https://github.com/tonlabs/ever-node.git
cd ever-node
cargo build --release
```

### Running tests

```
cargo test --release --package catchain -- --nocapture --test-threads=1 
cargo test --release --package storage -- --nocapture --test-threads=1 
cargo test --release --package validator_session -- --nocapture --test-threads=1 
cargo test --release -- --nocapture --test-threads=1
```

## Usage

To get help about command line arguments, run
```
ton_node --help
```

## Contributing

Contribution to the project is expected to be done via pull requests submission.

## License

See the [LICENSE](LICENSE) file for details.

## Tags

`blockchain` `everscale` `rust` `venom-blockchain` `venom-developer-program` `venom-node` `venom-validator` 
