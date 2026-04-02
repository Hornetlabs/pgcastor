# Contributing to pgcastor

Thank you for your interest in contributing to pgcastor! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Style](#code-style)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [Testing](#testing)
- [Documentation](#documentation)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

| Requirement | Version | Purpose |
|-------------|---------|---------|
| CMake | 3.10+ | Build system |
| GCC/Clang | C99 support | C compiler |
| PostgreSQL | 12+ | Database client library (libpq) |
| Readline | - | CLI line editing |
| LZ4 | - | Compression library |
| Bison | 3.0+ | Parser generator (optional) |
| Flex | 2.6+ | Lexer generator (optional) |

### Installing Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y cmake build-essential bison flex \
    libreadline-dev liblz4-dev postgresql-server-dev-12
```

**CentOS/RHEL:**
```bash
sudo yum install -y cmake gcc bison flex \
    readline-devel lz4-devel postgresql12-devel
```

### Building the Project

1. **Clone the repository:**
   ```bash
   git clone https://github.com/bytesync/pgcastor.git
   cd pgcastor
   ```

2. **Configure with CMake:**
   ```bash
   mkdir build && cd build
   cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
   ```

3. **Build:**
   ```bash
   make -j$(nproc)
   ```

4. **Verify the build:**
   ```bash
   ls -la bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci
   ```

## Development Workflow

1. **Fork the repository** on GitHub.

2. **Clone your fork:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/pgcastor.git
   cd pgcastor
   ```

3. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make your changes** following the code style guidelines.

5. **Test your changes:**
   ```bash
   # Build
   cd build && cmake .. && make -j$(nproc)
   
   # Test basic functionality
   ./bin/xscsci/xscsci --help
   ```

6. **Commit your changes:**
   ```bash
   git add .
   git commit -m "type: description"
   ```

7. **Push to your fork:**
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Create a Pull Request** on GitHub.

## Code Style

Please follow the project's coding standards as documented in [CODE_STYLE.md](CODE_STYLE.md):

- **Indententation:** 4 spaces (no tabs)
- **Braces:** Opening brace on a new line (Allman style)
- **Line length:** Maximum 120 characters
- **Naming:**
  - Functions: `module_function()` format
  - Global variables: `g_` prefix
  - Static variables: `m_` prefix
  - Struct members: `m_` prefix

### Formatting

We use `.clang-format` for consistent formatting. Before committing:

```bash
# Format all C files
find src bin -name "*.c" -o -name "*.h" | xargs clang-format -i
```

## Commit Guidelines

We follow conventional commit messages:

```
type(subject): [optional body]
[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Examples

```
feat: add PostgreSQL 15 support for WAL parsing
fix: resolve memory leak in capture process
docs: update build instructions for macOS
refactor: simplify encoding conversion logic
```

## Pull Request Process

1. **Update documentation** if your changes affect the public API or user-facing features.

2. **Update CHANGELOG.md** with your changes under the "Unreleased" section.

3. **Add tests** for new functionality (when test infrastructure is available).

4. **Ensure the build passes:**
   ```bash
   cd build
   cmake .. && make -j$(nproc)
   ```

5. **Request review** from maintainers.

6. **Address review feedback** promptly.

### PR Checklist

- [ ] Code follows the project's style guidelines
- [ ] Self-review of the code has been performed
- [ ] Comments are added for complex logic
- [ ] Documentation has been updated
- [ ] CHANGELOG.md has been updated
- [ ] No new compiler warnings are introduced

## Testing

Currently, the project has manual testing procedures. See [run_and_test.md](run_and_test.md) for detailed testing instructions.

### Basic Testing

```bash
# Set environment
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$(pwd)/lib:$(pwd)/interfaces/lib:$LD_LIBRARY_PATH

# Test binaries
./bin/capture/capture
./bin/integrate/integrate
./bin/xmanager/xmanager
./bin/xscsci/xscsci
```

## Documentation

- **README.md** - Project overview and quick start
- **compile.md** - Detailed compilation instructions
- **run_and_test.md** - Deployment and testing guide
- **CODE_STYLE.md** - Coding standards
- **CHANGELOG.md** - Version history

When adding new features, please update the relevant documentation.

## Questions?

If you have questions, feel free to:

1. Open an issue for bugs or feature requests
2. Start a discussion for general questions
3. Check existing issues and documentation

Thank you for contributing to pgcastor!
