# Licensing Notice

## Overview

This project contains components under different licenses.

### Actus (BUSL-1.1)

The actus source code (`src/`, `*.py`, `run.sh`, `Cargo.toml`) is licensed
under the Business Source License 1.1 (see `LICENSE`).

Copyright (c) 2026 SSCCS Foundation.

### Helix / Zed (GPL)

The `helix/subtree/` directory contains a fork of the
[Zed Editor](https://github.com/helixml/zed), which is licensed under the
GNU General Public License v3.0 (GPL-3.0), see `LICENSE.helix`.

The `helix/build.sh` script may clone, patch, and build this code. The
resulting binary (`helix-zed-headless-*`) is GPL-licensed and is executed
as a separate process — it is not linked into the actus binary.

### Docker Images

- **`base` target**: Contains only the actus binary (BUSL-1.1).
- **`full` target**: Contains both actus binary (BUSL-1.1) and the
  helix-zed-headless binary (GPL-3.0). This is an aggregate work.
  Both licenses apply to their respective components.

### Compliance

For the `full` Docker image, GPL compliance requires:
- Access to the Zed source: https://github.com/helixml/zed
- This notice is included in the image at `/NOTICE.md`
- No modification or linking of GPL code into BUSL code occurs
