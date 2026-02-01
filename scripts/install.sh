#!/usr/bin/env bash

set -euo pipefail

# rift installer script
# Usage: curl -fsSL https://riftdata.io/install.sh | sh

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
GITHUB_REPO="riftdata/rift"
BINARY_NAME="rift"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Detect OS
detect_os() {
    OS="$(uname -s)"
    case "$OS" in
        Linux*)     OS=linux;;
        Darwin*)    OS=darwin;;
        MINGW*|MSYS*|CYGWIN*) OS=windows;;
        *)          log_error "Unsupported OS: $OS"; exit 1;;
    esac
    echo "$OS"
}

# Detect architecture
detect_arch() {
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64|amd64)   ARCH=amd64;;
        aarch64|arm64)  ARCH=arm64;;
        *)              log_error "Unsupported architecture: $ARCH"; exit 1;;
    esac
    echo "$ARCH"
}

# Get latest version from GitHub
get_latest_version() {
    curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" | \
        grep '"tag_name":' | \
        sed -E 's/.*"([^"]+)".*/\1/'
}

# Download and install
install() {
    OS=$(detect_os)
    ARCH=$(detect_arch)

    log_info "Detected OS: $OS, Architecture: $ARCH"

    # Get version
    VERSION="${VERSION:-$(get_latest_version)}"
    if [[ -z "$VERSION" ]]; then
        log_error "Could not determine latest version"
        exit 1
    fi

    log_info "Installing rift $VERSION"

    # Construct download URL
    if [[ "$OS" == "windows" ]]; then
        FILENAME="${BINARY_NAME}-${OS}-${ARCH}.exe"
    else
        FILENAME="${BINARY_NAME}-${OS}-${ARCH}"
    fi

    DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${VERSION}/${FILENAME}"

    # Create temp directory
    TMP_DIR=$(mktemp -d)
    trap "rm -rf $TMP_DIR" EXIT

    # Download
    log_info "Downloading from $DOWNLOAD_URL"
    if command -v curl &> /dev/null; then
        curl -fsSL "$DOWNLOAD_URL" -o "$TMP_DIR/$BINARY_NAME"
    elif command -v wget &> /dev/null; then
        wget -q "$DOWNLOAD_URL" -O "$TMP_DIR/$BINARY_NAME"
    else
        log_error "Neither curl nor wget found. Please install one of them."
        exit 1
    fi

    # Make executable
    chmod +x "$TMP_DIR/$BINARY_NAME"

    # Verify binary works
    if ! "$TMP_DIR/$BINARY_NAME" --version &> /dev/null; then
        log_error "Downloaded binary failed verification"
        exit 1
    fi

    # Install
    log_info "Installing to $INSTALL_DIR"

    if [[ -w "$INSTALL_DIR" ]]; then
        mv "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    else
        log_warn "Need sudo to install to $INSTALL_DIR"
        sudo mv "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    fi

    # Verify installation
    if command -v rift &> /dev/null; then
        log_info "Installation complete!"
        echo ""
        rift --version
        echo ""
        echo -e "${BLUE}Get started:${NC}"
        echo "  rift init --upstream postgres://localhost:5432/mydb"
        echo "  rift serve"
        echo "  rift create my-feature-branch"
        echo ""
        echo "Documentation: https://riftdata.io/docs"
    else
        log_warn "rift installed but not in PATH"
        log_warn "Add $INSTALL_DIR to your PATH or run: $INSTALL_DIR/rift"
    fi
}

# Uninstall
uninstall() {
    log_info "Uninstalling rift"

    if [[ -f "$INSTALL_DIR/$BINARY_NAME" ]]; then
        if [[ -w "$INSTALL_DIR" ]]; then
            rm "$INSTALL_DIR/$BINARY_NAME"
        else
            sudo rm "$INSTALL_DIR/$BINARY_NAME"
        fi
        log_info "rift removed"
    else
        log_warn "rift not found in $INSTALL_DIR"
    fi
}

# Main
main() {
    case "${1:-install}" in
        install)
            install
            ;;
        uninstall)
            uninstall
            ;;
        *)
            echo "Usage: $0 [install|uninstall]"
            exit 1
            ;;
    esac
}

main "$@"
