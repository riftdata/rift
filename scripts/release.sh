#!/usr/bin/env bash

set -euo pipefail

# Release script for pgbranch
# Usage: ./scripts/release.sh v1.0.0

VERSION="${1:?Usage: $0 <version> (e.g., v1.0.0)}"

# Validate version format
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    echo "Error: Version must match vMAJOR.MINOR.PATCH (e.g., v1.0.0 or v1.0.0-rc.1)"
    exit 1
fi

# Ensure working directory is clean
if [[ -n "$(git status --porcelain)" ]]; then
    echo "Error: Working directory is not clean. Commit or stash changes first."
    exit 1
fi

# Ensure we're on main
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$BRANCH" != "main" ]]; then
    echo "Error: Releases must be created from the main branch (currently on $BRANCH)"
    exit 1
fi

# Ensure we're up to date with remote
git fetch origin main
LOCAL="$(git rev-parse HEAD)"
REMOTE="$(git rev-parse origin/main)"
if [[ "$LOCAL" != "$REMOTE" ]]; then
    echo "Error: Local main is not up to date with origin/main. Run 'git pull' first."
    exit 1
fi

# Run checks
echo "Running checks..."
make check

# Create and push the tag
echo "Creating tag $VERSION..."
git tag -a "$VERSION" -m "Release $VERSION"
git push origin "$VERSION"

echo ""
echo "Tag $VERSION pushed. The release workflow will now run on GitHub Actions."
echo "Monitor progress at: https://github.com/pgbranch/pgbranch/actions"