#!/usr/bin/env node

"use strict";

const os = require("os");
const path = require("path");
const {execFileSync} = require("child_process");

const PLATFORMS = {
    darwin: "darwin",
    linux: "linux",
    win32: "win32",
};

const ARCHS = {
    x64: "x64",
    arm64: "arm64",
};

function getPlatformPackage() {
    const platform = PLATFORMS[os.platform()];
    const arch = ARCHS[os.arch()];

    if (!platform || !arch) {
        console.error(
            `Unsupported platform: ${os.platform()} ${os.arch()}\n` +
            `rift supports: darwin-x64, darwin-arm64, linux-x64, linux-arm64, win32-x64`
        );
        process.exit(1);
    }

    return `@rift-data/cli-${platform}-${arch}`;
}

function getBinaryPath() {
    const pkg = getPlatformPackage();
    const bin = os.platform() === "win32" ? "rift.exe" : "rift";

    try {
        const pkgPath = require.resolve(`${pkg}/package.json`);
        return path.join(path.dirname(pkgPath), "bin", bin);
    } catch {
        console.error(
            `Could not find the rift binary for your platform (${os.platform()}-${os.arch()}).\n` +
            `Expected npm package: ${pkg}\n\n` +
            `This can happen if:\n` +
            `  - Your package manager does not install optional dependencies\n` +
            `  - Your platform is not supported\n\n` +
            `Try reinstalling with: npm install @rift-data/cli\n` +
            `Or install directly: curl -fsSL https://riftdata.io/install.sh | sh`
        );
        process.exit(1);
    }
}

try {
    execFileSync(getBinaryPath(), process.argv.slice(2), {
        stdio: "inherit",
    });
} catch (e) {
    if (e.status !== undefined) {
        process.exit(e.status);
    }
    console.error(e.message);
    process.exit(1);
}