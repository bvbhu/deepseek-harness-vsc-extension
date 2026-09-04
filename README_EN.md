# DeepSeek Harness VSCode Extension

> **English** · [中文](./README.md)

This project is a **Visual Studio Code extension** that provides additional features for DeepSeek Harness.

## About This Fork

[This repository](https://github.com/bvbhu/deepseek-harness-vsc-extension) is a personally maintained fork of the upstream [DeepSeek Harness VSCode Extension](https://github.com/weinibuliu/deepseek-harness-vsc-extension), focused on fixing issues encountered in personal use and tracking upstream `dsh` changes.

## Features

- VSCode-style interface
- Native file picker
- Current focus awareness
- Problems from editor
- Agent preset selector for blank sessions

## Install

### Download a prebuilt VSIX

[Github Release](https://github.com/bvbhu/deepseek-harness-vsc-extension/releases)

[Github Repo](https://github.com/bvbhu/deepseek-harness-vsc-extension)

### Build locally

See [Development](#development).

## Getting Started

The extension will try to find a usable `dsh`. Therefore, please install `dsh` yourself.

```bash
npm install -g @deepseek-ai/dsh
# or run on demand
npx @deepseek-ai/dsh
```

> [!NOTE]
> Due to the possibility of breaking changes to DeepSeek Harness, this extension may only run with a specific version of `dsh`.
>
> **Tested dsh version: `0.1.2-rc.1`** (since extension `0.1.15`).

## Development

```bash
pnpm i

# Debug
pnpm build
# then press F5

# Package (outputs a VSIX)
pnpm package
```

## License

This project is licensed under the **MIT License**.

## Related

- [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) — the `dsh` runtime.
