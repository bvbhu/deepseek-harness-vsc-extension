# DeepSeek Harness VSCode Extension

> [!NOTE]
> ### About This Fork
>
> [This repository](https://github.com/bvbhu/deepseek-harness-vsc-extension) is a personally maintained Fork of [DeepSeek Harness VSCode Extension](https://github.com/weinibuliu/deepseek-harness-vsc-extension), primarily aiming to fix issues encountered during personal use.
>
> [本仓库](https://github.com/bvbhu/deepseek-harness-vsc-extension) 是 [DeepSeek Harness VSCode Extension](https://github.com/weinibuliu/deepseek-harness-vsc-extension) 的个人维护 Fork，主要目标是修复个人使用中遇到的问题。
>
> **Important:** The original Visual Studio Marketplace listing is no longer available. Please download the latest build from the **GitHub Actions** page. 
>
> **重要提示：** 原 Visual Studio Marketplace 已不可用，可以从 **GitHub Actions** 页面下载最新构建。
>
> For manual local build, refer to the [Development](#development) section.
>
> 本地构建请参考 [Development](#development) 章节。

> [!NOTE]
> The content below is derived from the original repository README.
>
> 以下内容为原仓库 README。

> [!NOTE]
> This is a **COMMUNITY** project. There may be some issues, and we are trying to make it better.
>
> 这是一个**社区**项目，它可能存在一些问题，而我们正在努力让它变得足够好用。

<!--  -->

The Project is **Visual Studio Code Extension** provides additional features for DeepSeek Harness.

本项目是一个为 DeepSeek Harness 提供额外能力的 **Visual Studio Code 拓展**。

![Installs](https://vsmarketplacebadges.dev/installs-short/weinibuliu.dsh-vsc.svg) ![GitHub License](https://img.shields.io/github/license/weinibuliu/deepseek-harness-vsc-extension) ![GitHub commit activity](https://img.shields.io/github/commit-activity/w/weinibuliu/deepseek-harness-vsc-extension)

## Features

- VSCode style interface VSCode 风格的界面
- Native File Picker 原生文件选择器
- Feeling of current focus 当前焦点感知
- Problems from editor 编辑器问题
- Agent Preset selector for blank sessions 空白会话模式选择器

## Install

### Visual Studio Marketplace

[Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=weinibuliu.dsh-vsc)

### Download or Build VSIX

[Github Release](https://github.com/weinibuliu/deepseek-harness-vsc-extension/releases)

[Github Repo](https://github.com/weinibuliu/deepseek-harness-vsc-extension)

## Start

Extension will try to find `dsh`. so, you install `dsh`:

本拓展将会尝试寻找可用的 `dsh` 。因此，请自行安装 dsh 。

```bash
npm install -g @deepseek-ai/dsh
# or
npx @deepseek-ai/dsh
```

> [!NOTE]
> Due to the possibility of breaking changes to DeepSeek Harness, this extension may only run with specific version dsh.
>
> 由于 DeepSeek Harness 有可能发生破坏性变更，本拓展或许仅能与特定版本的 dsh 一起正常运行。
>
> Test Passed Version: 0.1.0-rc.6

## TODO

We are trying to make this project better, including following the dsh and adding new features.

我们正在努力让该插件足够好用，包括跟进 dsh 自身功能与添加新的功能。

### Following

- [x] Display usage 显示用量
- [x] Context usage 显示上下文
- [x] Agent preset Agent 预设 (PTC 模式等)
- [ ] Fork session fork 会话
- [ ] Changes list 产物列表
- [ ] SubAgents management SubAgent 管理
- [ ] Plugin management 插件管理
- [ ] i18n 国际化

### New

- [ ] Real cost time (use timestamp mark action instead of interface timer) 真实花费时间

## License

This project is licensed under **MIT LICENSE**.

项目以 MIT 协议开源。

## Development

```bash
pnpm i

# Debug
pnpm build
F5

# Package (output VSIX)
pnpm package
```

## Acknowledge

- [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness)
