# DeepSeek Harness VSCode Extension

> [English](./README_EN.md) · 中文

本项目是一个为 DeepSeek Harness 提供额外能力的 **Visual Studio Code 拓展**。

## 关于本 Fork

[本仓库](https://github.com/bvbhu/deepseek-harness-vsc-extension) 是上游 [DeepSeek Harness VSCode Extension](https://github.com/weinibuliu/deepseek-harness-vsc-extension) 的个人维护 Fork，主要目标是修复个人使用中遇到的问题并跟进上游 `dsh` 的变更。

## 功能特性

- VSCode 风格的界面
- 原生文件选择器
- 当前焦点感知
- 编辑器问题
- 空白会话 Agent 预设选择器

## 安装

### 下载预构建 VSIX

[Github Release](https://github.com/bvbhu/deepseek-harness-vsc-extension/releases)

[Github Repo](https://github.com/bvbhu/deepseek-harness-vsc-extension)

### 本地构建

参见 [开发](#开发) 章节。

## 开始使用

本拓展将会尝试寻找可用的 `dsh` 。因此，请自行安装 dsh 。

```bash
npm install -g @deepseek-ai/dsh
# 或按需运行
npx @deepseek-ai/dsh
```

> [!NOTE]
> 由于 DeepSeek Harness 有可能发生破坏性变更，本拓展或许仅能与特定版本的 dsh 一起运行。
>
> **测试 dsh 版本：`0.1.2-rc.1`**（自本拓展 `0.1.15` 起）。

## 开发

```bash
pnpm i

# 调试
pnpm build
# 然后按 F5

# 打包（输出 VSIX）
pnpm package
```

## License

本项目以 **MIT 协议** 开源。

## 相关

- [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) ——  `dsh` 运行时。
