import { Component, type ReactNode } from 'react'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import favicon from '@ui/favicon.svg?url'
import App from './App.tsx'
import './style.css'

// Vite alias (@ui) resolves the favicon; `?url` forces a real asset file
// instead of an inlined data URI. Hook it into the document head (HTML href
// aliases are not resolved by Vite).
const link = document.createElement('link')
link.rel = 'icon'
link.href = favicon
document.head.appendChild(link)

/**
 * 全局错误上报（诊断「对话一闪而过 / 面板空白」）：
 * - App 挂载后会把 acquireVsCodeApi() 实例挂到 window.__dshVscode，此时错误
 *   经 `notice` 进扩展输出面板（用户贴日志就能看到崩溃堆栈）；
 * - App 尚未挂载（首次渲染即崩）时把错误写进 #root，面板不至于纯白。
 */
type ErrorSink = { postMessage: (message: { type: 'notice'; text: string }) => void }
function reportWebviewError(text: string): void {
  const sink = (window as unknown as { __dshVscode?: ErrorSink }).__dshVscode
  if (sink) sink.postMessage({ type: 'notice', text: `[webview] ${text}` })
  const root = document.getElementById('root')
  if (root !== null && root.childElementCount === 0) {
    const pre = document.createElement('pre')
    pre.style.cssText = 'white-space:pre-wrap;padding:12px;font-size:12px;color:#f66'
    pre.textContent = text
    root.appendChild(pre)
  }
}
window.addEventListener('error', (event) => {
  const error = event.error as { stack?: string } | null
  reportWebviewError(`window.error: ${String(event.message)}\n${error?.stack ?? ''}`)
})
window.addEventListener('unhandledrejection', (event) => {
  const reason = event.reason as { stack?: string; message?: string } | null
  reportWebviewError(
    `unhandledrejection: ${String(reason?.message ?? event.reason)}\n${reason?.stack ?? ''}`,
  )
})

/** 渲染崩溃兜底：不吞错——上报 + 原样展示，避免整棵树卸载成空白面板。 */
class ErrorBoundary extends Component<{ children: ReactNode }, { message: string | null }> {
  override state = { message: null as string | null }
  static getDerivedStateFromError(error: unknown): { message: string } {
    const stack = error instanceof Error ? error.stack : String(error)
    return { message: `${error instanceof Error ? error.message : String(error)}\n${stack ?? ''}` }
  }
  override componentDidCatch(error: unknown): void {
    const stack = error instanceof Error ? error.stack : String(error)
    reportWebviewError(
      `react render error: ${error instanceof Error ? error.message : String(error)}\n${stack ?? ''}`,
    )
  }
  override render(): ReactNode {
    if (this.state.message === null) return this.props.children
    return (
      <pre style={{ whiteSpace: 'pre-wrap', padding: 12, fontSize: 12, color: '#f66' }}>
        {this.state.message}
      </pre>
    )
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </StrictMode>,
)
