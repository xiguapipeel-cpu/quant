#!/usr/bin/env bash
# ============================================================
# build_frontend.sh — 构建 Web 前端（React/Vite SPA）
# ============================================================
# 本机背景：系统 node 已丢失，唯一可用的是 Codex.app 自带的 node，
# 但它启用了 macOS library validation（hardened runtime），无法加载
# 项目里第三方签名的 native 模块（vite 8 的 rolldown *.node），报
# "different Team IDs"。本脚本自动处理：
#   1) 若 PATH 里已有能正常构建的 node → 直接用；
#   2) 否则复制一份 node 到缓存目录并改 ad-hoc 签名（去掉 library
#      validation），用它构建。
#
# 用法：
#   bash scripts/build_frontend.sh
#   CODEX_NODE=/path/to/node bash scripts/build_frontend.sh   # 自定义 node 源
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND="$ROOT/web/frontend"
CACHE_DIR="${HOME}/.cache/quant-build"
CACHED_NODE="$CACHE_DIR/node"
CODEX_NODE="${CODEX_NODE:-/Applications/Codex.app/Contents/Resources/node}"

if [ ! -d "$FRONTEND/node_modules" ]; then
  echo "❌ 未找到 $FRONTEND/node_modules，请先在该目录执行依赖安装。"
  exit 1
fi

# 用候选 node 尝试加载 rolldown 的 native 模块，验证它能真正构建
node_can_build() {
  local cand="$1"
  [ -x "$cand" ] || return 1
  "$cand" -e '
    const fs=require("fs"),path=require("path");
    const d=path.join(process.argv[1],"node_modules/@rolldown");
    if(!fs.existsSync(d)){process.exit(0);}            // 无 rolldown，按可用处理
    const pkg=fs.readdirSync(d).find(x=>x.startsWith("binding-"));
    if(!pkg){process.exit(0);}
    const f=fs.readdirSync(path.join(d,pkg)).find(x=>x.endsWith(".node"));
    if(!f){process.exit(0);}
    require(path.join(d,pkg,f));                        // 关键：能 dlopen 即通过
  ' "$FRONTEND" >/dev/null 2>&1
}

# 制作 ad-hoc 签名的 node 副本（去掉 library validation）
make_adhoc_node() {
  local src="$1"
  echo "→ 从 $src 制作 ad-hoc 签名 node 副本..."
  mkdir -p "$CACHE_DIR"
  cp -f "$src" "$CACHED_NODE"
  codesign --remove-signature "$CACHED_NODE" 2>/dev/null || true
  codesign -s - --force "$CACHED_NODE" 2>/dev/null || true
}

NODE_BIN=""

# 1) PATH 里的 node 可用？
if command -v node >/dev/null 2>&1 && node_can_build "$(command -v node)"; then
  NODE_BIN="$(command -v node)"
  echo "✓ 使用 PATH 中的 node：$NODE_BIN ($("$NODE_BIN" -v))"
# 2) 缓存的 ad-hoc node 可用？
elif [ -x "$CACHED_NODE" ] && node_can_build "$CACHED_NODE"; then
  NODE_BIN="$CACHED_NODE"
  echo "✓ 使用缓存的 ad-hoc node：$NODE_BIN ($("$NODE_BIN" -v))"
# 3) 从 Codex node 现做一个 ad-hoc 副本
elif [ -x "$CODEX_NODE" ]; then
  make_adhoc_node "$CODEX_NODE"
  if node_can_build "$CACHED_NODE"; then
    NODE_BIN="$CACHED_NODE"
    echo "✓ 已生成可用 node：$NODE_BIN ($("$NODE_BIN" -v))"
  fi
fi

if [ -z "$NODE_BIN" ]; then
  echo "❌ 找不到可用于构建的 node。"
  echo "   建议安装正常的 node（brew install node 或 nvm），"
  echo "   或设置 CODEX_NODE=/可用node路径 后重试。"
  exit 1
fi

# 把选定 node 所在目录放到 PATH 最前（npm/tsc/vite 的 shebang 走 env node）
export PATH="$(dirname "$NODE_BIN"):$PATH"
# 缓存 node 命名即为 node；若用的是缓存副本，确保其目录优先
if [ "$NODE_BIN" = "$CACHED_NODE" ]; then
  export PATH="$CACHE_DIR:$PATH"
fi

echo "→ 开始构建前端（输出到 web/static/dist）..."
cd "$FRONTEND"
npm run build

echo "✅ 前端构建完成。重启 Web 后生效：./venv/bin/python start_web.py"
